use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{Mutex, mpsc};
use tokio::time::timeout;
use uuid::Uuid;

use crate::config::Config;
use crate::metrics::RuntimeMetrics;
use crate::models::FontFaceInfo;

#[derive(Clone)]
pub struct FontWorkerPool {
    workers: Arc<Vec<Arc<Mutex<PythonWorker>>>>,
    available_tx: mpsc::UnboundedSender<usize>,
    available_rx: Arc<Mutex<mpsc::UnboundedReceiver<usize>>>,
    python_bin: Arc<String>,
    worker_script: Arc<PathBuf>,
    request_timeout: Duration,
    metrics: Arc<RuntimeMetrics>,
}

struct WorkerLease {
    index: usize,
    available_tx: mpsc::UnboundedSender<usize>,
}

struct PythonWorker {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct WorkerEnvelope<T> {
    id: String,
    ok: bool,
    result: Option<T>,
    error: Option<String>,
}

struct WorkerFailure {
    retryable: bool,
    error: anyhow::Error,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct InspectResult {
    pub faces: Vec<FontFaceInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SubsetResult {
    pub output_path: String,
    pub orig_size: u64,
    pub subset_size: u64,
    pub used_codepoints: Vec<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EmbeddedFontMetadata {
    pub font_subset_map: Option<FontSubsetMap>,
    pub draw_entries: Vec<DrawTableEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FontSubsetMap {
    pub original: String,
    pub subset: String,
    pub version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrawTableEntry {
    pub data: String,
    pub ch: String,
    pub flags: u8,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DrawFontResult {
    pub output_path: String,
    pub subset_size: u64,
    pub entries: Vec<DrawTableEntry>,
}

#[derive(Debug, Serialize)]
pub struct SubsetRequest<'a> {
    pub source_path: &'a str,
    pub ttc_index: i32,
    pub output_path: &'a str,
    pub codepoints: &'a [u32],
    pub include_ascii: bool,
    pub full_font: bool,
    pub retain_variations: bool,
    pub target_family: &'a str,
    pub original_family: &'a str,
    pub subfamily: &'a str,
    pub randomize_map: Option<RandomizeMap<'a>>,
    pub service_version: &'static str,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct RandomizeMap<'a> {
    pub original: &'a str,
    pub subset: &'a str,
}

#[derive(Debug, Serialize)]
pub struct DrawFontRequest<'a> {
    pub output_path: &'a str,
    pub family: &'a str,
    pub drawings: &'a [DrawTableEntry],
    pub service_version: &'static str,
}

impl FontWorkerPool {
    pub async fn start(config: &Config, metrics: Arc<RuntimeMetrics>) -> anyhow::Result<Self> {
        let mut workers = Vec::with_capacity(config.max_font_workers);
        for _ in 0..config.max_font_workers {
            workers.push(Arc::new(Mutex::new(
                PythonWorker::spawn(&config.python_bin, &config.worker_script).await?,
            )));
        }
        let (available_tx, available_rx) = mpsc::unbounded_channel();
        for index in 0..workers.len() {
            available_tx
                .send(index)
                .expect("new worker availability queue is open");
        }
        Ok(Self {
            workers: Arc::new(workers),
            available_tx,
            available_rx: Arc::new(Mutex::new(available_rx)),
            python_bin: Arc::new(config.python_bin.clone()),
            worker_script: Arc::new(config.worker_script.clone()),
            request_timeout: config.font_worker_timeout,
            metrics,
        })
    }

    pub async fn inspect_font(&self, path: &Path) -> anyhow::Result<InspectResult> {
        self.request(json!({
            "op": "inspect",
            "path": path.to_string_lossy(),
        }))
        .await
    }

    pub async fn subset_font(&self, req: &SubsetRequest<'_>) -> anyhow::Result<SubsetResult> {
        self.request(json!({
            "op": "subset",
            "payload": req,
        }))
        .await
    }

    pub async fn inspect_embedded_font(
        &self,
        font_b64: &str,
    ) -> anyhow::Result<EmbeddedFontMetadata> {
        self.request(json!({
            "op": "inspect_embedded",
            "payload": { "font_b64": font_b64 },
        }))
        .await
    }

    pub async fn create_draw_font(
        &self,
        req: &DrawFontRequest<'_>,
    ) -> anyhow::Result<DrawFontResult> {
        self.request(json!({
            "op": "create_draw_font",
            "payload": req,
        }))
        .await
    }

    async fn request<T: DeserializeOwned>(&self, mut payload: Value) -> anyhow::Result<T> {
        let id = Uuid::new_v4().to_string();
        payload["id"] = Value::String(id.clone());
        let pool = self.clone();
        let value = tokio::spawn(async move { pool.request_value(id, payload).await })
            .await
            .context("font worker request task failed")??;
        serde_json::from_value(value).context("invalid font worker result")
    }

    async fn request_value(&self, id: String, payload: Value) -> anyhow::Result<Value> {
        self.metrics.record_worker_request();
        let index = self
            .available_rx
            .lock()
            .await
            .recv()
            .await
            .context("font worker availability queue closed")?;
        let _lease = WorkerLease {
            index,
            available_tx: self.available_tx.clone(),
        };
        let worker = self.workers[index].clone();
        let mut worker = worker.lock().await;

        for attempt in 0..=1 {
            let failure = match timeout(
                self.request_timeout,
                worker.send_and_read(&id, payload.clone()),
            )
            .await
            {
                Ok(Ok(value)) => return Ok(value),
                Ok(Err(failure)) if !failure.retryable => return Err(failure.error),
                Ok(Err(failure)) => format!("{:#}", failure.error),
                Err(_) => format!(
                    "font worker request timed out after {} seconds",
                    self.request_timeout.as_secs()
                ),
            };

            tracing::warn!(worker = index, attempt, error = %failure, "restarting font worker");
            self.metrics.record_worker_restart();
            if let Err(restart_error) = worker
                .restart(self.python_bin.as_str(), self.worker_script.as_ref())
                .await
            {
                bail!("{failure}; font worker restart failed: {restart_error:#}");
            }
            if attempt == 1 {
                bail!("font worker request failed after one retry: {failure}");
            }
        }
        unreachable!("worker request retry loop always returns")
    }
}

impl Drop for WorkerLease {
    fn drop(&mut self) {
        let _ = self.available_tx.send(self.index);
    }
}

impl WorkerFailure {
    fn transport(error: impl Into<anyhow::Error>) -> Self {
        Self {
            retryable: true,
            error: error.into(),
        }
    }

    fn operation(message: String) -> Self {
        Self {
            retryable: false,
            error: anyhow!(message),
        }
    }
}

impl PythonWorker {
    async fn spawn(python_bin: &str, script: &PathBuf) -> anyhow::Result<Self> {
        let mut child = Command::new(python_bin)
            .arg(script)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("failed to spawn font worker {}", script.display()))?;
        let stdin = child.stdin.take().context("font worker stdin missing")?;
        let stdout = child.stdout.take().context("font worker stdout missing")?;
        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout).lines(),
        })
    }

    async fn restart(&mut self, python_bin: &str, script: &PathBuf) -> anyhow::Result<()> {
        let _ = self.child.start_kill();
        let _ = timeout(Duration::from_secs(5), self.child.wait()).await;
        *self = Self::spawn(python_bin, script).await?;
        Ok(())
    }

    async fn send_and_read(&mut self, id: &str, payload: Value) -> Result<Value, WorkerFailure> {
        let mut line = serde_json::to_vec(&payload).map_err(WorkerFailure::transport)?;
        line.push(b'\n');
        self.stdin
            .write_all(&line)
            .await
            .map_err(WorkerFailure::transport)?;
        self.stdin.flush().await.map_err(WorkerFailure::transport)?;
        let Some(resp_line) = self
            .stdout
            .next_line()
            .await
            .map_err(WorkerFailure::transport)?
        else {
            let status = self.child.try_wait().ok().flatten();
            return Err(WorkerFailure::transport(anyhow!(
                "font worker exited before response; status={status:?}"
            )));
        };
        let env: WorkerEnvelope<Value> = serde_json::from_str(&resp_line)
            .with_context(|| format!("invalid worker response: {resp_line}"))
            .map_err(WorkerFailure::transport)?;
        if env.id != id {
            return Err(WorkerFailure::transport(anyhow!(
                "font worker response id mismatch: expected {id}, got {}",
                env.id
            )));
        }
        if env.ok {
            env.result
                .ok_or_else(|| WorkerFailure::transport(anyhow!("worker response missing result")))
        } else {
            Err(WorkerFailure::operation(
                env.error
                    .unwrap_or_else(|| "font worker failed".to_string()),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ProcessingOptions;
    use std::fs;
    use std::time::Instant;

    fn test_config(script: PathBuf, workers: usize) -> Config {
        Config {
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            font_dirs: Vec::new(),
            watch_dirs: Vec::new(),
            backup_dir: PathBuf::from("backups"),
            data_dir: PathBuf::from("data"),
            worker_script: script,
            python_bin: if cfg!(windows) {
                "python".to_string()
            } else {
                "python3".to_string()
            },
            admin_password_hash: None,
            admin_password_plain: Some("test".to_string()),
            allow_no_auth: false,
            secure_cookies: false,
            max_concurrent_jobs: 1,
            max_font_workers: workers,
            max_index_concurrency: 1,
            max_scan_concurrency: 1,
            max_conversion_memory_mb: 64,
            subset_cache_max_mb: 64,
            font_worker_timeout: Duration::from_secs(5),
            job_queue_size: 16,
            scan_interval: Duration::ZERO,
            backup_retention_days: 0,
            options: ProcessingOptions::default(),
        }
    }

    fn fake_worker_script(dir: &Path) -> PathBuf {
        let script = dir.join("fake_worker.py");
        fs::write(
            &script,
            r#"import json
import os
import sys
import time

for line in sys.stdin:
    req = json.loads(line)
    if req.get("op") == "crash_once" and not os.path.exists(req["marker"]):
        open(req["marker"], "w").close()
        os._exit(7)
    time.sleep(float(req.get("sleep", 0)))
    result = {"recovered": req.get("op") == "crash_once"}
    print(json.dumps({"id": req["id"], "ok": True, "result": result, "error": None}), flush=True)
"#,
        )
        .unwrap();
        script
    }

    #[tokio::test]
    async fn worker_crash_restarts_and_retries_once() {
        let dir = tempfile::tempdir().unwrap();
        let script = fake_worker_script(dir.path());
        let marker = dir.path().join("crashed-once");
        let pool = FontWorkerPool::start(&test_config(script, 1), Arc::new(RuntimeMetrics::new()))
            .await
            .unwrap();
        let result: Value = pool
            .request(json!({
                "op": "crash_once",
                "marker": marker.to_string_lossy(),
            }))
            .await
            .unwrap();
        assert_eq!(result["recovered"], true);
        let metrics = pool.metrics.snapshot(0);
        assert_eq!(metrics.workers.requests, 1);
        assert_eq!(metrics.workers.restarts, 1);
    }

    #[tokio::test]
    async fn requests_are_dispatched_to_an_idle_worker() {
        let dir = tempfile::tempdir().unwrap();
        let script = fake_worker_script(dir.path());
        let pool = FontWorkerPool::start(&test_config(script, 2), Arc::new(RuntimeMetrics::new()))
            .await
            .unwrap();
        let slow_pool = pool.clone();
        let slow = tokio::spawn(async move {
            slow_pool
                .request::<Value>(json!({"op": "sleep", "sleep": 0.6}))
                .await
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let started = Instant::now();
        pool.request::<Value>(json!({"op": "sleep", "sleep": 0.0}))
            .await
            .unwrap();
        assert!(started.elapsed() < Duration::from_millis(400));
        slow.await.unwrap().unwrap();
    }
}
