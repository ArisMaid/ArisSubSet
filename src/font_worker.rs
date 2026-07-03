use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::{Context, bail};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::config::Config;
use crate::models::FontFaceInfo;

#[derive(Clone)]
pub struct FontWorkerPool {
    workers: Arc<Vec<Arc<Mutex<PythonWorker>>>>,
    next: Arc<AtomicUsize>,
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
    pub target_family: &'a str,
    pub original_family: &'a str,
    pub subfamily: &'a str,
    pub randomize_map: Option<RandomizeMap<'a>>,
}

#[derive(Debug, Serialize)]
pub struct RandomizeMap<'a> {
    pub original: &'a str,
    pub subset: &'a str,
}

#[derive(Debug, Serialize)]
pub struct DrawFontRequest<'a> {
    pub output_path: &'a str,
    pub family: &'a str,
    pub drawings: &'a [DrawTableEntry],
}

impl FontWorkerPool {
    pub async fn start(config: &Config) -> anyhow::Result<Self> {
        let mut workers = Vec::with_capacity(config.max_font_workers);
        for _ in 0..config.max_font_workers {
            workers.push(Arc::new(Mutex::new(
                PythonWorker::spawn(&config.python_bin, &config.worker_script).await?,
            )));
        }
        Ok(Self {
            workers: Arc::new(workers),
            next: Arc::new(AtomicUsize::new(0)),
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
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        let worker = self.workers[idx].clone();
        let mut guard = worker.lock().await;
        guard.send_and_read::<T>(&id, payload).await
    }
}

impl PythonWorker {
    async fn spawn(python_bin: &str, script: &PathBuf) -> anyhow::Result<Self> {
        let mut child = Command::new(python_bin)
            .arg(script)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
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

    async fn send_and_read<T: DeserializeOwned>(
        &mut self,
        id: &str,
        payload: Value,
    ) -> anyhow::Result<T> {
        let mut line = serde_json::to_vec(&payload)?;
        line.push(b'\n');
        self.stdin.write_all(&line).await?;
        self.stdin.flush().await?;
        let Some(resp_line) = self.stdout.next_line().await? else {
            let status = self.child.try_wait().ok().flatten();
            bail!("font worker exited before response; status={status:?}");
        };
        let env: WorkerEnvelope<T> = serde_json::from_str(&resp_line)
            .with_context(|| format!("invalid worker response: {resp_line}"))?;
        if env.id != id {
            bail!(
                "font worker response id mismatch: expected {id}, got {}",
                env.id
            );
        }
        if env.ok {
            env.result.context("worker response missing result")
        } else {
            bail!(
                env.error
                    .unwrap_or_else(|| "font worker failed".to_string())
            )
        }
    }
}
