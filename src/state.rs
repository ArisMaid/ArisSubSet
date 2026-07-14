use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Weak;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::sqlx::Row;
use chrono::Utc;
use tokio::sync::{Mutex, RwLock, mpsc, watch};

use crate::auth::{LoginRateLimiter, Session};
use crate::config::{Config, ProcessingOptions};
use crate::db::Db;
use crate::events::EventBus;
use crate::font_worker::FontWorkerPool;
use crate::fs_walk::WalkControl;
use crate::metrics::RuntimeMetrics;

pub struct AppState {
    pub config: Arc<Config>,
    pub db: Db,
    pub events: EventBus,
    pub workers: FontWorkerPool,
    pub metrics: Arc<RuntimeMetrics>,
    pub job_tx: mpsc::Sender<i64>,
    pub sessions: RwLock<HashMap<String, Session>>,
    pub login_limiter: LoginRateLimiter,
    pub failed_log_lock: Mutex<()>,
    pub(crate) cache_maintenance_lock: Mutex<()>,
    runtime_options: RwLock<ProcessingOptions>,
    scan_interval: RwLock<Duration>,
    scan_schedule_version: watch::Sender<u64>,
    font_index_revision: RwLock<u64>,
    conversion_parallelism: RwLock<usize>,
    scan_control: WalkControl,
    conversion_paused: RwLock<bool>,
    conversion_cancel_requested: RwLock<bool>,
    scan_running: Arc<AtomicBool>,
    index_running: Arc<AtomicBool>,
    scan_progress: RwLock<ScanProgress>,
    cache_locks: Mutex<HashMap<String, Weak<Mutex<()>>>>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ControlStatus {
    pub scan_paused: bool,
    pub scan_cancel_requested: bool,
    pub conversion_paused: bool,
    pub conversion_cancel_requested: bool,
    pub conversion_parallelism: usize,
    pub scan_running: bool,
    pub index_running: bool,
    pub scan_progress: ScanProgress,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct ScanProgress {
    pub stage: String,
    pub current: usize,
    pub total: usize,
    pub seen: usize,
    pub ready: usize,
    pub queued: usize,
    pub skipped: usize,
    pub failed: usize,
    pub started_at: Option<String>,
    pub updated_at: Option<String>,
}

pub struct OperationGuard {
    running: Arc<AtomicBool>,
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
    }
}

impl AppState {
    pub fn new(
        config: Arc<Config>,
        db: Db,
        events: EventBus,
        workers: FontWorkerPool,
        metrics: Arc<RuntimeMetrics>,
        job_tx: mpsc::Sender<i64>,
    ) -> Self {
        let runtime_options = config.options.clone();
        let scan_interval = config.scan_interval;
        let (scan_schedule_version, _) = watch::channel(0);
        let conversion_parallelism = config.max_concurrent_jobs;
        Self {
            config,
            db,
            events,
            workers,
            metrics,
            job_tx,
            sessions: RwLock::new(HashMap::new()),
            login_limiter: LoginRateLimiter::new(),
            failed_log_lock: Mutex::new(()),
            cache_maintenance_lock: Mutex::new(()),
            runtime_options: RwLock::new(runtime_options),
            scan_interval: RwLock::new(scan_interval),
            scan_schedule_version,
            font_index_revision: RwLock::new(0),
            conversion_parallelism: RwLock::new(conversion_parallelism),
            scan_control: WalkControl::new(),
            conversion_paused: RwLock::new(false),
            conversion_cancel_requested: RwLock::new(false),
            scan_running: Arc::new(AtomicBool::new(false)),
            index_running: Arc::new(AtomicBool::new(false)),
            scan_progress: RwLock::new(ScanProgress::default()),
            cache_locks: Mutex::new(HashMap::new()),
        }
    }

    pub async fn load_runtime_settings(&self) -> anyhow::Result<()> {
        if let Some(raw) = self.setting("processing_options").await?
            && let Ok(options) = serde_json::from_str::<ProcessingOptions>(&raw)
        {
            *self.runtime_options.write().await = options;
        }
        if let Some(raw) = self.setting("scan_interval_seconds").await?
            && let Ok(seconds) = raw.parse::<u64>()
        {
            *self.scan_interval.write().await = Duration::from_secs(seconds);
        }
        if let Some(raw) = self.setting("conversion_parallelism").await?
            && let Ok(value) = raw.parse::<usize>()
        {
            *self.conversion_parallelism.write().await = value.clamp(1, 32);
        }
        if let Some(raw) = self.setting("font_index_revision").await?
            && let Ok(value) = raw.parse::<u64>()
        {
            *self.font_index_revision.write().await = value;
        }
        Ok(())
    }

    pub async fn processing_options(&self) -> ProcessingOptions {
        self.runtime_options.read().await.clone()
    }

    pub async fn config_hash(&self) -> String {
        self.runtime_options.read().await.config_hash()
    }

    pub async fn set_processing_option(
        &self,
        key: &str,
        value: bool,
    ) -> anyhow::Result<ProcessingOptions> {
        let options = {
            let mut options = self.runtime_options.write().await;
            match key {
                "embed_external_fonts" => options.embed_external_fonts = value,
                "embed_system_fonts" => options.embed_system_fonts = value,
                "include_ascii" => options.include_ascii = value,
                "multi_weight" => options.multi_weight = value,
                "randomize_font_names" => options.randomize_font_names = value,
                "draw_subset" => options.draw_subset = value,
                "full_font_embed" => options.full_font_embed = value,
                "fallback_full_font_embed" => options.fallback_full_font_embed = value,
                "variable_fonts" => options.variable_fonts = value,
                _ => anyhow::bail!("unknown processing option: {key}"),
            }
            options.clone()
        };
        self.save_setting("processing_options", &serde_json::to_string(&options)?)
            .await?;
        Ok(options)
    }

    pub async fn scan_interval(&self) -> Duration {
        *self.scan_interval.read().await
    }

    pub async fn set_scan_interval(&self, interval: Duration) -> anyhow::Result<()> {
        *self.scan_interval.write().await = interval;
        self.save_setting("scan_interval_seconds", &interval.as_secs().to_string())
            .await?;
        self.scan_schedule_version
            .send_modify(|version| *version = version.wrapping_add(1));
        Ok(())
    }

    pub fn subscribe_scan_schedule(&self) -> watch::Receiver<u64> {
        self.scan_schedule_version.subscribe()
    }

    pub async fn controls(&self) -> ControlStatus {
        ControlStatus {
            scan_paused: self.scan_control.is_paused(),
            scan_cancel_requested: self.scan_control.is_cancelled(),
            conversion_paused: *self.conversion_paused.read().await,
            conversion_cancel_requested: *self.conversion_cancel_requested.read().await,
            conversion_parallelism: *self.conversion_parallelism.read().await,
            scan_running: self.scan_running.load(Ordering::Acquire),
            index_running: self.index_running.load(Ordering::Acquire),
            scan_progress: self.scan_progress.read().await.clone(),
        }
    }

    pub fn try_begin_scan(&self) -> Option<OperationGuard> {
        try_begin_operation(&self.scan_running)
    }

    pub fn try_begin_index(&self) -> Option<OperationGuard> {
        try_begin_operation(&self.index_running)
    }

    pub async fn begin_scan_progress(&self) {
        let now = Utc::now().to_rfc3339();
        *self.scan_progress.write().await = ScanProgress {
            stage: "discovering".to_string(),
            started_at: Some(now.clone()),
            updated_at: Some(now),
            ..ScanProgress::default()
        };
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn update_scan_progress(
        &self,
        stage: &str,
        current: usize,
        total: usize,
        seen: usize,
        ready: usize,
        queued: usize,
        skipped: usize,
        failed: usize,
    ) {
        let mut progress = self.scan_progress.write().await;
        progress.stage = stage.to_string();
        progress.current = current;
        progress.total = total;
        progress.seen = seen;
        progress.ready = ready;
        progress.queued = queued;
        progress.skipped = skipped;
        progress.failed = failed;
        progress.updated_at = Some(Utc::now().to_rfc3339());
    }

    pub async fn finish_scan_progress(&self, stage: &str) {
        let mut progress = self.scan_progress.write().await;
        progress.stage = stage.to_string();
        progress.updated_at = Some(Utc::now().to_rfc3339());
    }

    pub async fn cache_lock(&self, key: &str) -> Arc<Mutex<()>> {
        let mut locks = self.cache_locks.lock().await;
        locks.retain(|_, lock| lock.strong_count() > 0);
        if let Some(lock) = locks.get(key).and_then(Weak::upgrade) {
            return lock;
        }
        let lock = Arc::new(Mutex::new(()));
        locks.insert(key.to_string(), Arc::downgrade(&lock));
        lock
    }

    pub async fn font_index_revision(&self) -> u64 {
        *self.font_index_revision.read().await
    }

    pub async fn bump_font_index_revision(&self) -> anyhow::Result<u64> {
        let revision = {
            let mut value = self.font_index_revision.write().await;
            *value = value.saturating_add(1);
            *value
        };
        self.save_setting("font_index_revision", &revision.to_string())
            .await?;
        Ok(revision)
    }

    pub async fn conversion_parallelism(&self) -> usize {
        *self.conversion_parallelism.read().await
    }

    pub async fn set_conversion_parallelism(&self, value: usize) -> anyhow::Result<usize> {
        let value = value.clamp(1, 32);
        *self.conversion_parallelism.write().await = value;
        self.save_setting("conversion_parallelism", &value.to_string())
            .await?;
        Ok(value)
    }

    pub async fn set_scan_paused(&self, paused: bool) {
        self.scan_control.set_paused(paused);
        if !paused {
            self.scan_control.clear_cancel();
        }
    }

    pub async fn request_scan_cancel(&self) {
        self.scan_control.cancel();
    }

    pub async fn clear_scan_cancel(&self) {
        self.scan_control.clear_cancel();
    }

    pub async fn wait_for_scan_turn(&self) -> bool {
        self.scan_control.wait_async().await
    }

    pub fn scan_control(&self) -> WalkControl {
        self.scan_control.clone()
    }

    pub async fn set_conversion_paused(&self, paused: bool) {
        *self.conversion_paused.write().await = paused;
        if !paused {
            *self.conversion_cancel_requested.write().await = false;
        }
    }

    pub async fn request_conversion_cancel(&self) {
        *self.conversion_cancel_requested.write().await = true;
        *self.conversion_paused.write().await = false;
    }

    pub async fn clear_conversion_cancel(&self) {
        *self.conversion_cancel_requested.write().await = false;
    }

    pub async fn conversion_cancel_requested(&self) -> bool {
        *self.conversion_cancel_requested.read().await
    }

    pub async fn conversion_paused(&self) -> bool {
        *self.conversion_paused.read().await
    }

    async fn setting(&self, key: &str) -> anyhow::Result<Option<String>> {
        let row = crate::sqlx::query("SELECT value FROM runtime_settings WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.db.pool)
            .await?;
        Ok(row.map(|row| row.get("value")))
    }

    async fn save_setting(&self, key: &str, value: &str) -> anyhow::Result<()> {
        let now = Utc::now().to_rfc3339();
        crate::sqlx::query(
            r#"
INSERT INTO runtime_settings(key, value, updated_at)
VALUES(?, ?, ?)
ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
"#,
        )
        .bind(key)
        .bind(value)
        .bind(now)
        .execute(&self.db.pool)
        .await?;
        Ok(())
    }
}

fn try_begin_operation(running: &Arc<AtomicBool>) -> Option<OperationGuard> {
    running
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .ok()
        .map(|_| OperationGuard {
            running: running.clone(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_guard_allows_only_one_owner_and_releases_on_drop() {
        let running = Arc::new(AtomicBool::new(false));
        let first = try_begin_operation(&running).expect("first operation should start");
        assert!(try_begin_operation(&running).is_none());
        drop(first);
        assert!(try_begin_operation(&running).is_some());
    }
}
