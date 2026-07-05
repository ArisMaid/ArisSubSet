use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use sqlx::Row;
use tokio::sync::{RwLock, mpsc};

use crate::auth::Session;
use crate::config::{Config, ProcessingOptions};
use crate::db::Db;
use crate::events::EventBus;
use crate::font_worker::FontWorkerPool;

pub struct AppState {
    pub config: Arc<Config>,
    pub db: Db,
    pub events: EventBus,
    pub workers: FontWorkerPool,
    pub job_tx: mpsc::Sender<i64>,
    pub sessions: RwLock<HashMap<String, Session>>,
    runtime_options: RwLock<ProcessingOptions>,
    scan_interval: RwLock<Duration>,
    conversion_parallelism: RwLock<usize>,
    scan_paused: RwLock<bool>,
    scan_cancel_requested: RwLock<bool>,
    conversion_paused: RwLock<bool>,
    conversion_cancel_requested: RwLock<bool>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ControlStatus {
    pub scan_paused: bool,
    pub scan_cancel_requested: bool,
    pub conversion_paused: bool,
    pub conversion_cancel_requested: bool,
    pub conversion_parallelism: usize,
}

impl AppState {
    pub fn new(
        config: Arc<Config>,
        db: Db,
        events: EventBus,
        workers: FontWorkerPool,
        job_tx: mpsc::Sender<i64>,
    ) -> Self {
        let runtime_options = config.options.clone();
        let scan_interval = config.scan_interval;
        let conversion_parallelism = config.max_concurrent_jobs;
        Self {
            config,
            db,
            events,
            workers,
            job_tx,
            sessions: RwLock::new(HashMap::new()),
            runtime_options: RwLock::new(runtime_options),
            scan_interval: RwLock::new(scan_interval),
            conversion_parallelism: RwLock::new(conversion_parallelism),
            scan_paused: RwLock::new(false),
            scan_cancel_requested: RwLock::new(false),
            conversion_paused: RwLock::new(false),
            conversion_cancel_requested: RwLock::new(false),
        }
    }

    pub async fn load_runtime_settings(&self) -> anyhow::Result<()> {
        if let Some(raw) = self.setting("processing_options").await? {
            if let Ok(options) = serde_json::from_str::<ProcessingOptions>(&raw) {
                *self.runtime_options.write().await = options;
            }
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
            .await
    }

    pub async fn controls(&self) -> ControlStatus {
        ControlStatus {
            scan_paused: *self.scan_paused.read().await,
            scan_cancel_requested: *self.scan_cancel_requested.read().await,
            conversion_paused: *self.conversion_paused.read().await,
            conversion_cancel_requested: *self.conversion_cancel_requested.read().await,
            conversion_parallelism: *self.conversion_parallelism.read().await,
        }
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
        *self.scan_paused.write().await = paused;
        if !paused {
            *self.scan_cancel_requested.write().await = false;
        }
    }

    pub async fn request_scan_cancel(&self) {
        *self.scan_cancel_requested.write().await = true;
        *self.scan_paused.write().await = false;
    }

    pub async fn clear_scan_cancel(&self) {
        *self.scan_cancel_requested.write().await = false;
    }

    pub async fn wait_for_scan_turn(&self) -> bool {
        loop {
            if *self.scan_cancel_requested.read().await {
                return false;
            }
            if !*self.scan_paused.read().await {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
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
        let row = sqlx::query("SELECT value FROM runtime_settings WHERE key = ?")
            .bind(key)
            .fetch_optional(&self.db.pool)
            .await?;
        Ok(row.map(|row| row.get("value")))
    }

    async fn save_setting(&self, key: &str, value: &str) -> anyhow::Result<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
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
