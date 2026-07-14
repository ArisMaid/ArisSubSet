use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::sqlx::Row;
use anyhow::Context;
use chrono::Utc;
use tokio::io::AsyncWriteExt;

use crate::state::AppState;

#[derive(Debug, serde::Serialize)]
pub struct FailedJobLogExport {
    pub path: String,
    pub count: usize,
}

#[derive(Debug, serde::Serialize)]
struct FailedJobLogEntry<'a> {
    ts: &'a str,
    job_id: i64,
    subtitle_id: Option<i64>,
    mode: &'a str,
    file_name: String,
    path: &'a str,
    error: &'a str,
}

pub async fn export(state: &Arc<AppState>) -> anyhow::Result<FailedJobLogExport> {
    let _guard = state.failed_log_lock.lock().await;
    let rows = crate::sqlx::query(
        r#"
SELECT id, subtitle_id, path, mode, message
FROM jobs
WHERE status='failed'
ORDER BY id ASC
"#,
    )
    .fetch_all(&state.db.pool)
    .await?;
    let timestamp = Utc::now().to_rfc3339();
    let log_dir = log_dir(state);
    tokio::fs::create_dir_all(&log_dir).await?;
    let log_path = log_dir.join(format!(
        "failed-jobs-export-{}.jsonl",
        Utc::now().format("%Y%m%d-%H%M%S")
    ));
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .await
        .with_context(|| format!("open failed job export {}", log_path.display()))?;
    for row in &rows {
        let path: String = row.get("path");
        let mode: String = row.get("mode");
        let error: Option<String> = row.get("message");
        let entry = FailedJobLogEntry {
            ts: &timestamp,
            job_id: row.get("id"),
            subtitle_id: Some(row.get("subtitle_id")),
            mode: &mode,
            file_name: file_name_from_path(&path),
            path: &path,
            error: error.as_deref().unwrap_or("unknown error"),
        };
        write_entry(&mut file, &entry).await?;
    }
    Ok(FailedJobLogExport {
        path: log_path.to_string_lossy().to_string(),
        count: rows.len(),
    })
}

pub async fn append(
    state: &Arc<AppState>,
    job_id: i64,
    subtitle_id: Option<i64>,
    path: &str,
    mode: &str,
    error: &str,
    timestamp: &str,
) -> anyhow::Result<()> {
    let _guard = state.failed_log_lock.lock().await;
    let log_dir = log_dir(state);
    tokio::fs::create_dir_all(&log_dir).await?;
    let log_path = log_dir.join(format!("failed-jobs-{}.jsonl", Utc::now().format("%Y%m%d")));
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .await
        .with_context(|| format!("open failed job log {}", log_path.display()))?;
    let entry = FailedJobLogEntry {
        ts: timestamp,
        job_id,
        subtitle_id,
        mode,
        file_name: file_name_from_path(path),
        path,
        error,
    };
    write_entry(&mut file, &entry).await
}

async fn write_entry(
    file: &mut tokio::fs::File,
    entry: &FailedJobLogEntry<'_>,
) -> anyhow::Result<()> {
    let mut line = serde_json::to_vec(entry)?;
    line.push(b'\n');
    file.write_all(&line).await?;
    file.flush().await?;
    Ok(())
}

fn log_dir(state: &Arc<AppState>) -> PathBuf {
    state.config.data_dir.join("error-logs")
}

fn file_name_from_path(path: &str) -> String {
    Path::new(path)
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| path.to_string())
}
