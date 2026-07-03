use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use chrono::Utc;
use sha2::{Digest, Sha256};
use sqlx::Row;
use tokio::io::AsyncReadExt;
use walkdir::WalkDir;

use crate::config::root_label;
use crate::models::JobMode;
use crate::state::AppState;

pub fn spawn_scheduler(state: Arc<AppState>) {
    tokio::spawn(async move {
        let mut announced_disabled = false;
        loop {
            let interval = state.scan_interval().await;
            if interval.is_zero() {
                if !announced_disabled {
                    state.events.emit("scan", "info", "定时扫描已关闭");
                    announced_disabled = true;
                }
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
            announced_disabled = false;
            tokio::time::sleep(interval).await;
            match scan_now(state.clone()).await {
                Ok(summary) => state.events.emit(
                    "scan",
                    "ok",
                    format!(
                        "定时扫描完成：发现 {}，入队 {}，跳过 {}，失败 {}",
                        summary.seen, summary.queued, summary.skipped, summary.failed
                    ),
                ),
                Err(err) => state
                    .events
                    .emit("scan", "err", format!("定时扫描失败：{err:#}")),
            }
        }
    });
}

#[derive(Debug, Default, serde::Serialize)]
pub struct ScanSummary {
    pub seen: usize,
    pub queued: usize,
    pub skipped: usize,
    pub failed: usize,
}

pub async fn scan_now(state: Arc<AppState>) -> anyhow::Result<ScanSummary> {
    let mut summary = ScanSummary::default();
    let config_hash = state.config_hash().await;
    let roots = effective_watch_dirs(&state).await?;
    for (idx, root) in roots.iter().enumerate() {
        if !root.exists() {
            state.events.emit(
                "scan",
                "warn",
                format!("监听目录不存在：{}", root.display()),
            );
            continue;
        }
        let label = root_label(root, idx);
        for entry in WalkDir::new(root)
            .follow_links(true)
            .into_iter()
            .filter_map(Result::ok)
        {
            if !entry.file_type().is_file() || !is_subtitle_path(entry.path()) {
                continue;
            }
            summary.seen += 1;
            match inspect_and_enqueue(&state, root, &label, entry.path(), &config_hash).await {
                Ok(EnqueueResult::Queued) => summary.queued += 1,
                Ok(EnqueueResult::Skipped) => summary.skipped += 1,
                Err(err) => {
                    summary.failed += 1;
                    state.events.emit(
                        "scan",
                        "warn",
                        format!("扫描失败：{}：{err:#}", entry.path().display()),
                    );
                }
            }
        }
    }
    Ok(summary)
}

pub async fn effective_watch_dirs(state: &Arc<AppState>) -> anyhow::Result<Vec<PathBuf>> {
    Ok(watch_dir_entries(state)
        .await?
        .into_iter()
        .map(|entry| entry.path)
        .collect())
}

#[derive(Debug, serde::Serialize)]
pub struct WatchDirEntry {
    pub path: PathBuf,
    pub removable: bool,
}

pub async fn watch_dir_entries(state: &Arc<AppState>) -> anyhow::Result<Vec<WatchDirEntry>> {
    let rows = sqlx::query("SELECT path FROM watch_dirs ORDER BY id ASC")
        .fetch_all(&state.db.pool)
        .await?;
    let mut dirs: Vec<WatchDirEntry> = state
        .config
        .watch_dirs
        .iter()
        .cloned()
        .map(|path| WatchDirEntry {
            path,
            removable: false,
        })
        .collect();
    for row in rows {
        let path: String = row.get("path");
        let path_buf = PathBuf::from(path);
        if !dirs.iter().any(|existing| existing.path == path_buf) {
            dirs.push(WatchDirEntry {
                path: path_buf,
                removable: true,
            });
        }
    }
    Ok(dirs)
}

pub async fn add_watch_dir(state: &Arc<AppState>, path: &Path) -> anyhow::Result<()> {
    let now = Utc::now().to_rfc3339();
    sqlx::query(
        "INSERT INTO watch_dirs(path, created_at) VALUES(?, ?) ON CONFLICT(path) DO NOTHING",
    )
    .bind(path.to_string_lossy().to_string())
    .bind(now)
    .execute(&state.db.pool)
    .await?;
    Ok(())
}

pub async fn remove_watch_dir(state: &Arc<AppState>, path: &Path) -> anyhow::Result<bool> {
    let result = sqlx::query("DELETE FROM watch_dirs WHERE path = ?")
        .bind(path.to_string_lossy().to_string())
        .execute(&state.db.pool)
        .await?;
    Ok(result.rows_affected() > 0)
}

pub async fn register_uploaded_subtitle(
    state: &Arc<AppState>,
    path: &Path,
    display_name: &str,
) -> anyhow::Result<i64> {
    let meta = tokio::fs::metadata(path).await?;
    let size = meta.len() as i64;
    let mtime = meta
        .modified()
        .ok()
        .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let sha = full_hash(path).await?;
    let path_s = path.to_string_lossy().to_string();
    sqlx::query(
        r#"
INSERT INTO subtitle_files(path, root_label, relative_path, size, mtime, sha256)
VALUES(?, 'uploads', ?, ?, ?, ?)
ON CONFLICT(path) DO UPDATE SET
  root_label=excluded.root_label,
  relative_path=excluded.relative_path,
  size=excluded.size,
  mtime=excluded.mtime,
  sha256=excluded.sha256
"#,
    )
    .bind(&path_s)
    .bind(display_name)
    .bind(size)
    .bind(mtime)
    .bind(sha)
    .execute(&state.db.pool)
    .await?;
    let subtitle_id: i64 = sqlx::query_scalar("SELECT id FROM subtitle_files WHERE path = ?")
        .bind(&path_s)
        .fetch_one(&state.db.pool)
        .await?;
    Ok(subtitle_id)
}

enum EnqueueResult {
    Queued,
    Skipped,
}

async fn inspect_and_enqueue(
    state: &Arc<AppState>,
    root: &Path,
    root_label: &str,
    path: &Path,
    config_hash: &str,
) -> anyhow::Result<EnqueueResult> {
    let meta = tokio::fs::metadata(path).await?;
    let size = meta.len() as i64;
    let mtime = meta
        .modified()
        .ok()
        .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let sha = full_hash(path).await?;
    let path_s = path.to_string_lossy().to_string();
    let rel = path
        .strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/");

    if let Some(row) = sqlx::query(
        "SELECT id, sha256, last_config_hash, last_status FROM subtitle_files WHERE path = ?",
    )
    .bind(&path_s)
    .fetch_optional(&state.db.pool)
    .await?
    {
        let subtitle_id: i64 = row.get("id");
        let old_sha: String = row.get("sha256");
        let old_cfg: Option<String> = row.get("last_config_hash");
        let old_status: Option<String> = row.get("last_status");
        if old_sha == sha
            && old_cfg.as_deref() == Some(config_hash)
            && matches!(old_status.as_deref(), Some("success" | "partial"))
        {
            return Ok(EnqueueResult::Skipped);
        }
        if has_active_job(state, subtitle_id).await? {
            return Ok(EnqueueResult::Skipped);
        }
    }

    sqlx::query(
        r#"
INSERT INTO subtitle_files(path, root_label, relative_path, size, mtime, sha256)
VALUES(?, ?, ?, ?, ?, ?)
ON CONFLICT(path) DO UPDATE SET
  root_label=excluded.root_label,
  relative_path=excluded.relative_path,
  size=excluded.size,
  mtime=excluded.mtime,
  sha256=excluded.sha256
"#,
    )
    .bind(&path_s)
    .bind(root_label)
    .bind(&rel)
    .bind(size)
    .bind(mtime)
    .bind(&sha)
    .execute(&state.db.pool)
    .await?;
    let subtitle_id: i64 = sqlx::query_scalar("SELECT id FROM subtitle_files WHERE path = ?")
        .bind(&path_s)
        .fetch_one(&state.db.pool)
        .await?;
    if has_active_job(state, subtitle_id).await? {
        return Ok(EnqueueResult::Skipped);
    }
    let now = Utc::now().to_rfc3339();
    let job_id = sqlx::query(
        "INSERT INTO jobs(subtitle_id, path, mode, status, queued_at) VALUES(?, ?, ?, 'queued', ?)",
    )
    .bind(subtitle_id)
    .bind(&path_s)
    .bind(JobMode::Subset.as_str())
    .bind(now)
    .execute(&state.db.pool)
    .await?
    .last_insert_rowid();
    state
        .job_tx
        .send(job_id)
        .await
        .context("job queue closed")?;
    state
        .events
        .emit("scan", "info", format!("已加入队列：{}", path.display()));
    Ok(EnqueueResult::Queued)
}

pub async fn enqueue_subtitle_id(
    state: &Arc<AppState>,
    subtitle_id: i64,
    mode: JobMode,
) -> anyhow::Result<i64> {
    let row = sqlx::query("SELECT path FROM subtitle_files WHERE id = ?")
        .bind(subtitle_id)
        .fetch_one(&state.db.pool)
        .await?;
    let path: String = row.get("path");
    let now = Utc::now().to_rfc3339();
    let job_id = sqlx::query(
        "INSERT INTO jobs(subtitle_id, path, mode, status, queued_at) VALUES(?, ?, ?, 'queued', ?)",
    )
    .bind(subtitle_id)
    .bind(path)
    .bind(mode.as_str())
    .bind(now)
    .execute(&state.db.pool)
    .await?
    .last_insert_rowid();
    state
        .job_tx
        .send(job_id)
        .await
        .context("job queue closed")?;
    Ok(job_id)
}

pub async fn retry_job(state: &Arc<AppState>, job_id: i64) -> anyhow::Result<i64> {
    let row = sqlx::query("SELECT subtitle_id, mode FROM jobs WHERE id = ?")
        .bind(job_id)
        .fetch_one(&state.db.pool)
        .await?;
    let subtitle_id: i64 = row.get("subtitle_id");
    let mode: String = row.get("mode");
    enqueue_subtitle_id(state, subtitle_id, JobMode::from_db(&mode)).await
}

async fn has_active_job(state: &Arc<AppState>, subtitle_id: i64) -> anyhow::Result<bool> {
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM jobs WHERE subtitle_id = ? AND status IN ('queued', 'running')",
    )
    .bind(subtitle_id)
    .fetch_one(&state.db.pool)
    .await?;
    Ok(count > 0)
}

pub fn is_subtitle_path(path: &Path) -> bool {
    matches!(
        path.extension()
            .and_then(|s| s.to_str())
            .map(|s| s.to_ascii_lowercase())
            .as_deref(),
        Some("ass" | "ssa")
    )
}

async fn full_hash(path: &Path) -> anyhow::Result<String> {
    let mut f = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("open subtitle {}", path.display()))?;
    let mut h = Sha256::new();
    let mut buf = vec![0u8; 256 * 1024];
    loop {
        let n = f.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(hex::encode(h.finalize()))
}

#[allow(dead_code)]
fn _path_buf(path: &Path) -> PathBuf {
    path.to_path_buf()
}
