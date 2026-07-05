use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use chrono::Utc;
use sha2::{Digest, Sha256};
use sqlx::Row;
use tokio::io::AsyncReadExt;
use walkdir::WalkDir;

use crate::ass::parse_font_subset_comments;
use crate::config::root_label;
use crate::models::JobMode;
use crate::state::AppState;

const SCAN_PROGRESS_INTERVAL: Duration = Duration::from_secs(10);
const SCAN_PROGRESS_CANDIDATES: usize = 50;

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

#[derive(Debug, Clone)]
struct SubtitleCandidate {
    path: PathBuf,
    path_s: String,
    root_label: String,
    relative_path: String,
    size: i64,
    mtime: i64,
}

#[derive(Debug, Clone)]
struct ExistingSubtitle {
    id: i64,
    size: i64,
    mtime: i64,
    sha256: String,
    last_config_hash: Option<String>,
    last_status: Option<String>,
}

#[derive(Debug)]
struct QueueCandidate {
    meta: SubtitleCandidate,
    sha256: String,
}

pub async fn scan_now(state: Arc<AppState>) -> anyhow::Result<ScanSummary> {
    state.clear_scan_cancel().await;
    let mut summary = ScanSummary::default();
    let config_hash = state.config_hash().await;
    let roots = effective_watch_dirs(&state).await?;
    let mut last_progress = Instant::now();
    let mut candidates = Vec::new();

    for (idx, root) in roots.iter().enumerate() {
        if !state.wait_for_scan_turn().await {
            state.events.emit("scan", "warn", "scan cancelled");
            return Ok(summary);
        }
        if !root.exists() {
            state.events.emit(
                "scan",
                "warn",
                format!("监听目录不存在：{}", root.display()),
            );
            continue;
        }
        let label = root_label(root, idx);
        state.events.emit(
            "scan",
            "info",
            format!("scanning root {}: {}", idx + 1, root.display()),
        );
        for entry in WalkDir::new(root)
            .follow_links(false)
            .into_iter()
            .filter_entry(should_visit_scan_entry)
        {
            if !state.wait_for_scan_turn().await {
                state.events.emit("scan", "warn", "scan cancelled");
                return Ok(summary);
            }
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    summary.failed += 1;
                    state
                        .events
                        .emit("scan", "warn", format!("scan walk failed: {err:#}"));
                    continue;
                }
            };
            if !entry.file_type().is_file() || !is_subtitle_path(entry.path()) {
                continue;
            }
            summary.seen += 1;
            let candidate = match subtitle_candidate(root, &label, entry.path()).await {
                Ok(candidate) => candidate,
                Err(err) => {
                    summary.failed += 1;
                    state.events.emit(
                        "scan",
                        "warn",
                        format!("scan metadata failed: {}: {err:#}", entry.path().display()),
                    );
                    emit_scan_progress(
                        &state,
                        "scan discover progress",
                        &summary,
                        &mut last_progress,
                        false,
                    );
                    continue;
                }
            };
            candidates.push(candidate);
            emit_scan_progress(
                &state,
                "scan discover progress",
                &summary,
                &mut last_progress,
                false,
            );
        }
        emit_scan_progress(
            &state,
            "scan discover progress",
            &summary,
            &mut last_progress,
            true,
        );
    }

    let existing = load_existing_subtitles(&state).await?;
    let active = load_active_subtitle_ids(&state).await?;
    let mut queue_candidates = Vec::new();
    for (idx, candidate) in candidates.into_iter().enumerate() {
        if !state.wait_for_scan_turn().await {
            state.events.emit("scan", "warn", "scan cancelled");
            return Ok(summary);
        }
        match filter_candidate(&state, &candidate, &existing, &active, &config_hash).await {
            Ok(Some(queue_candidate)) => queue_candidates.push(queue_candidate),
            Ok(None) => summary.skipped += 1,
            Err(err) => {
                summary.failed += 1;
                state.events.emit(
                    "scan",
                    "warn",
                    format!("scan inspect failed: {}: {err:#}", candidate.path.display()),
                );
            }
        }
        emit_scan_stage_progress(
            &state,
            "scan filter progress",
            idx + 1,
            summary.seen,
            queue_candidates.len(),
            summary.skipped,
            summary.failed,
            &mut last_progress,
            false,
        );
    }
    emit_scan_stage_progress(
        &state,
        "scan filter progress",
        summary.seen,
        summary.seen,
        queue_candidates.len(),
        summary.skipped,
        summary.failed,
        &mut last_progress,
        true,
    );

    let ready = queue_candidates.len();
    for (idx, candidate) in queue_candidates.into_iter().enumerate() {
        if !state.wait_for_scan_turn().await {
            state.events.emit("scan", "warn", "scan cancelled");
            return Ok(summary);
        }
        match enqueue_candidate(&state, candidate).await {
            Ok(EnqueueResult::Queued) => summary.queued += 1,
            Ok(EnqueueResult::Skipped) => summary.skipped += 1,
            Err(err) => {
                summary.failed += 1;
                state
                    .events
                    .emit("scan", "warn", format!("enqueue failed: {err:#}"));
            }
        }
        emit_scan_stage_progress(
            &state,
            "scan enqueue progress",
            idx + 1,
            ready,
            summary.queued,
            summary.skipped,
            summary.failed,
            &mut last_progress,
            false,
        );
    }
    emit_scan_stage_progress(
        &state,
        "scan enqueue progress",
        ready,
        ready,
        summary.queued,
        summary.skipped,
        summary.failed,
        &mut last_progress,
        true,
    );

    state.clear_scan_cancel().await;
    Ok(summary)
}

fn should_visit_scan_entry(entry: &walkdir::DirEntry) -> bool {
    if entry.depth() == 0 || !entry.file_type().is_dir() {
        return true;
    }
    !is_ignored_scan_dir(entry.file_name().to_string_lossy().as_ref())
}

fn is_ignored_scan_dir(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        ".sync"
            | "@eadir"
            | "#recycle"
            | "$recycle.bin"
            | "system volume information"
            | "incomplete"
            | "speedtest"
    )
}

fn emit_scan_progress(
    state: &Arc<AppState>,
    label: &str,
    summary: &ScanSummary,
    last_progress: &mut Instant,
    force: bool,
) {
    let candidate_tick = summary.seen > 0 && summary.seen % SCAN_PROGRESS_CANDIDATES == 0;
    if !force && !candidate_tick && last_progress.elapsed() < SCAN_PROGRESS_INTERVAL {
        return;
    }
    *last_progress = Instant::now();
    state.events.emit(
        "scan",
        "info",
        format!(
            "{label}: seen {}, queued {}, skipped {}, failed {}",
            summary.seen, summary.queued, summary.skipped, summary.failed
        ),
    );
}

#[allow(clippy::too_many_arguments)]
fn emit_scan_stage_progress(
    state: &Arc<AppState>,
    label: &str,
    done: usize,
    total: usize,
    ready_or_queued: usize,
    skipped: usize,
    failed: usize,
    last_progress: &mut Instant,
    force: bool,
) {
    let candidate_tick = done > 0 && done % SCAN_PROGRESS_CANDIDATES == 0;
    if !force && !candidate_tick && last_progress.elapsed() < SCAN_PROGRESS_INTERVAL {
        return;
    }
    *last_progress = Instant::now();
    state.events.emit(
        "scan",
        "info",
        format!(
            "{label}: {done}/{total}, ready_or_queued {ready_or_queued}, skipped {skipped}, failed {failed}"
        ),
    );
}

async fn subtitle_candidate(
    root: &Path,
    root_label: &str,
    path: &Path,
) -> anyhow::Result<SubtitleCandidate> {
    let meta = tokio::fs::metadata(path).await?;
    let size = meta.len() as i64;
    let mtime = meta
        .modified()
        .ok()
        .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let path_s = path.to_string_lossy().to_string();
    let relative_path = path
        .strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/");
    Ok(SubtitleCandidate {
        path: path.to_path_buf(),
        path_s,
        root_label: root_label.to_string(),
        relative_path,
        size,
        mtime,
    })
}

async fn load_existing_subtitles(
    state: &Arc<AppState>,
) -> anyhow::Result<HashMap<String, ExistingSubtitle>> {
    let rows = sqlx::query(
        "SELECT id, path, size, mtime, sha256, last_config_hash, last_status FROM subtitle_files",
    )
    .fetch_all(&state.db.pool)
    .await?;
    let mut out = HashMap::with_capacity(rows.len());
    for row in rows {
        let path: String = row.get("path");
        out.insert(
            path,
            ExistingSubtitle {
                id: row.get("id"),
                size: row.get("size"),
                mtime: row.get("mtime"),
                sha256: row.get("sha256"),
                last_config_hash: row.get("last_config_hash"),
                last_status: row.get("last_status"),
            },
        );
    }
    Ok(out)
}

async fn load_active_subtitle_ids(state: &Arc<AppState>) -> anyhow::Result<HashSet<i64>> {
    let rows =
        sqlx::query("SELECT DISTINCT subtitle_id FROM jobs WHERE status IN ('queued', 'running')")
            .fetch_all(&state.db.pool)
            .await?;
    Ok(rows.into_iter().map(|row| row.get("subtitle_id")).collect())
}

async fn filter_candidate(
    state: &Arc<AppState>,
    candidate: &SubtitleCandidate,
    existing: &HashMap<String, ExistingSubtitle>,
    active: &HashSet<i64>,
    config_hash: &str,
) -> anyhow::Result<Option<QueueCandidate>> {
    if let Some(old) = existing.get(&candidate.path_s) {
        if active.contains(&old.id) {
            return Ok(None);
        }
        if old.size == candidate.size
            && old.mtime == candidate.mtime
            && old.last_config_hash.as_deref() == Some(config_hash)
            && matches!(old.last_status.as_deref(), Some("success" | "partial"))
        {
            return Ok(None);
        }
    }

    if detect_already_subsetted(&candidate.path).await? {
        let fingerprint = metadata_fingerprint(candidate);
        mark_already_subsetted(state, candidate, &fingerprint, config_hash).await?;
        return Ok(None);
    }

    let sha256 = full_hash(&candidate.path).await?;
    if let Some(old) = existing.get(&candidate.path_s)
        && old.sha256 == sha256
        && old.last_config_hash.as_deref() == Some(config_hash)
        && matches!(old.last_status.as_deref(), Some("success" | "partial"))
    {
        sync_subtitle_meta(state, candidate, &sha256).await?;
        return Ok(None);
    }
    Ok(Some(QueueCandidate {
        meta: candidate.clone(),
        sha256,
    }))
}

async fn sync_subtitle_meta(
    state: &Arc<AppState>,
    candidate: &SubtitleCandidate,
    sha256: &str,
) -> anyhow::Result<()> {
    sqlx::query(
        r#"
UPDATE subtitle_files
SET root_label=?, relative_path=?, size=?, mtime=?, sha256=?
WHERE path=?
"#,
    )
    .bind(&candidate.root_label)
    .bind(&candidate.relative_path)
    .bind(candidate.size)
    .bind(candidate.mtime)
    .bind(sha256)
    .bind(&candidate.path_s)
    .execute(&state.db.pool)
    .await?;
    Ok(())
}

async fn mark_already_subsetted(
    state: &Arc<AppState>,
    candidate: &SubtitleCandidate,
    sha256: &str,
    config_hash: &str,
) -> anyhow::Result<()> {
    let now = Utc::now().to_rfc3339();
    sqlx::query(
        r#"
INSERT INTO subtitle_files(path, root_label, relative_path, size, mtime, sha256, last_config_hash, last_status, last_processed_at, missing_fonts, error)
VALUES(?, ?, ?, ?, ?, ?, ?, 'success', ?, '[]', NULL)
ON CONFLICT(path) DO UPDATE SET
  root_label=excluded.root_label,
  relative_path=excluded.relative_path,
  size=excluded.size,
  mtime=excluded.mtime,
  sha256=excluded.sha256,
  last_config_hash=excluded.last_config_hash,
  last_status=excluded.last_status,
  last_processed_at=excluded.last_processed_at,
  missing_fonts=excluded.missing_fonts,
  error=NULL
"#,
    )
    .bind(&candidate.path_s)
    .bind(&candidate.root_label)
    .bind(&candidate.relative_path)
    .bind(candidate.size)
    .bind(candidate.mtime)
    .bind(sha256)
    .bind(config_hash)
    .bind(now)
    .execute(&state.db.pool)
    .await?;
    Ok(())
}

fn metadata_fingerprint(candidate: &SubtitleCandidate) -> String {
    format!("metadata:{}:{}", candidate.size, candidate.mtime)
}

async fn detect_already_subsetted(path: &Path) -> anyhow::Result<bool> {
    let mut f = tokio::fs::File::open(path).await?;
    let mut bytes = vec![0u8; 1024 * 1024];
    let n = f.read(&mut bytes).await?;
    bytes.truncate(n);
    let text = decode_prefix_lossy(&bytes);
    if !parse_font_subset_comments(&text).is_empty() {
        return Ok(true);
    }
    Ok(text.to_ascii_lowercase().contains("assdrawsubset"))
}

fn decode_prefix_lossy(bytes: &[u8]) -> String {
    if bytes.starts_with(&[0xFF, 0xFE]) {
        let body = &bytes[2..bytes.len() - (bytes.len() % 2)];
        let words: Vec<u16> = body
            .chunks_exact(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .collect();
        return String::from_utf16_lossy(&words);
    }
    if bytes.starts_with(&[0xFE, 0xFF]) {
        let body = &bytes[2..bytes.len() - (bytes.len() % 2)];
        let words: Vec<u16> = body
            .chunks_exact(2)
            .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]]))
            .collect();
        return String::from_utf16_lossy(&words);
    }
    let body = bytes.strip_prefix(&[0xEF, 0xBB, 0xBF]).unwrap_or(bytes);
    String::from_utf8_lossy(body).into_owned()
}

async fn enqueue_candidate(
    state: &Arc<AppState>,
    candidate: QueueCandidate,
) -> anyhow::Result<EnqueueResult> {
    let meta = candidate.meta;
    if let Some(row) = sqlx::query("SELECT id FROM subtitle_files WHERE path = ?")
        .bind(&meta.path_s)
        .fetch_optional(&state.db.pool)
        .await?
    {
        let subtitle_id: i64 = row.get("id");
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
    .bind(&meta.path_s)
    .bind(&meta.root_label)
    .bind(&meta.relative_path)
    .bind(meta.size)
    .bind(meta.mtime)
    .bind(&candidate.sha256)
    .execute(&state.db.pool)
    .await?;
    let subtitle_id: i64 = sqlx::query_scalar("SELECT id FROM subtitle_files WHERE path = ?")
        .bind(&meta.path_s)
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
    .bind(&meta.path_s)
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
        .emit("scan", "info", format!("queued: {}", meta.path.display()));
    Ok(EnqueueResult::Queued)
}

#[allow(dead_code)]
pub async fn scan_now_legacy(state: Arc<AppState>) -> anyhow::Result<ScanSummary> {
    let mut summary = ScanSummary::default();
    let config_hash = state.config_hash().await;
    let roots = effective_watch_dirs(&state).await?;
    for (idx, root) in roots.iter().enumerate() {
        if !root.exists() {
            state.events.emit(
                "scan",
                "warn",
                format!("鐩戝惉鐩綍涓嶅瓨鍦細{}", root.display()),
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
