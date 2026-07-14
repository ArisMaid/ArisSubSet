use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use chrono::Utc;
use sha2::{Digest, Sha256};
use sqlx::{Row, Sqlite, Transaction};
use tokio::io::AsyncReadExt;
use tokio::task::JoinSet;

use crate::ass::parse_font_subset_comments;
use crate::config::root_label;
use crate::fs_walk::{DiscoveredFile, WalkEvent, WalkOptions, spawn_file_walk};
use crate::models::JobMode;
use crate::state::{AppState, OperationGuard};

const SCAN_PROGRESS_INTERVAL: Duration = Duration::from_secs(10);
const SCAN_PROGRESS_CANDIDATES: usize = 250;
const SUBSET_MARKER_PREFIX_LIMIT: usize = 1024 * 1024;
const ENQUEUE_BATCH_SIZE: usize = 200;
const SUBTITLE_WALK_OPTIONS: WalkOptions = WalkOptions {
    follow_links: false,
    extensions: &["ass", "ssa"],
    ignored_directories: &[
        ".sync",
        "@eadir",
        "#recycle",
        "$recycle.bin",
        "system volume information",
        "incomplete",
        "speedtest",
    ],
};

pub fn spawn_scheduler(state: Arc<AppState>) {
    tokio::spawn(async move {
        let mut announced_disabled = false;
        let mut schedule_changes = state.subscribe_scan_schedule();
        loop {
            let interval = state.scan_interval().await;
            if interval.is_zero() {
                if !announced_disabled {
                    state.events.emit("scan", "info", "定时扫描已关闭");
                    announced_disabled = true;
                }
                if schedule_changes.changed().await.is_err() {
                    break;
                }
                continue;
            }
            announced_disabled = false;
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                changed = schedule_changes.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    continue;
                }
            }
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
    pub cancelled: bool,
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
    last_font_index_revision: Option<i64>,
}

#[derive(Debug, Clone)]
struct QueueCandidate {
    meta: SubtitleCandidate,
    sha256: String,
}

#[derive(Debug)]
struct CandidateInspection {
    already_subsetted: bool,
    sha256: Option<String>,
}

type FilterTask = (PathBuf, anyhow::Result<Option<QueueCandidate>>);

pub async fn scan_now(state: Arc<AppState>) -> anyhow::Result<ScanSummary> {
    let operation = state
        .try_begin_scan()
        .ok_or_else(|| anyhow!("subtitle scan is already running"))?;
    scan_now_reserved(state, operation).await
}

pub async fn scan_now_reserved(
    state: Arc<AppState>,
    _operation: OperationGuard,
) -> anyhow::Result<ScanSummary> {
    state.clear_scan_cancel().await;
    state.begin_scan_progress().await;
    let result = scan_now_inner(state.clone()).await;
    match &result {
        Ok(summary) if summary.cancelled => state.finish_scan_progress("cancelled").await,
        Ok(_) => state.finish_scan_progress("completed").await,
        Err(_) => state.finish_scan_progress("failed").await,
    }
    state.clear_scan_cancel().await;
    result
}

async fn scan_now_inner(state: Arc<AppState>) -> anyhow::Result<ScanSummary> {
    let mut summary = ScanSummary::default();
    let config_hash = state.config_hash().await;
    let font_index_revision = state.font_index_revision().await;
    let roots = effective_watch_dirs(&state).await?;
    let mut last_progress = Instant::now();
    let mut candidates = Vec::new();
    let mut discovered_paths = HashSet::new();

    for (idx, root) in roots.iter().enumerate() {
        if !state.wait_for_scan_turn().await {
            state.events.emit("scan", "warn", "scan cancelled");
            summary.cancelled = true;
            return Ok(summary);
        }
        let label = root_label(root, idx);
        state.events.emit(
            "scan",
            "info",
            format!("scanning root {}: {}", idx + 1, root.display()),
        );
        let (mut walk_events, walk_handle) =
            spawn_file_walk(root.clone(), SUBTITLE_WALK_OPTIONS, state.scan_control());
        loop {
            if !state.wait_for_scan_turn().await {
                drop(walk_events);
                walk_handle.abort();
                state.events.emit("scan", "warn", "scan cancelled");
                summary.cancelled = true;
                return Ok(summary);
            }
            let event = tokio::select! {
                event = walk_events.recv() => event,
                _ = tokio::time::sleep(Duration::from_millis(100)) => continue,
            };
            let Some(event) = event else {
                break;
            };
            let file = match event {
                WalkEvent::File(file) => file,
                WalkEvent::Error { path, message } => {
                    summary.failed += 1;
                    state.events.emit(
                        "scan",
                        "warn",
                        format!("scan walk failed: {}: {message}", path.display()),
                    );
                    continue;
                }
            };
            let candidate = subtitle_candidate(root, &label, file);
            if !discovered_paths.insert(candidate.path_s.clone()) {
                summary.skipped += 1;
                continue;
            }
            summary.seen += 1;
            candidates.push(candidate);
            emit_scan_progress(
                &state,
                "scan discover progress",
                &summary,
                &mut last_progress,
                false,
            )
            .await;
        }
        let walk_result = walk_handle.await.context("subtitle walk task failed")?;
        if walk_result.cancelled {
            state.events.emit("scan", "warn", "scan cancelled");
            summary.cancelled = true;
            return Ok(summary);
        }
        emit_scan_progress(
            &state,
            "scan discover progress",
            &summary,
            &mut last_progress,
            true,
        )
        .await;
    }

    let existing = Arc::new(load_existing_subtitles(&state).await?);
    let active = Arc::new(load_active_subtitle_ids(&state).await?);
    let mut queue_candidates = Vec::new();
    let filter_total = candidates.len();
    let filter_concurrency = state.config.max_scan_concurrency;
    let mut filter_tasks = JoinSet::new();
    let mut filtered = 0usize;
    for candidate in candidates {
        if !state.wait_for_scan_turn().await {
            filter_tasks.abort_all();
            state.events.emit("scan", "warn", "scan cancelled");
            summary.cancelled = true;
            return Ok(summary);
        }
        while filter_tasks.len() >= filter_concurrency {
            collect_filter_task(
                &state,
                &mut filter_tasks,
                &mut queue_candidates,
                &mut summary,
                &mut filtered,
                filter_total,
                &mut last_progress,
            )
            .await;
        }
        let st = state.clone();
        let existing = existing.clone();
        let active = active.clone();
        let config_hash = config_hash.clone();
        let path = candidate.path.clone();
        filter_tasks.spawn(async move {
            let result = filter_candidate(
                &st,
                &candidate,
                &existing,
                &active,
                &config_hash,
                font_index_revision,
            )
            .await;
            (path, result)
        });
    }
    while !filter_tasks.is_empty() {
        if !state.wait_for_scan_turn().await {
            filter_tasks.abort_all();
            state.events.emit("scan", "warn", "scan cancelled");
            summary.cancelled = true;
            return Ok(summary);
        }
        collect_filter_task(
            &state,
            &mut filter_tasks,
            &mut queue_candidates,
            &mut summary,
            &mut filtered,
            filter_total,
            &mut last_progress,
        )
        .await;
    }
    emit_scan_stage_progress(
        &state,
        "filtering",
        "scan filter progress",
        filtered,
        filter_total,
        queue_candidates.len(),
        &summary,
        &mut last_progress,
        true,
    )
    .await;

    let ready = queue_candidates.len();
    enqueue_candidates(&state, queue_candidates, &mut summary, &mut last_progress).await?;
    emit_scan_stage_progress(
        &state,
        "enqueuing",
        "scan enqueue progress",
        ready,
        ready,
        ready,
        &summary,
        &mut last_progress,
        true,
    )
    .await;

    Ok(summary)
}

async fn emit_scan_progress(
    state: &Arc<AppState>,
    label: &str,
    summary: &ScanSummary,
    last_progress: &mut Instant,
    force: bool,
) {
    let candidate_tick = summary.seen > 0 && summary.seen.is_multiple_of(SCAN_PROGRESS_CANDIDATES);
    if !force && !candidate_tick && last_progress.elapsed() < SCAN_PROGRESS_INTERVAL {
        return;
    }
    *last_progress = Instant::now();
    state
        .update_scan_progress(
            "discovering",
            summary.seen,
            0,
            summary.seen,
            0,
            summary.queued,
            summary.skipped,
            summary.failed,
        )
        .await;
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
async fn emit_scan_stage_progress(
    state: &Arc<AppState>,
    stage: &str,
    label: &str,
    done: usize,
    total: usize,
    ready: usize,
    summary: &ScanSummary,
    last_progress: &mut Instant,
    force: bool,
) {
    let candidate_tick = done > 0 && done.is_multiple_of(SCAN_PROGRESS_CANDIDATES);
    if !force && !candidate_tick && last_progress.elapsed() < SCAN_PROGRESS_INTERVAL {
        return;
    }
    *last_progress = Instant::now();
    state
        .update_scan_progress(
            stage,
            done,
            total,
            summary.seen,
            ready,
            summary.queued,
            summary.skipped,
            summary.failed,
        )
        .await;
    state.events.emit(
        "scan",
        "info",
        format!(
            "{label}: {done}/{total}, ready {ready}, queued {}, skipped {}, failed {}",
            summary.queued, summary.skipped, summary.failed
        ),
    );
}

#[allow(clippy::too_many_arguments)]
async fn collect_filter_task(
    state: &Arc<AppState>,
    tasks: &mut JoinSet<FilterTask>,
    queue_candidates: &mut Vec<QueueCandidate>,
    summary: &mut ScanSummary,
    filtered: &mut usize,
    total: usize,
    last_progress: &mut Instant,
) {
    let Some(joined) = tasks.join_next().await else {
        return;
    };
    *filtered += 1;
    match joined {
        Ok((_, Ok(Some(candidate)))) => queue_candidates.push(candidate),
        Ok((_, Ok(None))) => summary.skipped += 1,
        Ok((path, Err(err))) => {
            summary.failed += 1;
            state.events.emit(
                "scan",
                "warn",
                format!("scan inspect failed: {}: {err:#}", path.display()),
            );
        }
        Err(err) => {
            summary.failed += 1;
            state
                .events
                .emit("scan", "warn", format!("scan filter task failed: {err:#}"));
        }
    }
    emit_scan_stage_progress(
        state,
        "filtering",
        "scan filter progress",
        *filtered,
        total,
        queue_candidates.len(),
        summary,
        last_progress,
        false,
    )
    .await;
}

fn subtitle_candidate(root: &Path, root_label: &str, file: DiscoveredFile) -> SubtitleCandidate {
    let path_s = file.path.to_string_lossy().to_string();
    let relative_path = file
        .path
        .strip_prefix(root)
        .unwrap_or(&file.path)
        .to_string_lossy()
        .replace('\\', "/");
    SubtitleCandidate {
        path: file.path,
        path_s,
        root_label: root_label.to_string(),
        relative_path,
        size: file.size,
        mtime: file.mtime,
    }
}

async fn load_existing_subtitles(
    state: &Arc<AppState>,
) -> anyhow::Result<HashMap<String, ExistingSubtitle>> {
    let rows = sqlx::query(
        "SELECT id, path, size, mtime, sha256, last_config_hash, last_status, last_font_index_revision FROM subtitle_files",
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
                last_font_index_revision: row.get("last_font_index_revision"),
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
    font_index_revision: u64,
) -> anyhow::Result<Option<QueueCandidate>> {
    if let Some(old) = existing.get(&candidate.path_s) {
        if active.contains(&old.id) {
            return Ok(None);
        }
        if old.size == candidate.size
            && old.mtime == candidate.mtime
            && old.last_config_hash.as_deref() == Some(config_hash)
            && (old.last_status.as_deref() == Some("success")
                || (old.last_status.as_deref() == Some("partial")
                    && old.last_font_index_revision == Some(font_index_revision as i64)))
        {
            return Ok(None);
        }
    }

    let inspection = inspect_candidate_file(&candidate.path).await?;
    if inspection.already_subsetted {
        let fingerprint = metadata_fingerprint(candidate);
        mark_already_subsetted(
            state,
            candidate,
            &fingerprint,
            config_hash,
            font_index_revision,
        )
        .await?;
        return Ok(None);
    }

    let sha256 = inspection
        .sha256
        .context("scan inspection did not produce a subtitle hash")?;
    if let Some(old) = existing.get(&candidate.path_s)
        && old.sha256 == sha256
        && old.last_config_hash.as_deref() == Some(config_hash)
        && (old.last_status.as_deref() == Some("success")
            || (old.last_status.as_deref() == Some("partial")
                && old.last_font_index_revision == Some(font_index_revision as i64)))
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
    font_index_revision: u64,
) -> anyhow::Result<()> {
    let now = Utc::now().to_rfc3339();
    sqlx::query(
        r#"
INSERT INTO subtitle_files(path, root_label, relative_path, size, mtime, sha256, last_config_hash, last_status, last_processed_at, missing_fonts, error, last_font_index_revision)
VALUES(?, ?, ?, ?, ?, ?, ?, 'success', ?, '[]', NULL, ?)
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
  error=NULL,
  last_font_index_revision=excluded.last_font_index_revision
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
    .bind(font_index_revision as i64)
    .execute(&state.db.pool)
    .await?;
    Ok(())
}

fn metadata_fingerprint(candidate: &SubtitleCandidate) -> String {
    format!("metadata:{}:{}", candidate.size, candidate.mtime)
}

async fn inspect_candidate_file(path: &Path) -> anyhow::Result<CandidateInspection> {
    let mut f = tokio::fs::File::open(path).await?;
    let mut hash = Sha256::new();
    let mut prefix = Vec::with_capacity(SUBSET_MARKER_PREFIX_LIMIT);
    let mut buffer = vec![0u8; 256 * 1024];
    loop {
        let n = f.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
        if prefix.len() < SUBSET_MARKER_PREFIX_LIMIT {
            let keep = n.min(SUBSET_MARKER_PREFIX_LIMIT - prefix.len());
            prefix.extend_from_slice(&buffer[..keep]);
            if contains_subset_marker(&prefix) {
                return Ok(CandidateInspection {
                    already_subsetted: true,
                    sha256: None,
                });
            }
        }
    }
    Ok(CandidateInspection {
        already_subsetted: contains_subset_marker(&prefix),
        sha256: Some(hex::encode(hash.finalize())),
    })
}

fn contains_subset_marker(bytes: &[u8]) -> bool {
    let text = decode_prefix_lossy(bytes);
    !parse_font_subset_comments(&text).is_empty()
        || text.to_ascii_lowercase().contains("assdrawsubset")
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

async fn enqueue_candidates(
    state: &Arc<AppState>,
    candidates: Vec<QueueCandidate>,
    summary: &mut ScanSummary,
    last_progress: &mut Instant,
) -> anyhow::Result<()> {
    let total = candidates.len();
    let mut done = 0usize;
    for chunk in candidates.chunks(ENQUEUE_BATCH_SIZE) {
        if !state.wait_for_scan_turn().await {
            state.events.emit("scan", "warn", "scan cancelled");
            summary.cancelled = true;
            return Ok(());
        }
        let now = Utc::now().to_rfc3339();
        let mut tx = state.db.pool.begin().await?;
        let mut job_ids = Vec::with_capacity(chunk.len());
        for candidate in chunk {
            if let Some(job_id) = enqueue_candidate_tx(&mut tx, candidate, &now).await? {
                job_ids.push(job_id);
            }
        }
        tx.commit().await?;
        for job_id in &job_ids {
            state
                .job_tx
                .send(*job_id)
                .await
                .context("job queue closed")?;
        }
        summary.queued += job_ids.len();
        summary.skipped += chunk.len().saturating_sub(job_ids.len());
        done += chunk.len();
        emit_scan_stage_progress(
            state,
            "enqueuing",
            "scan enqueue progress",
            done,
            total,
            total,
            summary,
            last_progress,
            false,
        )
        .await;
    }
    Ok(())
}

async fn enqueue_candidate_tx(
    tx: &mut Transaction<'_, Sqlite>,
    candidate: &QueueCandidate,
    queued_at: &str,
) -> anyhow::Result<Option<i64>> {
    let meta = &candidate.meta;
    let subtitle_id: i64 = sqlx::query_scalar(
        r#"
INSERT INTO subtitle_files(path, root_label, relative_path, size, mtime, sha256)
VALUES(?, ?, ?, ?, ?, ?)
ON CONFLICT(path) DO UPDATE SET
  root_label=excluded.root_label,
  relative_path=excluded.relative_path,
  size=excluded.size,
  mtime=excluded.mtime,
  sha256=excluded.sha256
RETURNING id
"#,
    )
    .bind(&meta.path_s)
    .bind(&meta.root_label)
    .bind(&meta.relative_path)
    .bind(meta.size)
    .bind(meta.mtime)
    .bind(&candidate.sha256)
    .fetch_one(&mut **tx)
    .await?;
    let job_id = sqlx::query_scalar(
        r#"
INSERT INTO jobs(subtitle_id, path, mode, status, queued_at)
SELECT ?, ?, ?, 'queued', ?
WHERE NOT EXISTS (
  SELECT 1 FROM jobs WHERE subtitle_id = ? AND status IN ('queued', 'running')
)
RETURNING id
"#,
    )
    .bind(subtitle_id)
    .bind(&meta.path_s)
    .bind(JobMode::Subset.as_str())
    .bind(queued_at)
    .bind(subtitle_id)
    .fetch_optional(&mut **tx)
    .await?;
    Ok(job_id)
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
    let job_id: Option<i64> = sqlx::query_scalar(
        r#"
INSERT INTO jobs(subtitle_id, path, mode, status, queued_at)
SELECT ?, ?, ?, 'queued', ?
WHERE NOT EXISTS (
  SELECT 1 FROM jobs WHERE subtitle_id = ? AND status IN ('queued', 'running')
)
RETURNING id
"#,
    )
    .bind(subtitle_id)
    .bind(path)
    .bind(mode.as_str())
    .bind(now)
    .bind(subtitle_id)
    .fetch_optional(&state.db.pool)
    .await?;
    let job_id = job_id.ok_or_else(|| anyhow!("subtitle already has an active job"))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    #[tokio::test]
    async fn candidate_inspection_stops_hashing_after_subset_marker() {
        let dir = tempfile::tempdir().unwrap();
        let marked_path = dir.path().join("marked.ass");
        tokio::fs::write(
            &marked_path,
            b"[Script Info]\n; Font Subset: AS123 - Example\n[Events]\n",
        )
        .await
        .unwrap();
        let marked = inspect_candidate_file(&marked_path).await.unwrap();
        assert!(marked.already_subsetted);
        assert!(marked.sha256.is_none());

        let plain_path = dir.path().join("plain.ass");
        let plain_bytes = b"[Script Info]\n[Events]\nDialogue: 0,0,1,Default,Hello\n";
        tokio::fs::write(&plain_path, plain_bytes).await.unwrap();
        let plain = inspect_candidate_file(&plain_path).await.unwrap();
        assert!(!plain.already_subsetted);
        let mut expected = Sha256::new();
        expected.update(plain_bytes);
        assert_eq!(plain.sha256.unwrap(), hex::encode(expected.finalize()));
    }

    #[tokio::test]
    async fn enqueue_is_atomic_when_an_active_job_exists() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE subtitle_files (id INTEGER PRIMARY KEY AUTOINCREMENT, path TEXT NOT NULL UNIQUE, root_label TEXT NOT NULL, relative_path TEXT NOT NULL, size INTEGER NOT NULL, mtime INTEGER NOT NULL, sha256 TEXT NOT NULL)",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "CREATE TABLE jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, subtitle_id INTEGER NOT NULL, path TEXT NOT NULL, mode TEXT NOT NULL, status TEXT NOT NULL, queued_at TEXT NOT NULL)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let candidate = QueueCandidate {
            meta: SubtitleCandidate {
                path: PathBuf::from("/watch/test.ass"),
                path_s: "/watch/test.ass".to_string(),
                root_label: "watch".to_string(),
                relative_path: "test.ass".to_string(),
                size: 10,
                mtime: 20,
            },
            sha256: "hash".to_string(),
        };

        let mut first_tx = pool.begin().await.unwrap();
        assert!(
            enqueue_candidate_tx(&mut first_tx, &candidate, "now")
                .await
                .unwrap()
                .is_some()
        );
        first_tx.commit().await.unwrap();

        let mut second_tx = pool.begin().await.unwrap();
        assert!(
            enqueue_candidate_tx(&mut second_tx, &candidate, "later")
                .await
                .unwrap()
                .is_none()
        );
        second_tx.commit().await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM jobs")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 1);
    }
}
