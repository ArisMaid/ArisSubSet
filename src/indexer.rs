use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use crate::sqlx::{QueryBuilder, Row, Sqlite, Transaction};
use anyhow::anyhow;
use chrono::Utc;
use tokio::task::JoinSet;

use crate::ass::normalize_lookup_name;
use crate::font_worker::InspectResult;
use crate::fs_walk::{DiscoveredFile, WalkControl, WalkEvent, WalkOptions, spawn_file_walk};
use crate::models::FontFaceInfo;
use crate::state::{AppState, OperationGuard};

const FONT_WALK_OPTIONS: WalkOptions = WalkOptions {
    follow_links: true,
    extensions: &["ttf", "otf", "ttc", "otc", "woff", "woff2"],
    ignored_directories: &[],
};
const INDEX_WRITE_BATCH_SIZE: usize = 500;

pub fn spawn_initial_index(state: Arc<AppState>) {
    tokio::spawn(async move {
        state.events.emit("index", "info", "开始初始化字体索引");
        match rebuild_index(state.clone()).await {
            Ok(summary) => state.events.emit(
                "index",
                "ok",
                format!(
                    "索引就绪：扫描 {}，更新 {}，跳过 {}，失败 {}，回退 {}，耗时 {}ms",
                    summary.scanned,
                    summary.indexed,
                    summary.skipped,
                    summary.failed,
                    summary.fallback_used,
                    summary.walk_ms + summary.inspect_ms + summary.write_ms
                ),
            ),
            Err(err) => state
                .events
                .emit("index", "err", format!("字体索引失败：{err:#}")),
        }
    });
}

#[derive(Debug, Default, serde::Serialize)]
pub struct IndexSummary {
    pub scanned: usize,
    pub indexed: usize,
    pub skipped: usize,
    pub failed: usize,
    pub fallback_used: usize,
    pub pruned: usize,
    pub walk_ms: u128,
    pub inspect_ms: u128,
    pub write_ms: u128,
}

#[derive(Clone, Debug)]
struct FontMeta {
    path: PathBuf,
    path_s: String,
    size: i64,
    mtime: i64,
    format: String,
    existing_id: Option<i64>,
}

#[derive(Debug)]
struct ExistingMeta {
    id: i64,
    size: i64,
    mtime: i64,
    status: String,
}

#[derive(Debug)]
struct IndexedFont {
    meta: FontMeta,
    faces: Vec<FontFaceInfo>,
}

#[derive(Debug)]
struct FailedFont {
    meta: FontMeta,
    error: String,
}

#[derive(Debug)]
struct InspectOutcome {
    inspect: InspectResult,
    used_fallback: bool,
}

#[derive(Debug)]
struct NameInsert {
    face_id: i64,
    name: String,
    normalized: String,
    kind: String,
}

pub async fn rebuild_index(state: Arc<AppState>) -> anyhow::Result<IndexSummary> {
    let operation = state
        .try_begin_index()
        .ok_or_else(|| anyhow!("font index rebuild is already running"))?;
    rebuild_index_reserved(state, operation).await
}

pub async fn rebuild_index_reserved(
    state: Arc<AppState>,
    _operation: OperationGuard,
) -> anyhow::Result<IndexSummary> {
    let existing = load_existing_font_meta(&state).await?;
    let mut summary = IndexSummary::default();
    let mut changed_batch = Vec::with_capacity(INDEX_WRITE_BATCH_SIZE);
    let mut seen = HashSet::new();
    let mut authoritative_roots = Vec::new();
    let walk_started = Instant::now();

    for root in &state.config.font_dirs {
        let (mut walk_events, walk_handle) =
            spawn_file_walk(root.clone(), FONT_WALK_OPTIONS, WalkControl::new());
        while let Some(event) = walk_events.recv().await {
            let file = match event {
                WalkEvent::File(file) => file,
                WalkEvent::Error { path, message } => {
                    summary.failed += 1;
                    state.events.emit(
                        "index",
                        "warn",
                        format!("字体目录遍历失败：{}：{message}", path.display()),
                    );
                    continue;
                }
            };
            let path_s = file.path.to_string_lossy().to_string();
            if !seen.insert(path_s) {
                summary.skipped += 1;
                continue;
            }
            summary.scanned += 1;
            let mut meta = font_meta(file);
            if let Some(old) = existing.get(&meta.path_s) {
                if old.size == meta.size && old.mtime == meta.mtime && old.status == "ok" {
                    summary.skipped += 1;
                    continue;
                }
                meta.existing_id = Some(old.id);
            }
            changed_batch.push(meta);
            if changed_batch.len() >= INDEX_WRITE_BATCH_SIZE {
                inspect_and_write_changed_fonts(
                    state.clone(),
                    std::mem::take(&mut changed_batch),
                    &mut summary,
                )
                .await?;
            }
        }
        let walk_result = walk_handle.await?;
        if walk_result.complete {
            authoritative_roots.push(root.clone());
        }
    }

    summary.walk_ms = walk_started
        .elapsed()
        .as_millis()
        .saturating_sub(summary.inspect_ms)
        .saturating_sub(summary.write_ms);
    inspect_and_write_changed_fonts(state.clone(), changed_batch, &mut summary).await?;
    let write_started = Instant::now();
    if !authoritative_roots.is_empty() {
        summary.pruned = prune_stale_fonts(&state, &existing, &seen, &authoritative_roots).await?;
    }
    if summary.indexed > 0 || summary.failed > 0 || summary.pruned > 0 {
        state.bump_font_index_revision().await?;
    }
    summary.write_ms += write_started.elapsed().as_millis();
    Ok(summary)
}

async fn load_existing_font_meta(
    state: &Arc<AppState>,
) -> anyhow::Result<HashMap<String, ExistingMeta>> {
    let rows = crate::sqlx::query("SELECT id, path, size, mtime, status FROM font_files")
        .fetch_all(&state.db.pool)
        .await?;
    let mut out = HashMap::with_capacity(rows.len());
    for row in rows {
        out.insert(
            row.get("path"),
            ExistingMeta {
                size: row.get("size"),
                mtime: row.get("mtime"),
                status: row.get("status"),
                id: row.get("id"),
            },
        );
    }
    Ok(out)
}

async fn prune_stale_fonts(
    state: &Arc<AppState>,
    existing: &HashMap<String, ExistingMeta>,
    seen: &HashSet<String>,
    authoritative_roots: &[PathBuf],
) -> anyhow::Result<usize> {
    let stale_ids: Vec<i64> = existing
        .iter()
        .filter(|(path, _)| !seen.contains(*path) && path_is_under_roots(path, authoritative_roots))
        .map(|(_, meta)| meta.id)
        .collect();
    if stale_ids.is_empty() {
        return Ok(0);
    }

    let mut pruned = 0usize;
    for chunk in stale_ids.chunks(500) {
        let mut qb = QueryBuilder::new("DELETE FROM font_files WHERE id IN (");
        let mut separated = qb.separated(", ");
        for id in chunk {
            separated.push_bind(id);
        }
        separated.push_unseparated(")");
        let result = qb.build().execute(&state.db.pool).await?;
        pruned += result.rows_affected() as usize;
    }
    if pruned > 0 {
        state.events.emit(
            "index",
            "info",
            format!("pruned stale font index rows: {pruned}"),
        );
    }
    Ok(pruned)
}

fn path_is_under_roots(path: &str, roots: &[PathBuf]) -> bool {
    roots.iter().any(|root| Path::new(path).starts_with(root))
}

async fn inspect_and_write_changed_fonts(
    state: Arc<AppState>,
    changed: Vec<FontMeta>,
    summary: &mut IndexSummary,
) -> anyhow::Result<()> {
    if changed.is_empty() {
        return Ok(());
    }
    let started = Instant::now();
    let write_before = summary.write_ms;
    let concurrency = state.config.max_index_concurrency;
    let mut tasks = JoinSet::new();
    let mut indexed = Vec::new();
    let mut failed = Vec::new();

    for meta in changed {
        while tasks.len() >= concurrency {
            collect_inspect_task(&mut tasks, summary, &mut indexed, &mut failed).await;
            summary.write_ms +=
                flush_index_batch_if_ready(&state, &mut indexed, &mut failed, false).await?;
        }
        let st = state.clone();
        tasks.spawn(async move {
            let result = inspect_font_for_index(&st, &meta.path).await;
            (meta, result)
        });
    }

    while !tasks.is_empty() {
        collect_inspect_task(&mut tasks, summary, &mut indexed, &mut failed).await;
        summary.write_ms +=
            flush_index_batch_if_ready(&state, &mut indexed, &mut failed, false).await?;
    }
    summary.write_ms += flush_index_batch_if_ready(&state, &mut indexed, &mut failed, true).await?;
    let write_elapsed = summary.write_ms.saturating_sub(write_before);
    summary.inspect_ms += started.elapsed().as_millis().saturating_sub(write_elapsed);
    Ok(())
}

async fn flush_index_batch_if_ready(
    state: &Arc<AppState>,
    indexed: &mut Vec<IndexedFont>,
    failed: &mut Vec<FailedFont>,
    force: bool,
) -> anyhow::Result<u128> {
    if !force && indexed.len() + failed.len() < INDEX_WRITE_BATCH_SIZE {
        return Ok(0);
    }
    if indexed.is_empty() && failed.is_empty() {
        return Ok(0);
    }
    let started = Instant::now();
    write_index_results(state, std::mem::take(indexed), std::mem::take(failed)).await?;
    Ok(started.elapsed().as_millis())
}

async fn collect_inspect_task(
    tasks: &mut JoinSet<(FontMeta, anyhow::Result<InspectOutcome>)>,
    summary: &mut IndexSummary,
    indexed: &mut Vec<IndexedFont>,
    failed: &mut Vec<FailedFont>,
) {
    let Some(joined) = tasks.join_next().await else {
        return;
    };
    match joined {
        Ok((meta, Ok(outcome))) => {
            summary.indexed += 1;
            if outcome.used_fallback {
                summary.fallback_used += 1;
            }
            indexed.push(IndexedFont {
                meta,
                faces: outcome.inspect.faces,
            });
        }
        Ok((meta, Err(err))) => {
            summary.failed += 1;
            failed.push(FailedFont {
                meta,
                error: format!("{err:#}"),
            });
        }
        Err(err) => {
            summary.failed += 1;
            tracing::warn!("font index task failed: {err:#}");
        }
    }
}

async fn inspect_font_for_index(
    state: &Arc<AppState>,
    path: &Path,
) -> anyhow::Result<InspectOutcome> {
    match crate::font_inspect::inspect_font(path).await {
        Ok(faces) => Ok(InspectOutcome {
            inspect: InspectResult { faces },
            used_fallback: false,
        }),
        Err(fast_err) => {
            let inspect = state.workers.inspect_font(path).await.map_err(|fallback_err| {
                anyhow!(
                    "fast sfnt inspect failed: {fast_err:#}; fontTools fallback failed: {fallback_err:#}"
                )
            })?;
            Ok(InspectOutcome {
                inspect,
                used_fallback: true,
            })
        }
    }
}

async fn write_index_results(
    state: &Arc<AppState>,
    indexed: Vec<IndexedFont>,
    failed: Vec<FailedFont>,
) -> anyhow::Result<()> {
    if indexed.is_empty() && failed.is_empty() {
        return Ok(());
    }

    let indexed_at = Utc::now().to_rfc3339();
    let mut tx = state.db.pool.begin().await?;
    let mut names = Vec::new();

    for item in indexed {
        let file_id = upsert_font_ok(&mut tx, &item.meta, &indexed_at).await?;
        if item.meta.existing_id.is_some() {
            clear_font_faces(&mut tx, file_id).await?;
        }

        for face in item.faces {
            let face_id = insert_face(&mut tx, file_id, &face).await?;
            collect_face_names(face_id, &face, &mut names);
        }
    }

    insert_font_names(&mut tx, &names).await?;

    for item in failed {
        if let Some(file_id) = item.meta.existing_id {
            clear_font_faces(&mut tx, file_id).await?;
        }
        upsert_font_error(&mut tx, &item.meta, &item.error, &indexed_at).await?;
    }

    tx.commit().await?;
    Ok(())
}

async fn upsert_font_ok(
    tx: &mut Transaction<'_, Sqlite>,
    meta: &FontMeta,
    indexed_at: &str,
) -> anyhow::Result<i64> {
    if let Some(id) = meta.existing_id {
        crate::sqlx::query(
            r#"
UPDATE font_files SET
  size=?,
  mtime=?,
  quick_hash='',
  full_hash='',
  format=?,
  status='ok',
  error=NULL,
  indexed_at=?
WHERE id=?
"#,
        )
        .bind(meta.size)
        .bind(meta.mtime)
        .bind(&meta.format)
        .bind(indexed_at)
        .bind(id)
        .execute(&mut **tx)
        .await?;
        return Ok(id);
    }

    let result = crate::sqlx::query(
        r#"
INSERT INTO font_files(path, size, mtime, quick_hash, full_hash, format, status, error, indexed_at)
VALUES(?, ?, ?, '', '', ?, 'ok', NULL, ?)
"#,
    )
    .bind(&meta.path_s)
    .bind(meta.size)
    .bind(meta.mtime)
    .bind(&meta.format)
    .bind(indexed_at)
    .execute(&mut **tx)
    .await?;
    Ok(result.last_insert_rowid())
}

async fn upsert_font_error(
    tx: &mut Transaction<'_, Sqlite>,
    meta: &FontMeta,
    error: &str,
    indexed_at: &str,
) -> anyhow::Result<()> {
    if let Some(id) = meta.existing_id {
        crate::sqlx::query(
            r#"
UPDATE font_files SET
  size=?,
  mtime=?,
  quick_hash='',
  full_hash='',
  format=?,
  status='error',
  error=?,
  indexed_at=?
WHERE id=?
"#,
        )
        .bind(meta.size)
        .bind(meta.mtime)
        .bind(&meta.format)
        .bind(error)
        .bind(indexed_at)
        .bind(id)
        .execute(&mut **tx)
        .await?;
        return Ok(());
    }

    crate::sqlx::query(
        r#"
INSERT INTO font_files(path, size, mtime, quick_hash, full_hash, format, status, error, indexed_at)
VALUES(?, ?, ?, '', '', ?, 'error', ?, ?)
"#,
    )
    .bind(&meta.path_s)
    .bind(meta.size)
    .bind(meta.mtime)
    .bind(&meta.format)
    .bind(error)
    .bind(indexed_at)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn clear_font_faces(tx: &mut Transaction<'_, Sqlite>, file_id: i64) -> anyhow::Result<()> {
    crate::sqlx::query(
        "DELETE FROM font_names WHERE face_id IN (SELECT id FROM font_faces WHERE file_id = ?)",
    )
    .bind(file_id)
    .execute(&mut **tx)
    .await?;
    crate::sqlx::query("DELETE FROM font_faces WHERE file_id = ?")
        .bind(file_id)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn insert_face(
    tx: &mut Transaction<'_, Sqlite>,
    file_id: i64,
    face: &FontFaceInfo,
) -> anyhow::Result<i64> {
    let result = crate::sqlx::query(
        r#"
INSERT INTO font_faces(file_id, ttc_index, family, full_name, postscript_name, subfamily, version, weight, italic)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
"#,
    )
    .bind(file_id)
    .bind(face.ttc_index)
    .bind(&face.family)
    .bind(&face.full_name)
    .bind(&face.postscript_name)
    .bind(&face.subfamily)
    .bind(&face.version)
    .bind(face.weight)
    .bind(if face.italic { 1 } else { 0 })
    .execute(&mut **tx)
    .await?;
    Ok(result.last_insert_rowid())
}

fn collect_face_names(face_id: i64, face: &FontFaceInfo, out: &mut Vec<NameInsert>) {
    let mut seen = HashSet::new();
    for name in &face.names {
        push_name(face_id, &mut seen, out, &name.name, &name.kind);
    }
    add_name_if_present(face_id, &mut seen, out, face.family.as_deref(), "family");
    add_name_if_present(face_id, &mut seen, out, face.full_name.as_deref(), "full");
    add_name_if_present(
        face_id,
        &mut seen,
        out,
        face.postscript_name.as_deref(),
        "postscript",
    );
}

fn add_name_if_present(
    face_id: i64,
    seen: &mut HashSet<(String, String)>,
    out: &mut Vec<NameInsert>,
    value: Option<&str>,
    kind: &str,
) {
    if let Some(name) = value {
        push_name(face_id, seen, out, name, kind);
    }
}

fn push_name(
    face_id: i64,
    seen: &mut HashSet<(String, String)>,
    out: &mut Vec<NameInsert>,
    name: &str,
    kind: &str,
) {
    let normalized = normalize_lookup_name(name);
    if normalized.is_empty() {
        return;
    }
    let key = (kind.to_string(), normalized.clone());
    if !seen.insert(key) {
        return;
    }
    out.push(NameInsert {
        face_id,
        name: name.to_string(),
        normalized,
        kind: kind.to_string(),
    });
}

async fn insert_font_names(
    tx: &mut Transaction<'_, Sqlite>,
    names: &[NameInsert],
) -> anyhow::Result<()> {
    for chunk in names.chunks(500) {
        let mut qb: QueryBuilder<'_, Sqlite> =
            QueryBuilder::new("INSERT INTO font_names(face_id, name, normalized, kind) ");
        qb.push_values(chunk, |mut b, name| {
            b.push_bind(name.face_id)
                .push_bind(&name.name)
                .push_bind(&name.normalized)
                .push_bind(&name.kind);
        });
        qb.build().execute(&mut **tx).await?;
    }
    Ok(())
}

fn font_meta(file: DiscoveredFile) -> FontMeta {
    let path_s = file.path.to_string_lossy().to_string();
    let format = file
        .path
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    FontMeta {
        path: file.path,
        path_s,
        size: file.size,
        mtime: file.mtime,
        format,
        existing_id: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stale_pruning_is_scoped_to_fully_scanned_roots() {
        let roots = vec![PathBuf::from("/fonts/ready")];
        assert!(path_is_under_roots("/fonts/ready/a.ttf", &roots));
        assert!(!path_is_under_roots("/fonts/unavailable/a.ttf", &roots));
        assert!(!path_is_under_roots("/fonts/ready-other/a.ttf", &roots));
    }
}
