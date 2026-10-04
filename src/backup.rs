use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::sqlx::Row;
use anyhow::{Context, bail};
use chrono::{Duration as ChronoDuration, Utc};

use crate::state::AppState;

#[derive(Debug, Default, serde::Serialize)]
pub struct BackupPruneSummary {
    pub expired: usize,
    pub removed_files: usize,
    pub missing_files: usize,
    pub failed: usize,
}

pub async fn create(
    state: &Arc<AppState>,
    subtitle_id: i64,
    source: &Path,
    source_sha: &str,
) -> anyhow::Result<PathBuf> {
    let row =
        crate::sqlx::query("SELECT root_label, relative_path FROM subtitle_files WHERE id = ?")
            .bind(subtitle_id)
            .fetch_one(&state.db.pool)
            .await?;
    let root_label: String = row.get("root_label");
    let relative_path: String = row.get("relative_path");
    let rel = Path::new(&relative_path);
    let parent = rel.parent().unwrap_or_else(|| Path::new(""));
    let stem = rel
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("subtitle");
    let extension = rel
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or("ass");
    let timestamp = Utc::now().format("%Y%m%d-%H%M%S").to_string();
    let sha8 = &source_sha[..source_sha.len().min(8)];
    let suffix = format!(".{timestamp}.{sha8}.{extension}");
    let stem_limit = 240usize.saturating_sub(suffix.len()).max(32);
    let backup_name = format!("{}{}", truncate_utf8_bytes(stem, stem_limit), suffix);
    let backup_dir = state.config.backup_dir.join(root_label).join(parent);
    tokio::fs::create_dir_all(&backup_dir).await?;
    let backup_path = backup_dir.join(backup_name);
    tokio::fs::copy(source, &backup_path).await?;
    crate::sqlx::query(
        "INSERT INTO backups(subtitle_id, source_path, backup_path, source_sha256, created_at) VALUES(?, ?, ?, ?, ?)",
    )
    .bind(subtitle_id)
    .bind(source.to_string_lossy().to_string())
    .bind(backup_path.to_string_lossy().to_string())
    .bind(source_sha)
    .bind(Utc::now().to_rfc3339())
    .execute(&state.db.pool)
    .await?;
    Ok(backup_path)
}

pub async fn prune_expired(state: &Arc<AppState>) -> anyhow::Result<BackupPruneSummary> {
    let retention_days = state.config.backup_retention_days;
    if retention_days == 0 {
        return Ok(BackupPruneSummary::default());
    }
    let retention_days = retention_days.min(365_000) as i64;
    let cutoff = Utc::now()
        .checked_sub_signed(ChronoDuration::days(retention_days))
        .unwrap_or(chrono::DateTime::<Utc>::MIN_UTC)
        .to_rfc3339();
    let rows = crate::sqlx::query(
        "SELECT id, backup_path FROM backups WHERE created_at < ? ORDER BY created_at ASC, id ASC",
    )
    .bind(cutoff)
    .fetch_all(&state.db.pool)
    .await?;
    let backup_root = tokio::fs::canonicalize(&state.config.backup_dir)
        .await
        .with_context(|| {
            format!(
                "canonicalize backup root {}",
                state.config.backup_dir.display()
            )
        })?;
    let mut summary = BackupPruneSummary {
        expired: rows.len(),
        ..BackupPruneSummary::default()
    };

    for row in rows {
        let id: i64 = row.get("id");
        let raw_path: String = row.get("backup_path");
        let path = PathBuf::from(&raw_path);
        let metadata = match tokio::fs::symlink_metadata(&path).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                delete_record(state, id).await?;
                summary.missing_files += 1;
                continue;
            }
            Err(error) => {
                summary.failed += 1;
                tracing::warn!(path = %path.display(), %error, "failed to inspect expired backup");
                continue;
            }
        };
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            summary.failed += 1;
            tracing::warn!(path = %path.display(), "expired backup is not a regular file");
            continue;
        }
        let canonical_path = match tokio::fs::canonicalize(&path).await {
            Ok(path) => path,
            Err(error) => {
                summary.failed += 1;
                tracing::warn!(path = %path.display(), %error, "failed to resolve expired backup");
                continue;
            }
        };
        if !canonical_path.starts_with(&backup_root) {
            summary.failed += 1;
            tracing::warn!(path = %path.display(), "refusing to prune backup outside backup root");
            continue;
        }
        match tokio::fs::remove_file(&path).await {
            Ok(()) => {
                delete_record(state, id).await?;
                summary.removed_files += 1;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                delete_record(state, id).await?;
                summary.missing_files += 1;
            }
            Err(error) => {
                summary.failed += 1;
                tracing::warn!(path = %path.display(), %error, "failed to remove expired backup");
            }
        }
    }
    Ok(summary)
}

pub async fn restore(state: &Arc<AppState>, backup_id: i64) -> anyhow::Result<()> {
    let _scan_guard = state
        .try_begin_scan()
        .context("扫描正在运行，请等待扫描结束后恢复")?;
    if !state.scan_interval().await.is_zero() || !state.conversion_paused().await {
        bail!(
            "恢复前请关闭定时扫描并暂停新转换任务，等待当前任务结束；恢复后按需手动转换或重新开启扫描。"
        );
    }
    let mut tx = state.db.pool.begin().await?;
    // Serialize against job insertion until the replacement and state reset finish.
    crate::sqlx::query(
        "UPDATE subtitle_files SET id=id WHERE id=(SELECT subtitle_id FROM backups WHERE id=?)",
    )
    .bind(backup_id)
    .execute(&mut *tx)
    .await?;
    let row = crate::sqlx::query("SELECT source_path, backup_path FROM backups WHERE id = ?")
        .bind(backup_id)
        .fetch_one(&mut *tx)
        .await?;
    let source_path: String = row.get("source_path");
    let backup_path: String = row.get("backup_path");
    let active: i64 = crate::sqlx::query_scalar(
        "SELECT COUNT(*) FROM jobs WHERE path=? AND status IN ('queued', 'running')",
    )
    .bind(&source_path)
    .fetch_one(&mut *tx)
    .await?;
    if active > 0 {
        bail!("该字幕仍有排队或运行中的任务。请取消待执行任务，并等待运行中的任务结束后恢复。");
    }
    if !Path::new(&backup_path).exists() {
        bail!("backup file is missing: {backup_path}");
    }
    let bytes = tokio::fs::read(&backup_path).await?;
    crate::processor::write_replace(Path::new(&source_path), &bytes).await?;
    let meta = tokio::fs::metadata(&source_path).await?;
    let mtime = meta
        .modified()?
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs() as i64;
    crate::sqlx::query("UPDATE subtitle_files SET size=?, mtime=?, sha256='', last_status='restored', last_config_hash=NULL, last_processed_at=NULL, last_font_index_revision=NULL, missing_fonts='[]', error=NULL, analysis=NULL, analysis_size=NULL, analysis_mtime=NULL WHERE path=?")
        .bind(meta.len() as i64)
        .bind(mtime)
        .bind(&source_path)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    state.events.emit(
        "backup",
        "ok",
        format!("已恢复备份：{source_path} <- {backup_path}"),
    );
    Ok(())
}

async fn delete_record(state: &Arc<AppState>, id: i64) -> anyhow::Result<()> {
    crate::sqlx::query("DELETE FROM backups WHERE id = ?")
        .bind(id)
        .execute(&state.db.pool)
        .await?;
    Ok(())
}

fn truncate_utf8_bytes(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }
    let end = value
        .char_indices()
        .map(|(index, _)| index)
        .take_while(|index| *index <= max_bytes)
        .last()
        .unwrap_or(0);
    value[..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncation_keeps_utf8_boundaries() {
        assert_eq!(truncate_utf8_bytes("abc字幕def", 7), "abc字");
        assert_eq!(truncate_utf8_bytes("short", 20), "short");
    }
}
