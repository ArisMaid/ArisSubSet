use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, bail};
use chrono::{Duration as ChronoDuration, Utc};
use sqlx::Row;

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
    let row = sqlx::query("SELECT root_label, relative_path FROM subtitle_files WHERE id = ?")
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
    sqlx::query(
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
    let rows = sqlx::query(
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
    let row = sqlx::query("SELECT source_path, backup_path FROM backups WHERE id = ?")
        .bind(backup_id)
        .fetch_one(&state.db.pool)
        .await?;
    let source_path: String = row.get("source_path");
    let backup_path: String = row.get("backup_path");
    if !Path::new(&backup_path).exists() {
        bail!("backup file is missing: {backup_path}");
    }
    tokio::fs::copy(&backup_path, &source_path).await?;
    state.events.emit(
        "backup",
        "ok",
        format!("已恢复备份：{source_path} <- {backup_path}"),
    );
    Ok(())
}

async fn delete_record(state: &Arc<AppState>, id: i64) -> anyhow::Result<()> {
    sqlx::query("DELETE FROM backups WHERE id = ?")
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
