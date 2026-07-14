use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::Context;
use filetime::{FileTime, set_file_mtime};
use uuid::Uuid;

use crate::state::AppState;

const CACHE_TOUCH_INTERVAL: Duration = Duration::from_secs(60 * 60);
const STALE_TEMP_AGE: Duration = Duration::from_secs(24 * 60 * 60);
const CACHE_TARGET_PERCENT: u64 = 90;

#[derive(Debug, Default, serde::Serialize)]
pub struct CacheMaintenanceSummary {
    pub files: u64,
    pub bytes: u64,
    pub evicted_files: u64,
    pub evicted_bytes: u64,
    pub stale_temp_files: u64,
    pub deferred: bool,
    pub failed: u64,
}

#[derive(Debug)]
struct CacheEntry {
    path: PathBuf,
    lock_key: String,
    size: u64,
    last_used: SystemTime,
}

pub fn spawn_maintenance(state: std::sync::Arc<AppState>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30 * 60));
        interval.tick().await;
        loop {
            interval.tick().await;
            match maintain(&state).await {
                Ok(summary) if summary.evicted_files > 0 || summary.stale_temp_files > 0 => {
                    state.events.emit(
                        "cache",
                        if summary.failed == 0 { "ok" } else { "warn" },
                        format!(
                            "缓存维护：占用 {} MiB，淘汰 {} 个文件 / {} MiB，临时文件 {}，失败 {}",
                            summary.bytes / 1024 / 1024,
                            summary.evicted_files,
                            summary.evicted_bytes / 1024 / 1024,
                            summary.stale_temp_files,
                            summary.failed
                        ),
                    );
                }
                Ok(_) => {}
                Err(error) => state
                    .events
                    .emit("cache", "err", format!("缓存维护失败：{error:#}")),
            }
        }
    });
}

pub async fn maintain(state: &AppState) -> anyhow::Result<CacheMaintenanceSummary> {
    let Ok(_maintenance) = state.cache_maintenance_lock.try_lock() else {
        return Ok(CacheMaintenanceSummary {
            deferred: true,
            ..CacheMaintenanceSummary::default()
        });
    };
    let cache_dir = state.config.subset_cache_dir();
    tokio::fs::create_dir_all(&cache_dir).await?;

    let mut summary = CacheMaintenanceSummary::default();
    let mut entries = Vec::new();
    let mut dir = tokio::fs::read_dir(&cache_dir)
        .await
        .with_context(|| format!("read cache directory {}", cache_dir.display()))?;
    while let Some(entry) = dir.next_entry().await? {
        let path = entry.path();
        let file_name = entry.file_name().to_string_lossy().to_string();
        let metadata = match entry.metadata().await {
            Ok(metadata) if metadata.is_file() => metadata,
            Ok(_) => continue,
            Err(error) => {
                summary.failed += 1;
                tracing::warn!(path = %path.display(), error = %error, "read cache metadata failed");
                continue;
            }
        };
        let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        if is_stale_temp_file(&file_name, modified) {
            match tokio::fs::remove_file(&path).await {
                Ok(()) => {
                    summary.stale_temp_files += 1;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    summary.failed += 1;
                    tracing::warn!(path = %path.display(), error = %error, "remove stale cache temp failed");
                }
            }
            continue;
        }
        let Some(lock_key) = cache_lock_key(&file_name) else {
            continue;
        };
        entries.push(CacheEntry {
            path,
            lock_key,
            size: metadata.len(),
            last_used: modified,
        });
    }

    let mut current_bytes: u64 = entries.iter().map(|entry| entry.size).sum();
    let max_bytes = state.config.subset_cache_max_bytes();
    if max_bytes > 0 && current_bytes > max_bytes {
        entries.sort_unstable_by_key(|entry| entry.last_used);
        let target_bytes = max_bytes.saturating_mul(CACHE_TARGET_PERCENT) / 100;
        for entry in &entries {
            if current_bytes <= target_bytes {
                break;
            }
            let lock = state.cache_lock(&entry.lock_key).await;
            let Ok(_guard) = lock.try_lock_owned() else {
                continue;
            };
            match tokio::fs::remove_file(&entry.path).await {
                Ok(()) => {
                    current_bytes = current_bytes.saturating_sub(entry.size);
                    summary.evicted_files += 1;
                    summary.evicted_bytes = summary.evicted_bytes.saturating_add(entry.size);
                    state.metrics.record_cache_eviction(entry.size);
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    current_bytes = current_bytes.saturating_sub(entry.size);
                }
                Err(error) => {
                    summary.failed += 1;
                    tracing::warn!(path = %entry.path.display(), error = %error, "evict cache file failed");
                }
            }
        }
    }

    summary.files = entries.len().saturating_sub(summary.evicted_files as usize) as u64;
    summary.bytes = current_bytes;
    state.metrics.set_cache_usage(summary.files, summary.bytes);
    Ok(summary)
}

pub fn temp_path(output_path: &Path) -> PathBuf {
    let parent = output_path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = output_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("font-cache.ttf");
    parent.join(format!(".{file_name}.{}.tmp", Uuid::new_v4()))
}

pub async fn file_is_ready(path: &Path) -> bool {
    tokio::fs::metadata(path)
        .await
        .map(|metadata| metadata.is_file() && metadata.len() > 0)
        .unwrap_or(false)
}

pub async fn remove_file_if_exists(path: &Path) -> anyhow::Result<()> {
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("remove cache file {}", path.display())),
    }
}

pub async fn publish(state: &AppState, temp_path: &Path, output_path: &Path) -> anyhow::Result<()> {
    let _maintenance = state.cache_maintenance_lock.lock().await;
    let existed = file_is_ready(output_path).await;
    match tokio::fs::rename(temp_path, output_path).await {
        Ok(()) => {}
        Err(error) => {
            if file_is_ready(output_path).await {
                remove_file_if_exists(temp_path).await?;
            } else {
                return Err(error).with_context(|| {
                    format!(
                        "publish cache file {} -> {}",
                        temp_path.display(),
                        output_path.display()
                    )
                });
            }
        }
    }
    if !existed {
        let size = tokio::fs::metadata(output_path).await?.len();
        state.metrics.record_cache_insert(size);
    }
    Ok(())
}

pub async fn read_and_touch(path: &Path) -> anyhow::Result<Vec<u8>> {
    let data = tokio::fs::read(path)
        .await
        .with_context(|| format!("read cache file {}", path.display()))?;
    touch_if_stale(path).await?;
    Ok(data)
}

async fn touch_if_stale(path: &Path) -> anyhow::Result<()> {
    let metadata = tokio::fs::metadata(path).await?;
    let needs_touch = metadata
        .modified()
        .ok()
        .and_then(|modified| SystemTime::now().duration_since(modified).ok())
        .is_none_or(|age| age >= CACHE_TOUCH_INTERVAL);
    if !needs_touch {
        return Ok(());
    }
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        set_file_mtime(&path, FileTime::now())
            .with_context(|| format!("touch cache file {}", path.display()))
    })
    .await
    .context("touch cache file task failed")?
}

fn cache_lock_key(file_name: &str) -> Option<String> {
    let (prefix, hash) = if let Some(hash) = file_name.strip_suffix(".draw.ttf") {
        ("draw", hash)
    } else if let Some(hash) = file_name.strip_suffix(".ttf") {
        ("font", hash)
    } else {
        return None;
    };
    if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    Some(format!("{prefix}:{hash}"))
}

fn is_stale_temp_file(file_name: &str, modified: SystemTime) -> bool {
    file_name.starts_with('.')
        && file_name.ends_with(".tmp")
        && SystemTime::now()
            .duration_since(modified)
            .is_ok_and(|age| age >= STALE_TEMP_AGE)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use tokio::sync::mpsc;

    use crate::config::{Config, ProcessingOptions};
    use crate::db::Db;
    use crate::events::EventBus;
    use crate::font_worker::FontWorkerPool;
    use crate::metrics::RuntimeMetrics;

    async fn test_state(data_dir: PathBuf, cache_max_mb: u64) -> Arc<AppState> {
        let worker_dir = tempfile::tempdir().unwrap();
        let worker_path = worker_dir.path().join("worker.py");
        std::fs::write(
            &worker_path,
            "import sys\nfor line in sys.stdin:\n print('{}', flush=True)\n",
        )
        .unwrap();
        let config = Arc::new(Config {
            listen_addr: "127.0.0.1:0".parse().unwrap(),
            font_dirs: Vec::new(),
            watch_dirs: Vec::new(),
            backup_dir: data_dir.join("backups"),
            data_dir,
            worker_script: worker_path,
            python_bin: if cfg!(windows) {
                "python".to_string()
            } else {
                "python3".to_string()
            },
            admin_password_hash: None,
            admin_password_plain: Some("test".to_string()),
            allow_no_auth: false,
            secure_cookies: false,
            max_concurrent_jobs: 1,
            max_font_workers: 1,
            max_index_concurrency: 1,
            max_scan_concurrency: 1,
            max_conversion_memory_mb: 64,
            subset_cache_max_mb: cache_max_mb,
            font_worker_timeout: Duration::from_secs(5),
            job_queue_size: 16,
            scan_interval: Duration::ZERO,
            backup_retention_days: 0,
            options: ProcessingOptions::default(),
        });
        tokio::fs::create_dir_all(config.subset_cache_dir())
            .await
            .unwrap();
        let db = Db::connect(&config).await.unwrap();
        let metrics = Arc::new(RuntimeMetrics::new());
        let workers = FontWorkerPool::start(&config, metrics.clone())
            .await
            .unwrap();
        let (job_tx, _job_rx) = mpsc::channel(16);
        Arc::new(AppState::new(
            config,
            db,
            EventBus::new(),
            workers,
            metrics,
            job_tx,
        ))
    }

    #[test]
    fn cache_keys_only_accept_managed_hash_files() {
        let hash = "a".repeat(64);
        assert_eq!(
            cache_lock_key(&format!("{hash}.ttf")),
            Some(format!("font:{hash}"))
        );
        assert_eq!(
            cache_lock_key(&format!("{hash}.draw.ttf")),
            Some(format!("draw:{hash}"))
        );
        assert!(cache_lock_key("notes.ttf").is_none());
        assert!(cache_lock_key("../escape.ttf").is_none());
    }

    #[tokio::test]
    async fn maintenance_evicts_oldest_managed_files_to_target() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path().to_path_buf(), 1).await;
        let cache_dir = state.config.subset_cache_dir();
        let old = cache_dir.join(format!("{}.ttf", "a".repeat(64)));
        let middle = cache_dir.join(format!("{}.ttf", "b".repeat(64)));
        let newest = cache_dir.join(format!("{}.draw.ttf", "c".repeat(64)));
        for path in [&old, &middle, &newest] {
            tokio::fs::write(path, vec![0u8; 400 * 1024]).await.unwrap();
        }
        set_file_mtime(&old, FileTime::from_unix_time(1, 0)).unwrap();
        set_file_mtime(&middle, FileTime::from_unix_time(2, 0)).unwrap();
        set_file_mtime(&newest, FileTime::from_unix_time(3, 0)).unwrap();

        let summary = maintain(&state).await.unwrap();
        assert_eq!(summary.evicted_files, 1);
        assert!(!old.exists());
        assert!(middle.exists());
        assert!(newest.exists());
        assert!(summary.bytes <= 1024 * 1024 * 90 / 100);
    }
}
