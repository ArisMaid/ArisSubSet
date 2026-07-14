use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, UNIX_EPOCH};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use walkdir::WalkDir;

const WALK_EVENT_BUFFER: usize = 256;

#[derive(Clone, Debug)]
pub struct WalkControl {
    paused: Arc<AtomicBool>,
    cancelled: Arc<AtomicBool>,
}

#[derive(Clone, Copy, Debug)]
pub struct WalkOptions {
    pub follow_links: bool,
    pub extensions: &'static [&'static str],
    pub ignored_directories: &'static [&'static str],
}

#[derive(Debug)]
pub struct DiscoveredFile {
    pub path: PathBuf,
    pub size: i64,
    pub mtime: i64,
}

#[derive(Debug)]
pub enum WalkEvent {
    File(DiscoveredFile),
    Error { path: PathBuf, message: String },
}

#[derive(Debug, Default)]
pub struct WalkResult {
    pub complete: bool,
    pub cancelled: bool,
}

impl Default for WalkControl {
    fn default() -> Self {
        Self::new()
    }
}

impl WalkControl {
    pub fn new() -> Self {
        Self {
            paused: Arc::new(AtomicBool::new(false)),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn set_paused(&self, paused: bool) {
        self.paused.store(paused, Ordering::Release);
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        self.paused.store(false, Ordering::Release);
    }

    pub fn clear_cancel(&self) {
        self.cancelled.store(false, Ordering::Release);
    }

    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::Acquire)
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub async fn wait_async(&self) -> bool {
        loop {
            if self.is_cancelled() {
                return false;
            }
            if !self.is_paused() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    fn wait_blocking(&self) -> bool {
        loop {
            if self.is_cancelled() {
                return false;
            }
            if !self.is_paused() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

pub fn spawn_file_walk(
    root: PathBuf,
    options: WalkOptions,
    control: WalkControl,
) -> (mpsc::Receiver<WalkEvent>, JoinHandle<WalkResult>) {
    let (tx, rx) = mpsc::channel(WALK_EVENT_BUFFER);
    let handle = tokio::task::spawn_blocking(move || walk_files(root, options, control, tx));
    (rx, handle)
}

fn walk_files(
    root: PathBuf,
    options: WalkOptions,
    control: WalkControl,
    tx: mpsc::Sender<WalkEvent>,
) -> WalkResult {
    match std::fs::metadata(&root) {
        Ok(metadata) if metadata.is_dir() => {}
        Ok(_) => {
            let _ = tx.blocking_send(WalkEvent::Error {
                path: root,
                message: "configured root is not a directory".to_string(),
            });
            return WalkResult::default();
        }
        Err(error) => {
            let _ = tx.blocking_send(WalkEvent::Error {
                path: root,
                message: error.to_string(),
            });
            return WalkResult::default();
        }
    }

    let mut result = WalkResult {
        complete: true,
        ..WalkResult::default()
    };
    let walker = WalkDir::new(&root)
        .follow_links(options.follow_links)
        .into_iter()
        .filter_entry(|entry| should_visit(entry, options.ignored_directories));

    for entry in walker {
        if !control.wait_blocking() {
            result.complete = false;
            result.cancelled = true;
            break;
        }
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                result.complete = false;
                let path = error.path().unwrap_or(&root).to_path_buf();
                if tx
                    .blocking_send(WalkEvent::Error {
                        path,
                        message: error.to_string(),
                    })
                    .is_err()
                {
                    break;
                }
                continue;
            }
        };
        if !entry.file_type().is_file() || !has_extension(entry.path(), options.extensions) {
            continue;
        }
        let metadata = match entry.metadata() {
            Ok(metadata) => metadata,
            Err(error) => {
                result.complete = false;
                if tx
                    .blocking_send(WalkEvent::Error {
                        path: entry.path().to_path_buf(),
                        message: error.to_string(),
                    })
                    .is_err()
                {
                    break;
                }
                continue;
            }
        };
        let mtime = metadata
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_secs() as i64)
            .unwrap_or(0);
        if tx
            .blocking_send(WalkEvent::File(DiscoveredFile {
                path: entry.path().to_path_buf(),
                size: metadata.len() as i64,
                mtime,
            }))
            .is_err()
        {
            result.complete = false;
            break;
        }
    }
    result
}

fn should_visit(entry: &walkdir::DirEntry, ignored_directories: &[&str]) -> bool {
    if entry.depth() == 0 || !entry.file_type().is_dir() {
        return true;
    }
    let name = entry.file_name().to_string_lossy();
    !ignored_directories
        .iter()
        .any(|ignored| name.eq_ignore_ascii_case(ignored))
}

fn has_extension(path: &Path, extensions: &[&str]) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            extensions
                .iter()
                .any(|expected| extension.eq_ignore_ascii_case(expected))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    const OPTIONS: WalkOptions = WalkOptions {
        follow_links: false,
        extensions: &["ass", "ssa"],
        ignored_directories: &["#recycle"],
    };

    #[tokio::test]
    async fn walk_runs_off_runtime_and_filters_extensions_and_directories() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("one.ass"), b"ok").unwrap();
        std::fs::write(dir.path().join("two.txt"), b"skip").unwrap();
        std::fs::create_dir(dir.path().join("#recycle")).unwrap();
        std::fs::write(dir.path().join("#recycle").join("old.ass"), b"skip").unwrap();

        let (mut rx, handle) =
            spawn_file_walk(dir.path().to_path_buf(), OPTIONS, WalkControl::new());
        let mut files = Vec::new();
        while let Some(event) = rx.recv().await {
            if let WalkEvent::File(file) = event {
                files.push(file.path);
            }
        }
        let result = handle.await.unwrap();
        assert!(result.complete);
        assert_eq!(files, vec![dir.path().join("one.ass")]);
    }

    #[tokio::test]
    async fn cancelled_walk_reports_cancellation() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("one.ass"), b"ok").unwrap();
        let control = WalkControl::new();
        control.cancel();
        let (mut rx, handle) = spawn_file_walk(dir.path().to_path_buf(), OPTIONS, control);
        while rx.recv().await.is_some() {}
        let result = handle.await.unwrap();
        assert!(result.cancelled);
        assert!(!result.complete);
    }
}
