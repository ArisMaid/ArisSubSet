use std::env;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, bail};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ProcessingOptions {
    pub embed_external_fonts: bool,
    pub embed_system_fonts: bool,
    pub include_ascii: bool,
    pub multi_weight: bool,
    pub randomize_font_names: bool,
    pub draw_subset: bool,
    pub full_font_embed: bool,
    pub fallback_full_font_embed: bool,
    pub variable_fonts: bool,
}

impl Default for ProcessingOptions {
    fn default() -> Self {
        Self {
            embed_external_fonts: true,
            embed_system_fonts: false,
            include_ascii: true,
            multi_weight: true,
            randomize_font_names: true,
            draw_subset: true,
            full_font_embed: false,
            fallback_full_font_embed: true,
            variable_fonts: false,
        }
    }
}

impl ProcessingOptions {
    pub fn config_hash(&self) -> String {
        let json = serde_json::to_vec(self).expect("processing options serialize");
        let mut h = Sha256::new();
        h.update(json);
        hex::encode(h.finalize())
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub listen_addr: SocketAddr,
    pub font_dirs: Vec<PathBuf>,
    pub watch_dirs: Vec<PathBuf>,
    pub backup_dir: PathBuf,
    pub data_dir: PathBuf,
    pub worker_script: PathBuf,
    pub python_bin: String,
    pub admin_password_hash: Option<String>,
    pub admin_password_plain: Option<String>,
    pub allow_no_auth: bool,
    pub secure_cookies: bool,
    pub max_concurrent_jobs: usize,
    pub max_font_workers: usize,
    pub max_index_concurrency: usize,
    pub max_scan_concurrency: usize,
    pub max_conversion_memory_mb: usize,
    pub subset_cache_max_mb: u64,
    pub font_worker_timeout: Duration,
    pub job_queue_size: usize,
    pub scan_interval: Duration,
    pub backup_retention_days: u64,
    pub options: ProcessingOptions,
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let listen_addr = env::var("LISTEN_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:8080".to_string())
            .parse()
            .context("LISTEN_ADDR must be a socket address")?;

        let data_dir = env_path("DATA_DIR", "/data");
        let backup_dir = env_path("BACKUP_DIR", "/backups");
        let font_dirs = env_paths("FONT_DIRS", "/fonts");
        let watch_dirs = env_paths("WATCH_DIRS", "/watch");
        let worker_script = env_path("FONT_WORKER_PATH", "workers/font_worker.py");
        let python_bin = env::var("PYTHON_BIN").unwrap_or_else(|_| "python3".to_string());

        let admin_password_hash = env::var("ADMIN_PASSWORD_HASH").ok();
        let admin_password_plain = env::var("ADMIN_PASSWORD").ok();
        let allow_no_auth = env_bool("ASS_SUBSET_ALLOW_NO_AUTH", false);
        let secure_cookies = env_bool("SECURE_COOKIES", false);
        if !allow_no_auth && admin_password_hash.is_none() && admin_password_plain.is_none() {
            bail!(
                "set ADMIN_PASSWORD_HASH to an Argon2id PHC string (legacy sha256:<hex> is supported) or set ADMIN_PASSWORD; use ASS_SUBSET_ALLOW_NO_AUTH=1 only for trusted local development"
            );
        }

        let max_concurrent_jobs = env_usize("MAX_CONCURRENT_JOBS", 2).max(1);
        let max_font_workers = env_usize("MAX_FONT_WORKERS", 2).max(1);
        let max_index_concurrency =
            env_usize("MAX_INDEX_CONCURRENCY", (max_font_workers * 8).clamp(8, 64)).max(1);
        let max_scan_concurrency = env_usize("MAX_SCAN_CONCURRENCY", 4).clamp(1, 32);
        let max_conversion_memory_mb =
            env_usize("MAX_CONVERSION_MEMORY_MB", 512).clamp(64, 32 * 1024);
        let subset_cache_max_mb = env_u64("SUBSET_CACHE_MAX_MB", 2048).min(1024 * 1024);
        let font_worker_timeout =
            Duration::from_secs(env_u64("FONT_WORKER_TIMEOUT_SECONDS", 300).clamp(10, 3600));
        let job_queue_size = env_usize("JOB_QUEUE_SIZE", 1024).max(16);
        let backup_retention_days = env_u64("BACKUP_RETENTION_DAYS", 0).min(365_000);
        let scan_interval = parse_scan_interval();

        let mut options = ProcessingOptions::default();
        options.embed_external_fonts =
            env_bool("EMBED_EXTERNAL_FONTS", options.embed_external_fonts);
        options.embed_system_fonts = env_bool("EMBED_SYSTEM_FONTS", options.embed_system_fonts);
        options.include_ascii = env_bool("INCLUDE_ASCII", options.include_ascii);
        options.multi_weight = env_bool("MULTI_WEIGHT", options.multi_weight);
        options.randomize_font_names =
            env_bool("RANDOMIZE_FONT_NAMES", options.randomize_font_names);
        options.draw_subset = env_bool("DRAW_SUBSET", options.draw_subset);
        options.full_font_embed = env_bool("FULL_FONT_EMBED", options.full_font_embed);
        options.fallback_full_font_embed =
            env_bool("FALLBACK_FULL_FONT_EMBED", options.fallback_full_font_embed);
        options.variable_fonts = env_bool("VARIABLE_FONTS", options.variable_fonts);

        Ok(Self {
            listen_addr,
            font_dirs,
            watch_dirs,
            backup_dir,
            data_dir,
            worker_script,
            python_bin,
            admin_password_hash,
            admin_password_plain,
            allow_no_auth,
            secure_cookies,
            max_concurrent_jobs,
            max_font_workers,
            max_index_concurrency,
            max_scan_concurrency,
            max_conversion_memory_mb,
            subset_cache_max_mb,
            font_worker_timeout,
            job_queue_size,
            scan_interval,
            backup_retention_days,
            options,
        })
    }

    pub fn database_path(&self) -> PathBuf {
        self.data_dir.join("ass-subset-service.sqlite3")
    }

    pub fn subset_cache_dir(&self) -> PathBuf {
        self.data_dir.join("cache").join("subsets")
    }

    pub fn subset_cache_max_bytes(&self) -> u64 {
        self.subset_cache_max_mb.saturating_mul(1024 * 1024)
    }
}

fn env_path(key: &str, default: &str) -> PathBuf {
    env::var(key)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(default))
}

fn env_paths(key: &str, default: &str) -> Vec<PathBuf> {
    let raw = env::var(key).unwrap_or_else(|_| default.to_string());
    let mut paths = Vec::new();
    for value in raw
        .split([',', ';'])
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let path = PathBuf::from(value);
        if !paths.contains(&path) {
            paths.push(path);
        }
    }
    paths
}

fn env_bool(key: &str, default: bool) -> bool {
    match env::var(key).ok().as_deref().map(str::to_ascii_lowercase) {
        Some(v) if matches!(v.as_str(), "1" | "true" | "yes" | "on") => true,
        Some(v) if matches!(v.as_str(), "0" | "false" | "no" | "off") => false,
        _ => default,
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn parse_scan_interval() -> Duration {
    if let Ok(raw) = env::var("SCAN_CRON") {
        if raw.eq_ignore_ascii_case("disabled") || raw == "0" {
            return Duration::from_secs(0);
        }
        if let Some(rest) = raw.strip_prefix("@every ")
            && let Some(d) = parse_duration(rest.trim())
        {
            return d;
        }
    }
    Duration::from_secs(env_u64("SCAN_INTERVAL_SECONDS", 3600))
}

fn parse_duration(s: &str) -> Option<Duration> {
    let (num, unit) = s.split_at(s.len().saturating_sub(1));
    let n: u64 = num.parse().ok()?;
    match unit {
        "s" | "S" => Some(Duration::from_secs(n)),
        "m" | "M" => Some(Duration::from_secs(n * 60)),
        "h" | "H" => Some(Duration::from_secs(n * 3600)),
        _ => s.parse::<u64>().ok().map(Duration::from_secs),
    }
}

pub fn root_label(path: &Path, index: usize) -> String {
    path.file_name()
        .and_then(|s| s.to_str())
        .filter(|s| !s.trim().is_empty())
        .map(sanitize_path_segment)
        .unwrap_or_else(|| format!("watch{index}"))
}

pub fn sanitize_path_segment(s: &str) -> String {
    let out: String = s
        .chars()
        .map(|c| {
            if c.is_control() || matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|') {
                '_'
            } else {
                c
            }
        })
        .collect();
    let trimmed = out.trim_matches([' ', '.']);
    if trimmed.is_empty() {
        "_".to_string()
    } else {
        trimmed.to_string()
    }
}
