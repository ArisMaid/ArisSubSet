use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const LATENCY_SAMPLE_LIMIT: usize = 1024;

#[derive(Debug)]
pub struct RuntimeMetrics {
    started_at: Instant,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_evictions: AtomicU64,
    cache_evicted_bytes: AtomicU64,
    cache_files: AtomicU64,
    cache_bytes: AtomicU64,
    worker_requests: AtomicU64,
    worker_restarts: AtomicU64,
    conversions_started: AtomicU64,
    conversions_succeeded: AtomicU64,
    conversions_failed: AtomicU64,
    queue_latency: LatencySamples,
    conversion_duration: LatencySamples,
}

#[derive(Debug, Default)]
struct LatencySamples {
    values: Mutex<VecDeque<u64>>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct RuntimeMetricsSnapshot {
    pub uptime_seconds: u64,
    pub cache: CacheMetricsSnapshot,
    pub queue: LatencySnapshot,
    pub conversions: ConversionMetricsSnapshot,
    pub workers: WorkerMetricsSnapshot,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CacheMetricsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub hit_rate_percent: f64,
    pub files: u64,
    pub bytes: u64,
    pub max_bytes: u64,
    pub evictions: u64,
    pub evicted_bytes: u64,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct LatencySnapshot {
    pub samples: u64,
    pub average_ms: u64,
    pub p50_ms: u64,
    pub p95_ms: u64,
    pub max_ms: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ConversionMetricsSnapshot {
    pub started: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub duration: LatencySnapshot,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct WorkerMetricsSnapshot {
    pub requests: u64,
    pub restarts: u64,
}

impl Default for RuntimeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeMetrics {
    pub fn new() -> Self {
        Self {
            started_at: Instant::now(),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            cache_evictions: AtomicU64::new(0),
            cache_evicted_bytes: AtomicU64::new(0),
            cache_files: AtomicU64::new(0),
            cache_bytes: AtomicU64::new(0),
            worker_requests: AtomicU64::new(0),
            worker_restarts: AtomicU64::new(0),
            conversions_started: AtomicU64::new(0),
            conversions_succeeded: AtomicU64::new(0),
            conversions_failed: AtomicU64::new(0),
            queue_latency: LatencySamples::default(),
            conversion_duration: LatencySamples::default(),
        }
    }

    pub fn record_cache_lookup(&self, hit: bool) {
        if hit {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.cache_misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn set_cache_usage(&self, files: u64, bytes: u64) {
        self.cache_files.store(files, Ordering::Relaxed);
        self.cache_bytes.store(bytes, Ordering::Relaxed);
    }

    pub fn record_cache_insert(&self, bytes: u64) {
        self.cache_files.fetch_add(1, Ordering::Relaxed);
        self.cache_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_cache_eviction(&self, bytes: u64) {
        self.cache_evictions.fetch_add(1, Ordering::Relaxed);
        self.cache_evicted_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_worker_request(&self) {
        self.worker_requests.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_worker_restart(&self) {
        self.worker_restarts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_conversion_started(&self, queue_latency: Duration) {
        self.conversions_started.fetch_add(1, Ordering::Relaxed);
        self.queue_latency.record(queue_latency);
    }

    pub fn record_conversion_finished(&self, duration: Duration, succeeded: bool) {
        if succeeded {
            self.conversions_succeeded.fetch_add(1, Ordering::Relaxed);
        } else {
            self.conversions_failed.fetch_add(1, Ordering::Relaxed);
        }
        self.conversion_duration.record(duration);
    }

    pub fn snapshot(&self, cache_max_bytes: u64) -> RuntimeMetricsSnapshot {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let lookups = hits.saturating_add(misses);
        RuntimeMetricsSnapshot {
            uptime_seconds: self.started_at.elapsed().as_secs(),
            cache: CacheMetricsSnapshot {
                hits,
                misses,
                hit_rate_percent: if lookups == 0 {
                    0.0
                } else {
                    hits as f64 * 100.0 / lookups as f64
                },
                files: self.cache_files.load(Ordering::Relaxed),
                bytes: self.cache_bytes.load(Ordering::Relaxed),
                max_bytes: cache_max_bytes,
                evictions: self.cache_evictions.load(Ordering::Relaxed),
                evicted_bytes: self.cache_evicted_bytes.load(Ordering::Relaxed),
            },
            queue: self.queue_latency.snapshot(),
            conversions: ConversionMetricsSnapshot {
                started: self.conversions_started.load(Ordering::Relaxed),
                succeeded: self.conversions_succeeded.load(Ordering::Relaxed),
                failed: self.conversions_failed.load(Ordering::Relaxed),
                duration: self.conversion_duration.snapshot(),
            },
            workers: WorkerMetricsSnapshot {
                requests: self.worker_requests.load(Ordering::Relaxed),
                restarts: self.worker_restarts.load(Ordering::Relaxed),
            },
        }
    }
}

impl LatencySamples {
    fn record(&self, duration: Duration) {
        let millis = duration.as_millis().min(u64::MAX as u128) as u64;
        let mut values = self
            .values
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if values.len() == LATENCY_SAMPLE_LIMIT {
            values.pop_front();
        }
        values.push_back(millis);
    }

    fn snapshot(&self) -> LatencySnapshot {
        let values = self
            .values
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if values.is_empty() {
            return LatencySnapshot::default();
        }
        let mut sorted: Vec<u64> = values.iter().copied().collect();
        sorted.sort_unstable();
        let sum: u128 = sorted.iter().map(|value| *value as u128).sum();
        LatencySnapshot {
            samples: sorted.len() as u64,
            average_ms: (sum / sorted.len() as u128).min(u64::MAX as u128) as u64,
            p50_ms: percentile(&sorted, 50),
            p95_ms: percentile(&sorted, 95),
            max_ms: *sorted.last().unwrap_or(&0),
        }
    }
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let index = sorted
        .len()
        .saturating_mul(percentile)
        .div_ceil(100)
        .saturating_sub(1)
        .min(sorted.len().saturating_sub(1));
    sorted.get(index).copied().unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_snapshot_reports_percentiles_from_recent_samples() {
        let samples = LatencySamples::default();
        for millis in 1..=100 {
            samples.record(Duration::from_millis(millis));
        }
        let snapshot = samples.snapshot();
        assert_eq!(snapshot.samples, 100);
        assert_eq!(snapshot.average_ms, 50);
        assert_eq!(snapshot.p50_ms, 50);
        assert_eq!(snapshot.p95_ms, 95);
        assert_eq!(snapshot.max_ms, 100);
    }

    #[test]
    fn cache_hit_rate_uses_all_runtime_lookups() {
        let metrics = RuntimeMetrics::new();
        metrics.record_cache_lookup(true);
        metrics.record_cache_lookup(true);
        metrics.record_cache_lookup(false);
        let snapshot = metrics.snapshot(1024);
        assert!((snapshot.cache.hit_rate_percent - 66.666).abs() < 0.01);
    }
}
