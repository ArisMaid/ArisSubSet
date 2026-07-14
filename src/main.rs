mod api;
mod ass;
mod auth;
mod backup;
mod cache;
mod config;
mod db;
mod events;
mod failure_log;
mod font_inspect;
mod font_worker;
mod fs_walk;
mod indexer;
mod metrics;
mod models;
mod processor;
mod scanner;
mod sqlx;
mod state;

use std::sync::Arc;

use anyhow::Context;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::config::Config;
use crate::db::Db;
use crate::events::EventBus;
use crate::font_worker::FontWorkerPool;
use crate::metrics::RuntimeMetrics;
use crate::state::AppState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ass_subset_service=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let config = Arc::new(Config::from_env()?);
    tokio::fs::create_dir_all(&config.data_dir).await?;
    tokio::fs::create_dir_all(&config.backup_dir).await?;
    tokio::fs::create_dir_all(config.subset_cache_dir()).await?;

    let db = Db::connect(&config).await?;
    let events = EventBus::new();
    let metrics = Arc::new(RuntimeMetrics::new());
    let workers = FontWorkerPool::start(&config, metrics.clone()).await?;
    let (job_tx, job_rx) = mpsc::channel(config.job_queue_size);

    let state = Arc::new(AppState::new(
        config.clone(),
        db,
        events,
        workers,
        metrics,
        job_tx,
    ));
    state.load_runtime_settings().await?;

    match cache::maintain(&state).await {
        Ok(summary) => tracing::info!(
            files = summary.files,
            bytes = summary.bytes,
            evicted = summary.evicted_files,
            "subset cache ready"
        ),
        Err(error) => tracing::warn!(error = %error, "initial cache maintenance failed"),
    }

    processor::spawn_controlled_job_loop(job_rx, state.clone());
    processor::recover_incomplete_jobs(state.clone()).await?;
    indexer::spawn_initial_index(state.clone());
    scanner::spawn_scheduler(state.clone());
    spawn_session_cleanup(state.clone());
    spawn_backup_cleanup(state.clone());
    cache::spawn_maintenance(state.clone());

    let app = api::router(state.clone());
    let listener = TcpListener::bind(&config.listen_addr)
        .await
        .with_context(|| format!("failed to bind {}", config.listen_addr))?;
    tracing::info!("listening on http://{}", config.listen_addr);
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;
    Ok(())
}

fn spawn_session_cleanup(state: Arc<AppState>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60 * 60));
        interval.tick().await;
        loop {
            interval.tick().await;
            auth::cleanup_sessions(&state).await;
        }
    });
}

fn spawn_backup_cleanup(state: Arc<AppState>) {
    if state.config.backup_retention_days == 0 {
        return;
    }
    tokio::spawn(async move {
        loop {
            match backup::prune_expired(&state).await {
                Ok(summary) if summary.expired > 0 => state.events.emit(
                    "backup",
                    if summary.failed == 0 { "ok" } else { "warn" },
                    format!(
                        "备份保留清理：删除 {}，移除缺失记录 {}，失败 {}",
                        summary.removed_files, summary.missing_files, summary.failed
                    ),
                ),
                Ok(_) => {}
                Err(error) => {
                    state
                        .events
                        .emit("backup", "err", format!("备份保留清理失败：{error:#}"))
                }
            }
            tokio::time::sleep(std::time::Duration::from_secs(24 * 60 * 60)).await;
        }
    });
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let ctrl_c = tokio::signal::ctrl_c();
    if let Ok(mut terminate) = signal(SignalKind::terminate()) {
        tokio::select! {
            _ = ctrl_c => {}
            _ = terminate.recv() => {}
        }
    } else {
        let _ = ctrl_c.await;
    }
    tracing::info!("shutdown signal received");
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("shutdown signal received");
}
