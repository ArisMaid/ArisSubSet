mod api;
mod ass;
mod auth;
mod config;
mod db;
mod events;
mod font_inspect;
mod font_worker;
mod indexer;
mod models;
mod processor;
mod scanner;
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
    let workers = FontWorkerPool::start(&config).await?;
    let (job_tx, job_rx) = mpsc::channel(config.job_queue_size);

    let state = Arc::new(AppState::new(config.clone(), db, events, workers, job_tx));
    state.load_runtime_settings().await?;

    processor::spawn_job_loop(job_rx, state.clone());
    indexer::spawn_initial_index(state.clone());
    scanner::spawn_scheduler(state.clone());

    let app = api::router(state.clone());
    let listener = TcpListener::bind(&config.listen_addr)
        .await
        .with_context(|| format!("failed to bind {}", config.listen_addr))?;
    tracing::info!("listening on http://{}", config.listen_addr);
    axum::serve(listener, app).await?;
    Ok(())
}
