use std::convert::Infallible;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use async_stream::stream;
use axum::extract::{Multipart, Path, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use sqlx::Row;
use tower_http::services::ServeDir;
use tower_http::trace::TraceLayer;

use crate::ass::{decode_subtitle, is_system_font, parse_embedded_fonts, parse_subtitle};
use crate::auth;
use crate::config::sanitize_path_segment;
use crate::indexer;
use crate::models::JobMode;
use crate::processor;
use crate::scanner;
use crate::state::AppState;

pub fn router(state: Arc<AppState>) -> Router {
    let web_dir = std::env::var("WEB_DIR").unwrap_or_else(|_| {
        if FsPath::new("web/dist/index.html").exists() {
            "web/dist".to_string()
        } else {
            "web".to_string()
        }
    });

    Router::new()
        .route("/api/auth/login", post(login))
        .route("/api/status", get(status))
        .route("/api/index/rebuild", post(rebuild_index))
        .route("/api/scan", post(scan))
        .route("/api/scan/pause", post(pause_scan))
        .route("/api/scan/resume", post(resume_scan))
        .route("/api/scan/cancel", post(cancel_scan))
        .route("/api/watch-dirs", post(add_watch_dir))
        .route("/api/watch-dirs/remove", post(remove_watch_dir))
        .route("/api/options", post(set_option))
        .route("/api/conversion/pause", post(pause_conversion))
        .route("/api/conversion/resume", post(resume_conversion))
        .route("/api/conversion/cancel", post(cancel_conversion))
        .route(
            "/api/conversion/parallelism",
            post(set_conversion_parallelism),
        )
        .route("/api/schedule", post(set_schedule))
        .route("/api/upload", post(upload_subtitle))
        .route("/api/files", get(files))
        .route("/api/files/{id}/download", get(download_file))
        .route("/api/jobs", get(jobs))
        .route("/api/jobs/{id}/retry", post(retry_job))
        .route("/api/files/{id}/process", post(process_file))
        .route("/api/files/{id}/strip-embedded", post(strip_embedded_file))
        .route("/api/backups", get(backups))
        .route("/api/backups/{id}/restore", post(restore_backup))
        .route("/api/events", get(events))
        .fallback_service(ServeDir::new(web_dir).append_index_html_on_directories(true))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

#[derive(Debug, Deserialize)]
struct LoginRequest {
    password: String,
}

#[derive(Debug, Serialize)]
struct LoginResponse {
    ok: bool,
    csrf: String,
}

async fn login(State(state): State<Arc<AppState>>, Json(req): Json<LoginRequest>) -> Response {
    auth::cleanup_sessions(&state).await;
    if !auth::verify_password(&state, &req.password) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error":"invalid password"})),
        )
            .into_response();
    }
    let info = auth::create_session(&state).await;
    let mut resp = Json(LoginResponse {
        ok: true,
        csrf: info.csrf,
    })
    .into_response();
    resp.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&auth::session_cookie(&info.token)).expect("valid cookie"),
    );
    resp
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    fonts: serde_json::Value,
    subtitles: serde_json::Value,
    jobs: serde_json::Value,
    backups: i64,
    config: serde_json::Value,
    capabilities: serde_json::Value,
}

async fn status(State(state): State<Arc<AppState>>) -> Result<Json<StatusResponse>, StatusCode> {
    let font_files: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM font_files WHERE status='ok'")
        .fetch_one(&state.db.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let font_faces: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM font_faces")
        .fetch_one(&state.db.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let font_errors: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM font_files WHERE status='error'")
            .fetch_one(&state.db.pool)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let subtitle_files: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM subtitle_files")
        .fetch_one(&state.db.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let backups: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM backups")
        .fetch_one(&state.db.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let job_rows = sqlx::query("SELECT status, COUNT(*) AS n FROM jobs GROUP BY status")
        .fetch_all(&state.db.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut job_counts = serde_json::Map::new();
    for row in job_rows {
        let status: String = row.get("status");
        let n: i64 = row.get("n");
        job_counts.insert(status, serde_json::json!(n));
    }
    let watch_entries = scanner::watch_dir_entries(&state)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let watch_dirs: Vec<String> = watch_entries
        .iter()
        .map(|entry| entry.path.to_string_lossy().to_string())
        .collect();
    let watch_dir_items: Vec<_> = watch_entries
        .iter()
        .map(|entry| {
            serde_json::json!({
                "path": entry.path.to_string_lossy(),
                "removable": entry.removable,
            })
        })
        .collect();
    let options = state.processing_options().await;
    let scan_interval = state.scan_interval().await;
    let controls = state.controls().await;

    Ok(Json(StatusResponse {
        fonts: serde_json::json!({
            "files": font_files,
            "faces": font_faces,
            "errors": font_errors,
        }),
        subtitles: serde_json::json!({
            "files": subtitle_files,
        }),
        jobs: serde_json::Value::Object(job_counts),
        backups,
        config: serde_json::json!({
            "auth_required": !state.config.allow_no_auth,
            "font_dirs": state.config.font_dirs,
            "watch_dirs": watch_dirs,
            "watch_dir_items": watch_dir_items,
            "backup_dir": state.config.backup_dir,
            "data_dir": state.config.data_dir,
            "scan_interval_seconds": scan_interval.as_secs(),
            "backup_retention_days": state.config.backup_retention_days,
            "max_concurrent_jobs": state.config.max_concurrent_jobs,
            "max_index_concurrency": state.config.max_index_concurrency,
            "controls": controls,
            "options": options,
        }),
        capabilities: serde_json::json!({
            "font_subset_map": true,
            "draw_table_v27": true,
            "strip_embedded": true,
            "safe_strip_keeps_unrestorable_fonts": true,
        }),
    }))
}

async fn rebuild_index(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    auth::require_auth(&state, &headers, true).await?;
    let st = state.clone();
    tokio::spawn(async move {
        st.events.emit("index", "info", "开始重建字体索引");
        match indexer::rebuild_index(st.clone()).await {
            Ok(summary) => st.events.emit(
                "index",
                "ok",
                format!(
                    "索引完成：扫描 {}，更新 {}，跳过 {}，失败 {}，耗时 {}ms",
                    summary.scanned,
                    summary.indexed,
                    summary.skipped,
                    summary.failed,
                    summary.walk_ms + summary.inspect_ms + summary.write_ms
                ),
            ),
            Err(err) => st.events.emit("index", "err", format!("索引失败：{err:#}")),
        }
    });
    Ok(Json(serde_json::json!({"ok": true})))
}

async fn scan(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    auth::require_auth(&state, &headers, true).await?;
    let st = state.clone();
    tokio::spawn(async move {
        st.events.emit("scan", "info", "开始扫描监听目录");
        match scanner::scan_now(st.clone()).await {
            Ok(summary) => st.events.emit(
                "scan",
                "ok",
                format!(
                    "扫描完成：发现 {}，入队 {}，跳过 {}，失败 {}",
                    summary.seen, summary.queued, summary.skipped, summary.failed
                ),
            ),
            Err(err) => st.events.emit("scan", "err", format!("扫描失败：{err:#}")),
        }
    });
    Ok(Json(serde_json::json!({"ok": true})))
}

async fn pause_scan(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    auth::require_auth(&state, &headers, true).await?;
    state.set_scan_paused(true).await;
    state.events.emit("scan", "info", "scan paused");
    Ok(Json(serde_json::json!({"ok": true})))
}

async fn resume_scan(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    auth::require_auth(&state, &headers, true).await?;
    state.set_scan_paused(false).await;
    state.events.emit("scan", "info", "scan resumed");
    Ok(Json(serde_json::json!({"ok": true})))
}

async fn cancel_scan(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    auth::require_auth(&state, &headers, true).await?;
    state.request_scan_cancel().await;
    state.events.emit("scan", "warn", "scan cancel requested");
    Ok(Json(serde_json::json!({"ok": true})))
}

#[derive(Debug, Deserialize)]
struct WatchDirRequest {
    path: String,
}

async fn add_watch_dir(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<WatchDirRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    auth::require_auth(&state, &headers, true).await?;
    let path = PathBuf::from(req.path.trim());
    if path.as_os_str().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    scanner::add_watch_dir(&state, &path)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    state
        .events
        .emit("config", "ok", format!("已添加监听目录 {}", path.display()));
    Ok(Json(serde_json::json!({"ok": true})))
}

async fn remove_watch_dir(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<WatchDirRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    auth::require_auth(&state, &headers, true).await?;
    let path = PathBuf::from(req.path.trim());
    if path.as_os_str().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let removed = scanner::remove_watch_dir(&state, &path)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if !removed {
        return Err(StatusCode::BAD_REQUEST);
    }
    state
        .events
        .emit("config", "ok", format!("已移除监听目录 {}", path.display()));
    Ok(Json(serde_json::json!({"ok": true})))
}

#[derive(Debug, Deserialize)]
struct OptionRequest {
    key: String,
    value: bool,
}

async fn set_option(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<OptionRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    auth::require_auth(&state, &headers, true).await?;
    let options = state
        .set_processing_option(&req.key, req.value)
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    state.events.emit(
        "config",
        "ok",
        format!(
            "已{} {}",
            if req.value { "启用" } else { "关闭" },
            option_label(&req.key)
        ),
    );
    Ok(Json(serde_json::json!({"ok": true, "options": options})))
}

async fn pause_conversion(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    auth::require_auth(&state, &headers, true).await?;
    state.set_conversion_paused(true).await;
    state.events.emit("job", "info", "conversion queue paused");
    Ok(Json(serde_json::json!({"ok": true})))
}

async fn resume_conversion(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    auth::require_auth(&state, &headers, true).await?;
    state.set_conversion_paused(false).await;
    state.events.emit("job", "info", "conversion queue resumed");
    Ok(Json(serde_json::json!({"ok": true})))
}

async fn cancel_conversion(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    auth::require_auth(&state, &headers, true).await?;
    state.request_conversion_cancel().await;
    let cancelled = processor::cancel_queued_jobs(&state)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    state.events.emit(
        "job",
        "warn",
        format!("conversion cancel requested, queued jobs cancelled: {cancelled}"),
    );
    Ok(Json(
        serde_json::json!({"ok": true, "cancelled": cancelled}),
    ))
}

#[derive(Debug, Deserialize)]
struct ParallelismRequest {
    value: usize,
}

async fn set_conversion_parallelism(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<ParallelismRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    auth::require_auth(&state, &headers, true).await?;
    let value = state
        .set_conversion_parallelism(req.value)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    state.events.emit(
        "config",
        "ok",
        format!("conversion parallelism set to {value}"),
    );
    Ok(Json(serde_json::json!({"ok": true, "value": value})))
}

#[derive(Debug, Deserialize)]
struct ScheduleRequest {
    interval_seconds: u64,
}

async fn set_schedule(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<ScheduleRequest>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    auth::require_auth(&state, &headers, true).await?;
    let seconds = req.interval_seconds.min(7 * 24 * 3600);
    state
        .set_scan_interval(Duration::from_secs(seconds))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let message = if seconds == 0 {
        "已关闭定时扫描".to_string()
    } else {
        format!("定时扫描间隔已设为 {} 分钟", seconds / 60)
    };
    state.events.emit("config", "ok", &message);
    Ok(Json(serde_json::json!({
        "ok": true,
        "scan_interval_seconds": seconds,
    })))
}

async fn upload_subtitle(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    mut multipart: Multipart,
) -> Result<Json<serde_json::Value>, StatusCode> {
    auth::require_auth(&state, &headers, true).await?;
    let mut saved: Option<(PathBuf, String)> = None;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?
    {
        if field.name() != Some("file") {
            continue;
        }
        let original = field.file_name().unwrap_or("subtitle.ass").to_string();
        if !scanner::is_subtitle_path(FsPath::new(&original)) {
            return Err(StatusCode::BAD_REQUEST);
        }
        let display_name = original
            .rsplit(['/', '\\'])
            .next()
            .filter(|name| !name.trim().is_empty())
            .unwrap_or("subtitle.ass");
        let safe_name = sanitize_path_segment(display_name);
        let upload_dir = state
            .config
            .data_dir
            .join("uploads")
            .join(uuid::Uuid::new_v4().to_string());
        tokio::fs::create_dir_all(&upload_dir)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let path = upload_dir.join(&safe_name);
        let bytes = field.bytes().await.map_err(|_| StatusCode::BAD_REQUEST)?;
        tokio::fs::write(&path, &bytes)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        saved = Some((path, safe_name));
        break;
    }
    let Some((path, safe_name)) = saved else {
        return Err(StatusCode::BAD_REQUEST);
    };
    let subtitle_id = scanner::register_uploaded_subtitle(&state, &path, &safe_name)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let job_id = scanner::enqueue_subtitle_id(&state, subtitle_id, JobMode::Subset)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    state
        .events
        .emit("upload", "ok", format!("已上传并加入转换队列：{safe_name}"));
    Ok(Json(serde_json::json!({
        "ok": true,
        "file_id": subtitle_id,
        "job_id": job_id,
    })))
}

async fn jobs(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    auth::require_auth(&state, &headers, false).await?;
    let rows = sqlx::query(
        r#"
SELECT id, subtitle_id, path, mode, status, queued_at, started_at, finished_at, message, missing_fonts, stats
FROM jobs
ORDER BY id DESC
LIMIT 1000
"#,
    )
    .fetch_all(&state.db.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let data: Vec<_> = rows
        .into_iter()
        .map(|r| {
            serde_json::json!({
                "id": r.get::<i64, _>("id"),
                "subtitle_id": r.get::<i64, _>("subtitle_id"),
                "path": r.get::<String, _>("path"),
                "mode": r.get::<String, _>("mode"),
                "status": r.get::<String, _>("status"),
                "queued_at": r.get::<String, _>("queued_at"),
                "started_at": r.get::<Option<String>, _>("started_at"),
                "finished_at": r.get::<Option<String>, _>("finished_at"),
                "message": r.get::<Option<String>, _>("message"),
                "missing_fonts": parse_json_opt(r.get::<Option<String>, _>("missing_fonts")),
                "stats": parse_json_opt(r.get::<Option<String>, _>("stats")),
            })
        })
        .collect();
    Ok(Json(serde_json::json!({ "jobs": data })))
}

async fn files(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    auth::require_auth(&state, &headers, false).await?;
    let rows = sqlx::query(
        r#"
SELECT id, path, root_label, relative_path, size, mtime, last_status, last_processed_at,
       missing_fonts, error
FROM subtitle_files
ORDER BY id DESC
LIMIT 200
"#,
    )
    .fetch_all(&state.db.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut data = Vec::with_capacity(rows.len());
    for r in rows {
        let path = r.get::<String, _>("path");
        let analysis = analyze_subtitle_file(&path).await;
        data.push(serde_json::json!({
            "id": r.get::<i64, _>("id"),
            "path": path,
            "root_label": r.get::<String, _>("root_label"),
            "relative_path": r.get::<String, _>("relative_path"),
            "size": r.get::<i64, _>("size"),
            "mtime": r.get::<i64, _>("mtime"),
            "last_status": r.get::<Option<String>, _>("last_status"),
            "last_processed_at": r.get::<Option<String>, _>("last_processed_at"),
            "missing_fonts": parse_json_opt(r.get::<Option<String>, _>("missing_fonts")),
            "error": r.get::<Option<String>, _>("error"),
            "analysis": analysis,
        }));
    }
    Ok(Json(serde_json::json!({ "files": data })))
}

async fn download_file(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Result<Response, StatusCode> {
    auth::require_auth(&state, &headers, false).await?;
    let row = sqlx::query("SELECT path, relative_path FROM subtitle_files WHERE id = ?")
        .bind(id)
        .fetch_one(&state.db.pool)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    let path: String = row.get("path");
    let relative_path: String = row.get("relative_path");
    let bytes = tokio::fs::read(&path)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    let filename = sanitize_path_segment(
        FsPath::new(&relative_path)
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("subtitle.ass"),
    );
    let mut resp = bytes.into_response();
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    resp.headers_mut().insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&content_disposition_filename(&filename))
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    );
    Ok(resp)
}

async fn retry_job(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    auth::require_auth(&state, &headers, true).await?;
    let new_id = scanner::retry_job(&state, id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(serde_json::json!({"ok": true, "job_id": new_id})))
}

async fn process_file(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    auth::require_auth(&state, &headers, true).await?;
    let job_id = scanner::enqueue_subtitle_id(&state, id, JobMode::Subset)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(serde_json::json!({"ok": true, "job_id": job_id})))
}

async fn strip_embedded_file(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    auth::require_auth(&state, &headers, true).await?;
    let job_id = scanner::enqueue_subtitle_id(&state, id, JobMode::StripEmbedded)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(serde_json::json!({"ok": true, "job_id": job_id})))
}

async fn backups(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, StatusCode> {
    auth::require_auth(&state, &headers, false).await?;
    let rows = sqlx::query(
        "SELECT id, subtitle_id, source_path, backup_path, source_sha256, created_at FROM backups ORDER BY id DESC LIMIT 200",
    )
    .fetch_all(&state.db.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let data: Vec<_> = rows
        .into_iter()
        .map(|r| {
            serde_json::json!({
                "id": r.get::<i64, _>("id"),
                "subtitle_id": r.get::<Option<i64>, _>("subtitle_id"),
                "source_path": r.get::<String, _>("source_path"),
                "backup_path": r.get::<String, _>("backup_path"),
                "source_sha256": r.get::<String, _>("source_sha256"),
                "created_at": r.get::<String, _>("created_at"),
            })
        })
        .collect();
    Ok(Json(serde_json::json!({ "backups": data })))
}

async fn restore_backup(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    auth::require_auth(&state, &headers, true).await?;
    processor::restore_backup(&state, id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(serde_json::json!({"ok": true})))
}

async fn events(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Sse<impl futures_core::Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    auth::require_auth(&state, &headers, false).await?;
    let mut rx = state.events.subscribe();
    let stream = stream! {
        while let Ok(payload) = rx.recv().await {
            let event = Event::default()
                .event(payload.kind.clone())
                .json_data(payload)
                .unwrap_or_else(|_| Event::default().data("event serialization failed"));
            yield Ok(event);
        }
    };
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

fn parse_json_opt(raw: Option<String>) -> serde_json::Value {
    raw.and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or(serde_json::Value::Null)
}

fn option_label(key: &str) -> &str {
    match key {
        "embed_external_fonts" => "嵌入外部字体",
        "embed_system_fonts" => "嵌入系统字体",
        "include_ascii" => "保留 ASCII",
        "multi_weight" => "多字重",
        "randomize_font_names" => "随机字体名",
        "draw_subset" => "绘图字体",
        "full_font_embed" => "完整嵌入",
        "fallback_full_font_embed" => "失败回退完整嵌入",
        "variable_fonts" => "可变字体",
        _ => "处理选项",
    }
}

async fn analyze_subtitle_file(path: &str) -> serde_json::Value {
    let Ok(bytes) = tokio::fs::read(path).await else {
        return serde_json::Value::Null;
    };
    let Ok(decoded) = decode_subtitle(&bytes) else {
        return serde_json::Value::Null;
    };
    let parsed = parse_subtitle(&decoded.text);
    let embedded = parse_embedded_fonts(&decoded.text);
    let mut system_fonts = Vec::new();
    let mut third_party_fonts = Vec::new();
    let mut char_count = 0usize;
    for (name, usage) in parsed.usages {
        char_count += usage.all_codepoints().len();
        if is_system_font(&name) {
            system_fonts.push(name);
        } else {
            third_party_fonts.push(name);
        }
    }
    system_fonts.sort();
    third_party_fonts.sort();
    let mut embedded_fonts: Vec<String> = embedded.into_iter().map(|font| font.fontname).collect();
    embedded_fonts.sort();
    serde_json::json!({
        "drawing_count": parsed.drawing_count,
        "third_party_fonts": third_party_fonts,
        "system_fonts": system_fonts,
        "embedded_fonts": embedded_fonts,
        "char_count": char_count,
    })
}

fn content_disposition_filename(filename: &str) -> String {
    let fallback: String = filename
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '_'
            }
        })
        .collect();
    format!(
        "attachment; filename=\"{}\"; filename*=UTF-8''{}",
        fallback.trim_matches('_').if_empty("subtitle.ass"),
        percent_encode_utf8(filename)
    )
}

fn percent_encode_utf8(value: &str) -> String {
    let mut out = String::new();
    for &byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

trait EmptyFallback {
    fn if_empty<'a>(&'a self, fallback: &'a str) -> &'a str;
}

impl EmptyFallback for str {
    fn if_empty<'a>(&'a self, fallback: &'a str) -> &'a str {
        if self.is_empty() { fallback } else { self }
    }
}
