use std::borrow::Cow;
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use base64::{Engine, engine::general_purpose};
use chrono::Utc;
use filetime::{FileTime, set_file_mtime};
use fs2::FileExt;
use sha2::{Digest, Sha256};
use sqlx::Row;
use tokio::io::AsyncReadExt;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::ass::{
    DrawRestoreEntry, FontUsage, decode_subtitle, encode_subtitle, is_system_font,
    normalize_lookup_name, parse_embedded_fonts, parse_font_subset_comments, parse_subtitle,
    rewrite_drawings_as_font, rewrite_strip_embedded, rewrite_with_embedded_fonts,
};
use crate::backup;
use crate::cache;
use crate::config::ProcessingOptions;
use crate::failure_log;
use crate::font_worker::{DrawFontRequest, RandomizeMap, SubsetRequest};
use crate::models::{EmbeddedFont, FontCandidate, FontSlot, JobMode};
use crate::state::AppState;

#[allow(dead_code)]
pub fn spawn_job_loop(mut rx: mpsc::Receiver<i64>, state: Arc<AppState>) {
    tokio::spawn(async move {
        let sem = Arc::new(Semaphore::new(state.config.max_concurrent_jobs));
        while let Some(job_id) = rx.recv().await {
            let permit = sem
                .clone()
                .acquire_owned()
                .await
                .expect("job semaphore closed");
            let st = state.clone();
            tokio::spawn(async move {
                let _permit = permit;
                if let Err(err) = process_job(st.clone(), job_id).await {
                    st.events
                        .emit("job", "err", format!("作业 #{job_id} 失败：{err:#}"));
                    let _ = fail_job(&st, job_id, &format!("{err:#}")).await;
                }
            });
        }
    });
}

pub fn spawn_controlled_job_loop(mut rx: mpsc::Receiver<i64>, state: Arc<AppState>) {
    tokio::spawn(async move {
        let mut pending = VecDeque::new();
        let mut active = JoinSet::new();
        let memory = Arc::new(Semaphore::new(state.config.max_conversion_memory_mb));
        loop {
            tokio::select! {
                received = rx.recv() => {
                    if let Some(job_id) = received {
                        pending.push_back(job_id);
                    } else if pending.is_empty() && active.is_empty() {
                        break;
                    }
                }
                Some(_) = active.join_next(), if !active.is_empty() => {}
                _ = tokio::time::sleep(std::time::Duration::from_millis(200)) => {}
            }

            if state.conversion_cancel_requested().await {
                while let Some(job_id) = pending.pop_front() {
                    let _ = cancel_job(&state, job_id, "cancelled before start").await;
                }
                let _ = cancel_queued_jobs(&state).await;
                if active.is_empty() {
                    state.clear_conversion_cancel().await;
                }
                continue;
            }

            if state.conversion_paused().await {
                continue;
            }

            let limit = state.conversion_parallelism().await;
            while active.len() < limit {
                let Some(job_id) = pending.pop_front() else {
                    break;
                };
                if !job_is_runnable(&state, job_id).await.unwrap_or(false) {
                    continue;
                }
                let st = state.clone();
                let memory = memory.clone();
                active.spawn(async move {
                    let result = async {
                        let _memory_permit = acquire_job_memory(&st, job_id, memory).await?;
                        process_job(st.clone(), job_id).await
                    }
                    .await;
                    if let Err(err) = result {
                        if st.conversion_cancel_requested().await {
                            let _ = cancel_job(&st, job_id, "cancelled while running").await;
                        } else {
                            st.events
                                .emit("job", "err", format!("job #{job_id} failed: {err:#}"));
                            let _ = fail_job(&st, job_id, &format!("{err:#}")).await;
                        }
                    }
                });
            }
        }
    });
}

pub async fn recover_incomplete_jobs(state: Arc<AppState>) -> anyhow::Result<usize> {
    sqlx::query("UPDATE jobs SET status='queued', started_at=NULL WHERE status='running'")
        .execute(&state.db.pool)
        .await?;
    let rows = sqlx::query("SELECT id FROM jobs WHERE status='queued' ORDER BY id ASC")
        .fetch_all(&state.db.pool)
        .await?;
    let mut count = 0usize;
    for row in rows {
        let job_id: i64 = row.get("id");
        state
            .job_tx
            .send(job_id)
            .await
            .context("job queue closed")?;
        count += 1;
    }
    if count > 0 {
        state
            .events
            .emit("job", "info", format!("recovered queued jobs: {count}"));
    }
    Ok(count)
}

async fn acquire_job_memory(
    state: &Arc<AppState>,
    job_id: i64,
    memory: Arc<Semaphore>,
) -> anyhow::Result<OwnedSemaphorePermit> {
    let size: i64 = sqlx::query_scalar(
        "SELECT sf.size FROM jobs j JOIN subtitle_files sf ON sf.id=j.subtitle_id WHERE j.id=?",
    )
    .bind(job_id)
    .fetch_one(&state.db.pool)
    .await?;
    let permits = memory_permits_for_size(size, state.config.max_conversion_memory_mb);
    memory
        .acquire_many_owned(permits)
        .await
        .context("conversion memory limiter closed")
}

fn memory_permits_for_size(size: i64, max_memory_mb: usize) -> u32 {
    const MIB: u64 = 1024 * 1024;
    const WORKING_SET_MULTIPLIER: u64 = 5;
    let estimated = (size.max(0) as u64).saturating_mul(WORKING_SET_MULTIPLIER);
    estimated
        .saturating_add(MIB - 1)
        .checked_div(MIB)
        .unwrap_or(1)
        .max(1)
        .min(max_memory_mb.max(1) as u64) as u32
}

#[derive(Debug, serde::Serialize)]
struct ProcessStats {
    embedded_count: usize,
    missing_count: usize,
    drawing_count: usize,
    embedded_removed_count: usize,
    random_names_restored: usize,
    drawings_restored: usize,
    draw_fonts_created: usize,
    original_size: u64,
    output_size: u64,
}

async fn process_job(state: Arc<AppState>, job_id: i64) -> anyhow::Result<()> {
    let row = sqlx::query("SELECT subtitle_id, path, mode, queued_at FROM jobs WHERE id = ?")
        .bind(job_id)
        .fetch_one(&state.db.pool)
        .await?;
    let subtitle_id: i64 = row.get("subtitle_id");
    let path: String = row.get("path");
    let mode = JobMode::from_db(&row.get::<String, _>("mode"));
    let queued_at: String = row.get("queued_at");
    let queue_latency = chrono::DateTime::parse_from_rfc3339(&queued_at)
        .ok()
        .and_then(|queued| (Utc::now() - queued.with_timezone(&Utc)).to_std().ok())
        .unwrap_or(Duration::ZERO);
    state.metrics.record_conversion_started(queue_latency);
    let started = Instant::now();
    let result = match mode {
        JobMode::Subset => process_subset_job(state.clone(), job_id, subtitle_id, path).await,
        JobMode::StripEmbedded => process_strip_job(state.clone(), job_id, subtitle_id, path).await,
    };
    state
        .metrics
        .record_conversion_finished(started.elapsed(), result.is_ok());
    result
}

async fn process_subset_job(
    state: Arc<AppState>,
    job_id: i64,
    subtitle_id: i64,
    path: String,
) -> anyhow::Result<()> {
    let options = state.processing_options().await;
    let config_hash = options.config_hash();
    let font_index_revision = state.font_index_revision().await;
    let path_buf = PathBuf::from(&path);
    let started = Utc::now().to_rfc3339();
    sqlx::query("UPDATE jobs SET status='running', started_at=?, message=NULL WHERE id=?")
        .bind(started)
        .bind(job_id)
        .execute(&state.db.pool)
        .await?;
    state
        .events
        .emit("job", "info", format!("开始转换：{path}"));

    if state.conversion_cancel_requested().await {
        bail!("cancelled");
    }
    let bytes = read_locked(&path_buf).await?;
    let original_size = bytes.len() as u64;
    let decoded = decode_subtitle(&bytes)?;
    let parsed = parse_subtitle(&decoded.text);

    let mut embedded = Vec::new();
    let mut rename_map = HashMap::new();
    let mut missing_fonts = Vec::new();
    let mut used_random_names = HashSet::new();
    let mut assigned_random_names: HashMap<String, String> = HashMap::new();

    for (font_name, usage) in parsed.usages.iter() {
        if state.conversion_cancel_requested().await {
            bail!("cancelled");
        }
        let normalized = normalize_lookup_name(font_name);
        if normalized.starts_with("assdrawsubset") {
            continue;
        }
        let system = is_system_font(font_name);
        if system && !options.embed_system_fonts {
            continue;
        }
        if !system && !options.embed_external_fonts {
            continue;
        }
        let candidates = query_candidates(&state, font_name).await?;
        if candidates.is_empty() {
            missing_fonts.push(font_name.clone());
            continue;
        }
        let embedded_name = if options.randomize_font_names {
            if let Some(name) = assigned_random_names.get(&normalized) {
                name.clone()
            } else {
                let name = stable_font_name(&normalized, &mut used_random_names);
                assigned_random_names.insert(normalized.clone(), name.clone());
                name
            }
        } else {
            font_name.clone()
        };
        if embedded_name != *font_name {
            rename_map.insert(normalized, embedded_name.clone());
        }
        subset_usage(
            &state,
            &options,
            font_name,
            &embedded_name,
            usage,
            &candidates,
            &mut embedded,
        )
        .await?;
    }

    let mut draw_fonts_created = 0usize;
    let base_text = if options.draw_subset {
        let rewritten = rewrite_drawings_as_font(&decoded.text, &parsed.newline);
        if !rewritten.entries.is_empty() {
            let draw_font = create_draw_font(&state, &rewritten.entries).await?;
            embedded.push(draw_font);
            draw_fonts_created = 1;
        }
        rewritten.text
    } else {
        decoded.text.clone()
    };

    let mut wrote_file = false;
    let final_bytes: Cow<'_, [u8]> = if embedded.is_empty() {
        Cow::Borrowed(&bytes)
    } else {
        let rewritten =
            rewrite_with_embedded_fonts(&base_text, &parsed.newline, &rename_map, &embedded);
        wrote_file = rewritten != decoded.text;
        Cow::Owned(encode_subtitle(&rewritten, &decoded.bom))
    };

    if wrote_file {
        let source_sha = sha256_hex(&bytes);
        let backup_path = backup::create(&state, subtitle_id, &path_buf, &source_sha).await?;
        state.events.emit(
            "backup",
            "ok",
            format!("已创建备份：{}", backup_path.display()),
        );
        write_replace(&path_buf, final_bytes.as_ref()).await?;
    }

    let new_sha = sha256_hex(final_bytes.as_ref());
    touch_processed_file(&path_buf).await?;
    let new_meta = tokio::fs::metadata(&path_buf).await?;
    let status = if missing_fonts.is_empty() {
        "success"
    } else {
        "partial"
    };
    let stats = ProcessStats {
        embedded_count: embedded.len(),
        missing_count: missing_fonts.len(),
        drawing_count: parsed.drawing_count,
        embedded_removed_count: 0,
        random_names_restored: 0,
        drawings_restored: 0,
        draw_fonts_created,
        original_size,
        output_size: final_bytes.len() as u64,
    };
    let missing_json = serde_json::to_string(&missing_fonts)?;
    let stats_json = serde_json::to_string(&stats)?;
    let finished = Utc::now().to_rfc3339();
    let message = if missing_fonts.is_empty() {
        format!("转换完成：嵌入 {} 个字体", embedded.len())
    } else {
        format!(
            "部分完成：嵌入 {} 个字体，缺失 {} 个字体",
            embedded.len(),
            missing_fonts.len()
        )
    };

    sqlx::query(
        "UPDATE jobs SET status=?, finished_at=?, message=?, missing_fonts=?, stats=? WHERE id=?",
    )
    .bind(status)
    .bind(&finished)
    .bind(&message)
    .bind(&missing_json)
    .bind(&stats_json)
    .bind(job_id)
    .execute(&state.db.pool)
    .await?;
    sqlx::query(
        r#"
UPDATE subtitle_files
SET size=?, mtime=?, sha256=?, last_config_hash=?, last_status=?, last_processed_at=?,
    missing_fonts=?, error=NULL, last_font_index_revision=?
WHERE id=?
"#,
    )
    .bind(new_meta.len() as i64)
    .bind(
        new_meta
            .modified()
            .ok()
            .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0),
    )
    .bind(new_sha)
    .bind(&config_hash)
    .bind(status)
    .bind(&finished)
    .bind(&missing_json)
    .bind(font_index_revision as i64)
    .bind(subtitle_id)
    .execute(&state.db.pool)
    .await?;
    state.events.emit("job", "ok", format!("{message}：{path}"));
    Ok(())
}

async fn process_strip_job(
    state: Arc<AppState>,
    job_id: i64,
    subtitle_id: i64,
    path: String,
) -> anyhow::Result<()> {
    let config_hash = state.config_hash().await;
    let font_index_revision = state.font_index_revision().await;
    let path_buf = PathBuf::from(&path);
    let started = Utc::now().to_rfc3339();
    sqlx::query("UPDATE jobs SET status='running', started_at=?, message=NULL WHERE id=?")
        .bind(started)
        .bind(job_id)
        .execute(&state.db.pool)
        .await?;
    state
        .events
        .emit("job", "info", format!("开始清理还原：{path}"));

    if state.conversion_cancel_requested().await {
        bail!("cancelled");
    }
    let bytes = read_locked(&path_buf).await?;
    let original_size = bytes.len() as u64;
    let decoded = decode_subtitle(&bytes)?;
    let parsed = parse_subtitle(&decoded.text);
    let embedded_fonts = parse_embedded_fonts(&decoded.text);
    let comment_map = parse_font_subset_comments(&decoded.text);
    let mut restore_map = comment_map.clone();
    let mut draw_map: HashMap<String, DrawRestoreEntry> = HashMap::new();
    let mut kept_fonts = Vec::new();
    let mut removed_count = 0usize;
    let mut warnings = Vec::new();

    for font in &embedded_fonts {
        if state.conversion_cancel_requested().await {
            bail!("cancelled");
        }
        let mut removable = false;
        let family = embedded_family_name(&font.fontname);
        if comment_map.contains_key(&normalize_lookup_name(&family))
            || normalize_lookup_name(&family).starts_with("assdrawsubset")
        {
            removable = true;
        }
        let encoded = general_purpose::STANDARD.encode(&font.data);
        match state.workers.inspect_embedded_font(&encoded).await {
            Ok(meta) => {
                if let Some(map) = meta.font_subset_map {
                    restore_map.insert(normalize_lookup_name(&map.subset), map.original);
                    removable = true;
                }
                if !meta.draw_entries.is_empty() {
                    for entry in meta.draw_entries {
                        draw_map.insert(
                            entry.ch.clone(),
                            DrawRestoreEntry {
                                data: entry.data,
                                ch: entry.ch,
                                flags: entry.flags,
                            },
                        );
                    }
                    removable = true;
                }
            }
            Err(err) => {
                warnings.push(format!("kept {}: {err:#}", font.fontname));
            }
        }
        if removable {
            removed_count += 1;
        } else {
            kept_fonts.push(font.clone());
        }
    }

    let rewritten = rewrite_strip_embedded(
        &decoded.text,
        &parsed.newline,
        &restore_map,
        &kept_fonts,
        &draw_map,
    );
    let wrote_file = rewritten != decoded.text;
    let final_bytes: Cow<'_, [u8]> = if wrote_file {
        Cow::Owned(encode_subtitle(&rewritten, &decoded.bom))
    } else {
        Cow::Borrowed(&bytes)
    };
    if wrote_file {
        let source_sha = sha256_hex(&bytes);
        let backup_path = backup::create(&state, subtitle_id, &path_buf, &source_sha).await?;
        state.events.emit(
            "backup",
            "ok",
            format!("已创建备份：{}", backup_path.display()),
        );
        write_replace(&path_buf, final_bytes.as_ref()).await?;
    }

    let new_sha = sha256_hex(final_bytes.as_ref());
    touch_processed_file(&path_buf).await?;
    let new_meta = tokio::fs::metadata(&path_buf).await?;
    let status = if warnings.is_empty() {
        "success"
    } else {
        "partial"
    };
    let stats = ProcessStats {
        embedded_count: embedded_fonts.len(),
        missing_count: warnings.len(),
        drawing_count: parsed.drawing_count,
        embedded_removed_count: removed_count,
        random_names_restored: restore_map.len(),
        drawings_restored: draw_map.len(),
        draw_fonts_created: 0,
        original_size,
        output_size: final_bytes.len() as u64,
    };
    let warnings_json = serde_json::to_string(&warnings)?;
    let stats_json = serde_json::to_string(&stats)?;
    let finished = Utc::now().to_rfc3339();
    let message = if warnings.is_empty() {
        format!(
            "清理完成：移除 {removed_count} 个字体，还原 {} 个名称",
            restore_map.len()
        )
    } else {
        format!(
            "部分完成：移除 {removed_count} 个字体，保留 {} 个需检查字体",
            warnings.len()
        )
    };
    sqlx::query(
        "UPDATE jobs SET status=?, finished_at=?, message=?, missing_fonts=?, stats=? WHERE id=?",
    )
    .bind(status)
    .bind(&finished)
    .bind(&message)
    .bind(&warnings_json)
    .bind(&stats_json)
    .bind(job_id)
    .execute(&state.db.pool)
    .await?;
    sqlx::query(
        r#"
UPDATE subtitle_files
SET size=?, mtime=?, sha256=?, last_config_hash=?, last_status=?, last_processed_at=?,
    missing_fonts=?, error=NULL, last_font_index_revision=?
WHERE id=?
"#,
    )
    .bind(new_meta.len() as i64)
    .bind(
        new_meta
            .modified()
            .ok()
            .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0),
    )
    .bind(new_sha)
    .bind(config_hash)
    .bind(status)
    .bind(&finished)
    .bind(&warnings_json)
    .bind(font_index_revision as i64)
    .bind(subtitle_id)
    .execute(&state.db.pool)
    .await?;
    state.events.emit("job", "ok", format!("{message}：{path}"));
    Ok(())
}

async fn subset_usage(
    state: &Arc<AppState>,
    options: &ProcessingOptions,
    original_name: &str,
    embedded_name: &str,
    usage: &FontUsage,
    candidates: &[FontCandidate],
    embedded: &mut Vec<EmbeddedFont>,
) -> anyhow::Result<()> {
    if options.multi_weight {
        for slot in [
            FontSlot::Normal,
            FontSlot::Bold,
            FontSlot::Italic,
            FontSlot::BoldItalic,
        ] {
            let cps = usage.slot_codepoints(slot);
            if cps.is_empty() {
                continue;
            }
            let Some(candidate) = select_best_candidate(candidates, slot) else {
                continue;
            };
            let font = subset_candidate(
                state,
                options,
                original_name,
                embedded_name,
                slot,
                &cps,
                candidate,
            )
            .await?;
            embedded.push(font);
        }
    } else {
        let cps = usage.all_codepoints();
        if cps.is_empty() {
            return Ok(());
        }
        let Some(candidate) = select_best_candidate(candidates, FontSlot::Normal) else {
            return Ok(());
        };
        let font = subset_candidate(
            state,
            options,
            original_name,
            embedded_name,
            FontSlot::Normal,
            &cps,
            candidate,
        )
        .await?;
        embedded.push(font);
    }
    Ok(())
}

async fn create_draw_font(
    state: &Arc<AppState>,
    entries: &[DrawRestoreEntry],
) -> anyhow::Result<EmbeddedFont> {
    let cache_key = draw_cache_key(entries);
    let output_path = state
        .config
        .subset_cache_dir()
        .join(format!("{cache_key}.draw.ttf"));
    let cache_lock = state.cache_lock(&format!("draw:{cache_key}")).await;
    let _cache_guard = cache_lock.lock().await;
    let cache_hit = cache::file_is_ready(&output_path).await;
    state.metrics.record_cache_lookup(cache_hit);
    if !cache_hit {
        tokio::fs::create_dir_all(state.config.subset_cache_dir()).await?;
        cache::remove_file_if_exists(&output_path).await?;
        let temp_path = cache::temp_path(&output_path);
        let output_path_s = temp_path.to_string_lossy().to_string();
        let worker_entries: Vec<crate::font_worker::DrawTableEntry> = entries
            .iter()
            .map(|entry| crate::font_worker::DrawTableEntry {
                data: entry.data.clone(),
                ch: entry.ch.clone(),
                flags: entry.flags,
            })
            .collect();
        let req = DrawFontRequest {
            output_path: &output_path_s,
            family: "ASSDrawSubset",
            drawings: &worker_entries,
            service_version: env!("CARGO_PKG_VERSION"),
        };
        if let Err(error) = state.workers.create_draw_font(&req).await {
            let _ = cache::remove_file_if_exists(&temp_path).await;
            return Err(error);
        }
        if !cache::file_is_ready(&temp_path).await {
            let _ = cache::remove_file_if_exists(&temp_path).await;
            bail!(
                "draw font worker did not create a valid cache file {}",
                temp_path.display()
            );
        }
        cache::publish(state, &temp_path, &output_path).await?;
    }
    let data = cache::read_and_touch(&output_path).await.with_context(|| {
        format!(
            "draw font worker did not create expected cache file {}",
            output_path.display()
        )
    })?;
    Ok(EmbeddedFont {
        original_name: "ASSDrawSubset".to_string(),
        embedded_name: "ASSDrawSubset".to_string(),
        slot: FontSlot::Normal,
        data,
    })
}

fn draw_cache_key(entries: &[DrawRestoreEntry]) -> String {
    let mut h = Sha256::new();
    h.update(b"draw-cache-v3");
    for entry in entries {
        h.update(entry.data.as_bytes());
        h.update(entry.ch.as_bytes());
        h.update([entry.flags]);
    }
    hex::encode(h.finalize())
}

async fn subset_candidate(
    state: &Arc<AppState>,
    options: &ProcessingOptions,
    original_name: &str,
    embedded_name: &str,
    slot: FontSlot,
    codepoints: &[u32],
    candidate: &FontCandidate,
) -> anyhow::Result<EmbeddedFont> {
    let font_hash = ensure_candidate_full_hash(state, candidate).await?;
    let cache_key = subset_cache_key(
        embedded_name,
        slot,
        codepoints,
        candidate,
        &font_hash,
        options,
    );
    let output_path = state
        .config
        .subset_cache_dir()
        .join(format!("{cache_key}.ttf"));
    let cache_lock = state.cache_lock(&format!("font:{cache_key}")).await;
    let _cache_guard = cache_lock.lock().await;
    let cache_hit = cache::file_is_ready(&output_path).await;
    state.metrics.record_cache_lookup(cache_hit);
    if !cache_hit {
        tokio::fs::create_dir_all(state.config.subset_cache_dir()).await?;
        cache::remove_file_if_exists(&output_path).await?;
        let subfamily = match slot {
            FontSlot::Normal => "Regular",
            FontSlot::Bold => "Bold",
            FontSlot::Italic => "Italic",
            FontSlot::BoldItalic => "Bold Italic",
        };
        let randomize_map = if embedded_name != original_name {
            Some(RandomizeMap {
                original: original_name,
                subset: embedded_name,
            })
        } else {
            None
        };
        let temp_path = cache::temp_path(&output_path);
        let output_path_s = temp_path.to_string_lossy().to_string();
        let req = SubsetRequest {
            source_path: &candidate.path,
            ttc_index: candidate.ttc_index,
            output_path: &output_path_s,
            codepoints,
            include_ascii: options.include_ascii,
            full_font: options.full_font_embed,
            retain_variations: options.variable_fonts,
            target_family: embedded_name,
            original_family: original_name,
            subfamily,
            randomize_map,
            service_version: env!("CARGO_PKG_VERSION"),
        };
        let generation_result = if let Err(err) = state.workers.subset_font(&req).await {
            if options.full_font_embed || !options.fallback_full_font_embed {
                Err(err)
            } else {
                let fallback_req = SubsetRequest {
                    source_path: &candidate.path,
                    ttc_index: candidate.ttc_index,
                    output_path: &output_path_s,
                    codepoints,
                    include_ascii: options.include_ascii,
                    full_font: true,
                    retain_variations: options.variable_fonts,
                    target_family: embedded_name,
                    original_family: original_name,
                    subfamily,
                    randomize_map,
                    service_version: env!("CARGO_PKG_VERSION"),
                };
                state.events.emit(
                    "job",
                    "warn",
                    format!("subset failed, retrying full embed for {original_name}: {err:#}"),
                );
                state.workers.subset_font(&fallback_req).await.map(|_| ())
            }
        } else {
            Ok(())
        };
        if let Err(error) = generation_result {
            let _ = cache::remove_file_if_exists(&temp_path).await;
            return Err(error);
        }
        if !cache::file_is_ready(&temp_path).await {
            let _ = cache::remove_file_if_exists(&temp_path).await;
            bail!(
                "subset worker did not create a valid cache file {}",
                temp_path.display()
            );
        }
        if let Err(error) = cache::publish(state, &temp_path, &output_path).await {
            let _ = cache::remove_file_if_exists(&temp_path).await;
            return Err(error);
        }
    }
    let data = cache::read_and_touch(&output_path).await.with_context(|| {
        format!(
            "subset worker did not create expected cache file {}",
            output_path.display()
        )
    })?;
    Ok(EmbeddedFont {
        original_name: original_name.to_string(),
        embedded_name: embedded_name.to_string(),
        slot,
        data,
    })
}

fn subset_cache_key(
    embedded_name: &str,
    slot: FontSlot,
    codepoints: &[u32],
    candidate: &FontCandidate,
    font_hash: &str,
    options: &ProcessingOptions,
) -> String {
    let mut h = Sha256::new();
    h.update(b"font-cache-v3");
    h.update(font_hash.as_bytes());
    h.update(candidate.ttc_index.to_le_bytes());
    h.update(slot.as_str().as_bytes());
    h.update([options.include_ascii as u8]);
    h.update([options.full_font_embed as u8]);
    h.update([options.fallback_full_font_embed as u8]);
    h.update([options.variable_fonts as u8]);
    h.update(embedded_name.as_bytes());
    for cp in codepoints {
        h.update(cp.to_le_bytes());
    }
    hex::encode(h.finalize())
}

fn embedded_family_name(fontname: &str) -> String {
    let mut name = fontname.trim().to_string();
    for suffix in ["_BI0.ttf", "_B0.ttf", "_I0.ttf", "_0.ttf", ".ttf", ".otf"] {
        if name
            .to_ascii_lowercase()
            .ends_with(&suffix.to_ascii_lowercase())
        {
            let keep = name.len().saturating_sub(suffix.len());
            name.truncate(keep);
            break;
        }
    }
    name
}

async fn ensure_candidate_full_hash(
    state: &Arc<AppState>,
    candidate: &FontCandidate,
) -> anyhow::Result<String> {
    if !candidate.full_hash.trim().is_empty() {
        return Ok(candidate.full_hash.clone());
    }
    let cache_lock = state
        .cache_lock(&format!("font-hash:{}", candidate.file_id))
        .await;
    let _cache_guard = cache_lock.lock().await;
    let existing: Option<String> =
        sqlx::query_scalar("SELECT full_hash FROM font_files WHERE id = ?")
            .bind(candidate.file_id)
            .fetch_optional(&state.db.pool)
            .await?;
    if let Some(existing) = existing.filter(|hash| !hash.trim().is_empty()) {
        return Ok(existing);
    }
    let hash = sha256_file(Path::new(&candidate.path)).await?;
    sqlx::query("UPDATE font_files SET full_hash = ? WHERE id = ? AND full_hash = ''")
        .bind(&hash)
        .bind(candidate.file_id)
        .execute(&state.db.pool)
        .await?;
    Ok(hash)
}

async fn query_candidates(
    state: &Arc<AppState>,
    font_name: &str,
) -> anyhow::Result<Vec<FontCandidate>> {
    let normalized = normalize_lookup_name(font_name);
    let rows = sqlx::query(
        r#"
SELECT DISTINCT
  f.id AS file_id,
  f.path AS path,
  f.full_hash AS full_hash,
  ff.ttc_index AS ttc_index,
  ff.weight AS weight,
  ff.italic AS italic
FROM font_names n
JOIN font_faces ff ON ff.id = n.face_id
JOIN font_files f ON f.id = ff.file_id
WHERE n.normalized = ? AND f.status = 'ok'
"#,
    )
    .bind(normalized)
    .fetch_all(&state.db.pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| FontCandidate {
            file_id: row.get("file_id"),
            path: row.get("path"),
            full_hash: row.get("full_hash"),
            ttc_index: row.get("ttc_index"),
            weight: row.get("weight"),
            italic: row.get::<i64, _>("italic") != 0,
        })
        .collect())
}

fn select_best_candidate(candidates: &[FontCandidate], slot: FontSlot) -> Option<&FontCandidate> {
    let (target_weight, target_italic) = slot.target();
    candidates.iter().min_by_key(|c| {
        let weight_score = (c.weight - target_weight).abs();
        let italic_score = if c.italic == target_italic { 0 } else { 10_000 };
        italic_score + weight_score
    })
}

async fn write_replace(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let file_name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("subtitle.ass");
    let tmp = parent.join(format!(".{file_name}.{}.tmp", Uuid::new_v4()));
    tokio::fs::write(&tmp, bytes).await?;
    #[cfg(windows)]
    {
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        std::fs::rename(&tmp, path)?;
    }
    #[cfg(not(windows))]
    {
        tokio::fs::rename(&tmp, path).await?;
    }
    Ok(())
}

async fn touch_processed_file(path: &Path) -> anyhow::Result<()> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        set_file_mtime(&path, FileTime::now())
            .with_context(|| format!("touch processed subtitle {}", path.display()))
    })
    .await
    .context("touch processed subtitle task failed")?
}

async fn read_locked(path: &Path) -> anyhow::Result<Vec<u8>> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || read_locked_sync(&path))
        .await
        .context("read locked subtitle task failed")?
}

fn read_locked_sync(path: &Path) -> anyhow::Result<Vec<u8>> {
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .open(path)
        .with_context(|| format!("open subtitle {}", path.display()))?;
    file.try_lock_exclusive()
        .with_context(|| format!("lock subtitle {}", path.display()))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    FileExt::unlock(&file)?;
    Ok(bytes)
}

async fn fail_job(state: &Arc<AppState>, job_id: i64, error: &str) -> anyhow::Result<()> {
    let finished = Utc::now().to_rfc3339();
    sqlx::query("UPDATE jobs SET status='failed', finished_at=?, message=? WHERE id=?")
        .bind(&finished)
        .bind(error)
        .bind(job_id)
        .execute(&state.db.pool)
        .await?;
    if let Some(row) = sqlx::query("SELECT subtitle_id, path, mode FROM jobs WHERE id=?")
        .bind(job_id)
        .fetch_optional(&state.db.pool)
        .await?
    {
        let subtitle_id: i64 = row.get("subtitle_id");
        let path: String = row.get("path");
        let mode: String = row.get("mode");
        sqlx::query("UPDATE subtitle_files SET last_status='failed', error=? WHERE id=?")
            .bind(error)
            .bind(subtitle_id)
            .execute(&state.db.pool)
            .await?;
        if let Err(err) = failure_log::append(
            state,
            job_id,
            Some(subtitle_id),
            &path,
            &mode,
            error,
            &finished,
        )
        .await
        {
            state
                .events
                .emit("job", "warn", format!("failed to write error log: {err:#}"));
        }
    }
    Ok(())
}

async fn cancel_job(state: &Arc<AppState>, job_id: i64, message: &str) -> anyhow::Result<()> {
    let finished = Utc::now().to_rfc3339();
    sqlx::query("UPDATE jobs SET status='cancelled', finished_at=?, message=? WHERE id=?")
        .bind(&finished)
        .bind(message)
        .bind(job_id)
        .execute(&state.db.pool)
        .await?;
    Ok(())
}

pub async fn cancel_queued_jobs(state: &Arc<AppState>) -> anyhow::Result<u64> {
    let finished = Utc::now().to_rfc3339();
    let result = sqlx::query(
        "UPDATE jobs SET status='cancelled', finished_at=?, message='cancelled before start' WHERE status='queued'",
    )
    .bind(finished)
    .execute(&state.db.pool)
    .await?;
    Ok(result.rows_affected())
}

async fn job_is_runnable(state: &Arc<AppState>, job_id: i64) -> anyhow::Result<bool> {
    let status: Option<String> = sqlx::query_scalar("SELECT status FROM jobs WHERE id=?")
        .bind(job_id)
        .fetch_optional(&state.db.pool)
        .await?;
    Ok(matches!(status.as_deref(), Some("queued")))
}

fn stable_font_name(normalized_name: &str, used: &mut HashSet<String>) -> String {
    for salt in 0u32.. {
        let mut hash = Sha256::new();
        hash.update(b"stable-font-name-v1");
        hash.update(normalized_name.as_bytes());
        hash.update(salt.to_le_bytes());
        let digest = hex::encode_upper(hash.finalize());
        let name = format!("AS{}", &digest[..10]);
        if used.insert(name.clone()) {
            return name;
        }
    }
    unreachable!("32-bit stable font name salt space exhausted")
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex::encode(h.finalize())
}

async fn sha256_file(path: &Path) -> anyhow::Result<String> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("open {}", path.display()))?;
    let mut hash = Sha256::new();
    let mut buffer = vec![0u8; 256 * 1024];
    loop {
        let n = file.read(&mut buffer).await?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(hex::encode(hash.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate() -> FontCandidate {
        FontCandidate {
            file_id: 1,
            path: "font.ttf".to_string(),
            full_hash: "abc".to_string(),
            ttc_index: -1,
            weight: 400,
            italic: false,
        }
    }

    #[test]
    fn memory_budget_rounds_up_and_caps_large_subtitles() {
        assert_eq!(memory_permits_for_size(0, 512), 1);
        assert_eq!(memory_permits_for_size(1024 * 1024, 512), 5);
        assert_eq!(memory_permits_for_size(200 * 1024 * 1024, 512), 512);
        assert_eq!(memory_permits_for_size(-1, 512), 1);
    }

    #[test]
    fn stable_names_are_repeatable_and_collision_safe() {
        let first = stable_font_name("example font", &mut HashSet::new());
        let second = stable_font_name("example font", &mut HashSet::new());
        assert_eq!(first, second);
        assert_eq!(first.len(), 12);

        let mut used = HashSet::from([first.clone()]);
        assert_ne!(stable_font_name("example font", &mut used), first);
    }

    #[test]
    fn cache_key_ignores_non_font_processing_switches() {
        let base = ProcessingOptions::default();
        let key = subset_cache_key(
            "AS1234567890",
            FontSlot::Normal,
            &[65, 66],
            &candidate(),
            "font-hash",
            &base,
        );
        let mut changed = base.clone();
        changed.embed_external_fonts = !changed.embed_external_fonts;
        changed.embed_system_fonts = !changed.embed_system_fonts;
        changed.multi_weight = !changed.multi_weight;
        changed.randomize_font_names = !changed.randomize_font_names;
        changed.draw_subset = !changed.draw_subset;
        assert_eq!(
            key,
            subset_cache_key(
                "AS1234567890",
                FontSlot::Normal,
                &[65, 66],
                &candidate(),
                "font-hash",
                &changed,
            )
        );

        changed.include_ascii = !changed.include_ascii;
        assert_ne!(
            key,
            subset_cache_key(
                "AS1234567890",
                FontSlot::Normal,
                &[65, 66],
                &candidate(),
                "font-hash",
                &changed,
            )
        );
    }
}
