use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::http::{HeaderMap, StatusCode, header};
use rand::{Rng, distributions::Alphanumeric};
use sha2::{Digest, Sha256};

use crate::state::AppState;

pub const SESSION_COOKIE: &str = "ass_subset_session";
const SESSION_TTL: Duration = Duration::from_secs(24 * 60 * 60);

#[derive(Clone, Debug)]
pub struct Session {
    pub csrf: String,
    pub expires_at: Instant,
}

#[derive(Debug)]
pub struct AuthInfo {
    pub token: String,
    pub csrf: String,
}

pub fn verify_password(state: &AppState, password: &str) -> bool {
    if state.config.allow_no_auth {
        return true;
    }
    if let Some(plain) = &state.config.admin_password_plain {
        return password == plain;
    }
    let Some(hash) = &state.config.admin_password_hash else {
        return false;
    };
    if let Some(expected) = hash.strip_prefix("sha256:") {
        let mut h = Sha256::new();
        h.update(password.as_bytes());
        return hex::encode(h.finalize()).eq_ignore_ascii_case(expected.trim());
    }
    false
}

pub async fn create_session(state: &Arc<AppState>) -> AuthInfo {
    let token = random_token(48);
    let csrf = random_token(32);
    let session = Session {
        csrf: csrf.clone(),
        expires_at: Instant::now() + SESSION_TTL,
    };
    state.sessions.write().await.insert(token.clone(), session);
    AuthInfo { token, csrf }
}

pub async fn require_auth(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    require_csrf: bool,
) -> Result<AuthInfo, StatusCode> {
    if state.config.allow_no_auth {
        return Ok(AuthInfo {
            token: "dev".to_string(),
            csrf: "dev".to_string(),
        });
    }
    let token = read_cookie(headers, SESSION_COOKIE).ok_or(StatusCode::UNAUTHORIZED)?;
    let sessions = state.sessions.read().await;
    let session = sessions.get(&token).ok_or(StatusCode::UNAUTHORIZED)?;
    if session.expires_at <= Instant::now() {
        return Err(StatusCode::UNAUTHORIZED);
    }
    if require_csrf {
        let got = headers
            .get("x-csrf-token")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if got != session.csrf {
            return Err(StatusCode::FORBIDDEN);
        }
    }
    Ok(AuthInfo {
        token,
        csrf: session.csrf.clone(),
    })
}

pub async fn cleanup_sessions(state: &Arc<AppState>) {
    let now = Instant::now();
    state
        .sessions
        .write()
        .await
        .retain(|_, s| s.expires_at > now);
}

pub fn session_cookie(token: &str) -> String {
    format!("{SESSION_COOKIE}={token}; HttpOnly; SameSite=Lax; Path=/; Max-Age=86400")
}

fn random_token(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn read_cookie(headers: &HeaderMap, name: &str) -> Option<String> {
    let raw = headers.get(header::COOKIE)?.to_str().ok()?;
    for part in raw.split(';') {
        let (k, v) = part.trim().split_once('=')?;
        if k == name {
            return Some(v.to_string());
        }
    }
    None
}
