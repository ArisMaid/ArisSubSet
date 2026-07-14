use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use argon2::password_hash::PasswordHash;
use argon2::{Argon2, PasswordVerifier};
use axum::http::{HeaderMap, StatusCode, header};
use rand::{Rng, distributions::Alphanumeric};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use tokio::sync::Mutex;

use crate::state::AppState;

pub const SESSION_COOKIE: &str = "ass_subset_session";
const SESSION_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const LOGIN_WINDOW: Duration = Duration::from_secs(60);
const LOGIN_BLOCK: Duration = Duration::from_secs(60);
const LOGIN_MAX_FAILURES: u32 = 5;
const LOGIN_ENTRY_TTL: Duration = Duration::from_secs(15 * 60);

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

pub struct LoginRateLimiter {
    attempts: Mutex<HashMap<String, LoginAttempt>>,
}

struct LoginAttempt {
    failures: u32,
    window_started: Instant,
    blocked_until: Option<Instant>,
    last_seen: Instant,
}

impl LoginRateLimiter {
    pub fn new() -> Self {
        Self {
            attempts: Mutex::new(HashMap::new()),
        }
    }

    pub async fn retry_after(&self, key: &str) -> Option<u64> {
        let now = Instant::now();
        let mut attempts = self.attempts.lock().await;
        attempts.retain(|_, attempt| now.duration_since(attempt.last_seen) < LOGIN_ENTRY_TTL);
        let attempt = attempts.get_mut(key)?;
        attempt.last_seen = now;
        let blocked_until = attempt.blocked_until?;
        if blocked_until <= now {
            attempt.blocked_until = None;
            attempt.failures = 0;
            attempt.window_started = now;
            return None;
        }
        Some(blocked_until.duration_since(now).as_secs().max(1))
    }

    pub async fn record_failure(&self, key: &str) {
        let now = Instant::now();
        let mut attempts = self.attempts.lock().await;
        let attempt = attempts
            .entry(key.to_string())
            .or_insert_with(|| LoginAttempt {
                failures: 0,
                window_started: now,
                blocked_until: None,
                last_seen: now,
            });
        if now.duration_since(attempt.window_started) >= LOGIN_WINDOW {
            attempt.failures = 0;
            attempt.window_started = now;
        }
        attempt.failures = attempt.failures.saturating_add(1);
        attempt.last_seen = now;
        if attempt.failures >= LOGIN_MAX_FAILURES {
            attempt.blocked_until = Some(now + LOGIN_BLOCK);
        }
    }

    pub async fn record_success(&self, key: &str) {
        self.attempts.lock().await.remove(key);
    }
}

pub async fn verify_password(state: &AppState, password: &str) -> bool {
    if state.config.allow_no_auth {
        return true;
    }
    let plain = state.config.admin_password_plain.clone();
    let hash = state.config.admin_password_hash.clone();
    let password = password.to_string();
    tokio::task::spawn_blocking(move || {
        verify_password_value(plain.as_deref(), hash.as_deref(), &password)
    })
    .await
    .unwrap_or(false)
}

fn verify_password_value(plain: Option<&str>, hash: Option<&str>, password: &str) -> bool {
    if let Some(plain) = plain {
        return constant_time_eq(plain.as_bytes(), password.as_bytes());
    }
    let Some(hash) = hash else {
        return false;
    };
    if hash.starts_with("$argon2") {
        let Ok(parsed) = PasswordHash::new(hash) else {
            return false;
        };
        return Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok();
    }
    if let Some(expected) = hash.strip_prefix("sha256:") {
        let mut h = Sha256::new();
        h.update(password.as_bytes());
        let actual = hex::encode(h.finalize());
        return constant_time_eq(
            actual.to_ascii_lowercase().as_bytes(),
            expected.trim().to_ascii_lowercase().as_bytes(),
        );
    }
    false
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len() && left.ct_eq(right).into()
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

pub fn session_cookie(token: &str, secure: bool) -> String {
    let secure = if secure { "; Secure" } else { "" };
    format!("{SESSION_COOKIE}={token}; HttpOnly; SameSite=Lax; Path=/; Max-Age=86400{secure}")
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

#[cfg(test)]
mod tests {
    use super::*;
    use argon2::password_hash::{PasswordHasher, SaltString};

    #[test]
    fn verifies_argon2_and_legacy_sha256_hashes() {
        let salt = SaltString::encode_b64(b"0123456789abcdef").unwrap();
        let argon = Argon2::default()
            .hash_password(b"secret", &salt)
            .unwrap()
            .to_string();
        assert!(verify_password_value(None, Some(&argon), "secret"));
        assert!(!verify_password_value(None, Some(&argon), "wrong"));

        let legacy = format!("sha256:{}", hex::encode(Sha256::digest(b"secret")));
        assert!(verify_password_value(None, Some(&legacy), "secret"));
        assert!(!verify_password_value(None, Some(&legacy), "wrong"));
    }

    #[tokio::test]
    async fn repeated_login_failures_are_temporarily_blocked() {
        let limiter = LoginRateLimiter::new();
        for _ in 0..LOGIN_MAX_FAILURES {
            limiter.record_failure("client").await;
        }
        assert!(limiter.retry_after("client").await.is_some());
        limiter.record_success("client").await;
        assert!(limiter.retry_after("client").await.is_none());
    }
}
