use chrono::{DateTime, Utc};
use tokio::sync::broadcast;

#[derive(Clone, Debug, serde::Serialize)]
pub struct EventPayload {
    pub ts: DateTime<Utc>,
    pub kind: String,
    pub level: String,
    pub message: String,
}

#[derive(Clone)]
pub struct EventBus {
    tx: broadcast::Sender<EventPayload>,
}

impl EventBus {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(512);
        Self { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<EventPayload> {
        self.tx.subscribe()
    }

    pub fn emit(
        &self,
        kind: impl Into<String>,
        level: impl Into<String>,
        message: impl Into<String>,
    ) {
        let kind = kind.into();
        let level = level.into();
        let message = message.into();
        match level.as_str() {
            "err" | "error" => tracing::error!(kind = %kind, message = %message),
            "warn" | "warning" => tracing::warn!(kind = %kind, message = %message),
            _ => tracing::info!(kind = %kind, level = %level, message = %message),
        }
        let payload = EventPayload {
            ts: Utc::now(),
            kind,
            level,
            message,
        };
        let _ = self.tx.send(payload);
    }
}
