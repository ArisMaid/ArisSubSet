use std::str::FromStr;

use anyhow::Context;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use sqlx::{Executor, Row, SqlitePool};

use crate::config::Config;

#[derive(Clone)]
pub struct Db {
    pub pool: SqlitePool,
}

async fn add_column_if_missing(
    conn: &mut sqlx::pool::PoolConnection<sqlx::Sqlite>,
    table: &str,
    column: &str,
    definition: &str,
) -> anyhow::Result<()> {
    let pragma = format!("PRAGMA table_info({table})");
    let rows = sqlx::query(&pragma).fetch_all(&mut **conn).await?;
    let exists = rows.iter().any(|row| {
        let name: String = row.get("name");
        name == column
    });
    if !exists {
        let sql = format!("ALTER TABLE {table} ADD COLUMN {column} {definition}");
        conn.execute(sql.as_str()).await?;
    }
    Ok(())
}

impl Db {
    pub async fn connect(config: &Config) -> anyhow::Result<Self> {
        let url = format!("sqlite://{}", config.database_path().display());
        let options = SqliteConnectOptions::from_str(&url)?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .foreign_keys(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await
            .context("failed to open sqlite database")?;
        let db = Self { pool };
        db.migrate().await?;
        Ok(db)
    }

    async fn migrate(&self) -> anyhow::Result<()> {
        let mut conn = self.pool.acquire().await?;
        conn.execute("PRAGMA foreign_keys = ON;").await?;
        conn.execute("PRAGMA synchronous = NORMAL;").await?;
        conn.execute("PRAGMA temp_store = MEMORY;").await?;
        conn.execute(
            r#"
CREATE TABLE IF NOT EXISTS font_files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  path TEXT NOT NULL UNIQUE,
  size INTEGER NOT NULL,
  mtime INTEGER NOT NULL,
  quick_hash TEXT NOT NULL,
  full_hash TEXT NOT NULL,
  format TEXT NOT NULL,
  status TEXT NOT NULL,
  error TEXT,
  indexed_at TEXT NOT NULL
);
"#,
        )
        .await?;
        conn.execute(
            r#"
CREATE TABLE IF NOT EXISTS font_faces (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  file_id INTEGER NOT NULL REFERENCES font_files(id) ON DELETE CASCADE,
  ttc_index INTEGER NOT NULL,
  family TEXT,
  full_name TEXT,
  postscript_name TEXT,
  subfamily TEXT,
  version TEXT,
  weight INTEGER NOT NULL DEFAULT 400,
  italic INTEGER NOT NULL DEFAULT 0
);
"#,
        )
        .await?;
        conn.execute(
            r#"
CREATE TABLE IF NOT EXISTS font_names (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  face_id INTEGER NOT NULL REFERENCES font_faces(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  normalized TEXT NOT NULL,
  kind TEXT NOT NULL
);
"#,
        )
        .await?;
        conn.execute("CREATE INDEX IF NOT EXISTS idx_font_names_norm ON font_names(normalized);")
            .await?;
        conn.execute("CREATE INDEX IF NOT EXISTS idx_font_names_face ON font_names(face_id);")
            .await?;
        conn.execute("CREATE INDEX IF NOT EXISTS idx_font_faces_file ON font_faces(file_id);")
            .await?;
        conn.execute(
            r#"
CREATE TABLE IF NOT EXISTS subtitle_files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  path TEXT NOT NULL UNIQUE,
  root_label TEXT NOT NULL,
  relative_path TEXT NOT NULL,
  size INTEGER NOT NULL,
  mtime INTEGER NOT NULL,
  sha256 TEXT NOT NULL,
  last_config_hash TEXT,
  last_status TEXT,
  last_processed_at TEXT,
  missing_fonts TEXT,
  error TEXT
);
"#,
        )
        .await?;
        conn.execute(
            r#"
CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  subtitle_id INTEGER NOT NULL REFERENCES subtitle_files(id) ON DELETE CASCADE,
  path TEXT NOT NULL,
  mode TEXT NOT NULL DEFAULT 'subset',
  status TEXT NOT NULL,
  queued_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  message TEXT,
  missing_fonts TEXT,
  stats TEXT
);
"#,
        )
        .await?;
        add_column_if_missing(&mut conn, "jobs", "mode", "TEXT NOT NULL DEFAULT 'subset'").await?;
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);")
            .await?;
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jobs_mode ON jobs(mode);")
            .await?;
        conn.execute(
            r#"
CREATE TABLE IF NOT EXISTS backups (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  subtitle_id INTEGER,
  source_path TEXT NOT NULL,
  backup_path TEXT NOT NULL UNIQUE,
  source_sha256 TEXT NOT NULL,
  created_at TEXT NOT NULL
);
"#,
        )
        .await?;
        conn.execute(
            r#"
CREATE TABLE IF NOT EXISTS watch_dirs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  path TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL
);
"#,
        )
        .await?;
        conn.execute(
            r#"
CREATE TABLE IF NOT EXISTS runtime_settings (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
"#,
        )
        .await?;
        Ok(())
    }
}
