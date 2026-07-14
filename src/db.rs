use std::str::FromStr;
use std::time::Duration;

use crate::sqlx::sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
};
use crate::sqlx::{Executor, Row, SqlitePool};
use anyhow::Context;

use crate::config::Config;

const MIGRATION_TABLE_SQL: &str = include_str!("../migrations/0000_schema_migrations.sql");
const BASELINE_SQL: &str = include_str!("../migrations/0001_baseline.sql");
const ANALYSIS_INDEXES_SQL: &str = include_str!("../migrations/0002_analysis_and_job_indexes.sql");
const BACKUP_RETENTION_SQL: &str = include_str!("../migrations/0003_backup_retention.sql");
const FONT_FACE_IDENTITY_SQL: &str = include_str!("../migrations/0004_font_face_identity.sql");

#[derive(Clone)]
pub struct Db {
    pub pool: SqlitePool,
}

async fn add_column_if_missing(
    conn: &mut crate::sqlx::pool::PoolConnection<crate::sqlx::Sqlite>,
    table: &str,
    column: &str,
    definition: &str,
) -> anyhow::Result<()> {
    let pragma = format!("PRAGMA table_info({table})");
    let rows = crate::sqlx::query(&pragma).fetch_all(&mut **conn).await?;
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
            .synchronous(SqliteSynchronous::Normal)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(10))
            .pragma("temp_store", "MEMORY")
            .pragma("cache_size", "-32768")
            .pragma("mmap_size", "268435456");
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
        execute_script(&mut conn, MIGRATION_TABLE_SQL).await?;
        execute_script(&mut conn, BASELINE_SQL).await?;
        let face_identity_index_exists: i64 = crate::sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_font_faces_file_ttc'",
        )
        .fetch_one(&mut *conn)
        .await?;
        add_column_if_missing(&mut conn, "subtitle_files", "analysis", "TEXT").await?;
        add_column_if_missing(&mut conn, "subtitle_files", "analysis_size", "INTEGER").await?;
        add_column_if_missing(&mut conn, "subtitle_files", "analysis_mtime", "INTEGER").await?;
        add_column_if_missing(
            &mut conn,
            "subtitle_files",
            "last_font_index_revision",
            "INTEGER",
        )
        .await?;
        add_column_if_missing(&mut conn, "jobs", "mode", "TEXT NOT NULL DEFAULT 'subset'").await?;
        execute_script(&mut conn, ANALYSIS_INDEXES_SQL).await?;
        execute_script(&mut conn, BACKUP_RETENTION_SQL).await?;
        if face_identity_index_exists == 0 {
            execute_script(&mut conn, FONT_FACE_IDENTITY_SQL).await?;
        }
        record_migration(&mut conn, 1, "baseline schema").await?;
        record_migration(&mut conn, 2, "analysis cache and active-job indexes").await?;
        record_migration(&mut conn, 3, "backup retention index").await?;
        record_migration(
            &mut conn,
            4,
            "deduplicate font faces and enforce face identity",
        )
        .await?;
        Ok(())
    }
}

async fn execute_script(
    conn: &mut crate::sqlx::pool::PoolConnection<crate::sqlx::Sqlite>,
    script: &str,
) -> anyhow::Result<()> {
    for statement in script
        .split(';')
        .map(str::trim)
        .filter(|sql| !sql.is_empty())
    {
        conn.execute(statement).await?;
    }
    Ok(())
}

async fn record_migration(
    conn: &mut crate::sqlx::pool::PoolConnection<crate::sqlx::Sqlite>,
    version: i64,
    description: &str,
) -> anyhow::Result<()> {
    crate::sqlx::query(
        "INSERT OR IGNORE INTO schema_migrations(version, description, applied_at) VALUES(?, ?, ?)",
    )
    .bind(version)
    .bind(description)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(&mut **conn)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn migrations_are_idempotent_and_create_current_indexes() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let db = Db { pool };
        db.migrate().await.unwrap();
        db.migrate().await.unwrap();

        let analysis_column: i64 = crate::sqlx::query_scalar(
            "SELECT COUNT(*) FROM pragma_table_info('subtitle_files') WHERE name='analysis'",
        )
        .fetch_one(&db.pool)
        .await
        .unwrap();
        let active_index: i64 = crate::sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_jobs_one_active_per_subtitle'",
        )
        .fetch_one(&db.pool)
        .await
        .unwrap();
        let face_identity_index: i64 = crate::sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_font_faces_file_ttc'",
        )
        .fetch_one(&db.pool)
        .await
        .unwrap();
        let migrations: i64 = crate::sqlx::query_scalar("SELECT COUNT(*) FROM schema_migrations")
            .fetch_one(&db.pool)
            .await
            .unwrap();
        assert_eq!(analysis_column, 1);
        assert_eq!(active_index, 1);
        assert_eq!(face_identity_index, 1);
        assert_eq!(migrations, 4);
    }

    #[tokio::test]
    async fn migration_repairs_duplicate_font_faces_before_adding_unique_index() {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let db = Db { pool };
        db.migrate().await.unwrap();
        crate::sqlx::query("DROP INDEX idx_font_faces_file_ttc")
            .execute(&db.pool)
            .await
            .unwrap();
        let file_id: i64 = crate::sqlx::query_scalar(
            r#"
INSERT INTO font_files(path, size, mtime, quick_hash, full_hash, format, status, indexed_at)
VALUES('/fonts/example.ttc', 1, 1, '', '', 'ttc', 'ok', 'now')
RETURNING id
"#,
        )
        .fetch_one(&db.pool)
        .await
        .unwrap();
        for _ in 0..2 {
            crate::sqlx::query("INSERT INTO font_faces(file_id, ttc_index) VALUES(?, 0)")
                .bind(file_id)
                .execute(&db.pool)
                .await
                .unwrap();
        }

        db.migrate().await.unwrap();

        let faces: i64 = crate::sqlx::query_scalar(
            "SELECT COUNT(*) FROM font_faces WHERE file_id=? AND ttc_index=0",
        )
        .bind(file_id)
        .fetch_one(&db.pool)
        .await
        .unwrap();
        assert_eq!(faces, 1);
    }
}
