use anyhow::{Context, Result};
use rusqlite::Connection;
use std::path::{Path, PathBuf};

use crate::config::JobStoreConfig;

const JOB_STORE_SCHEMA_VERSION: i32 = 1;

pub fn bootstrap_job_store(config: &JobStoreConfig) -> Result<()> {
    match config {
        JobStoreConfig::Memory => Ok(()),
        JobStoreConfig::Sqlite { path } => {
            SqliteJobStore::open(path)?;
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteJobStore {
    path: PathBuf,
}

impl SqliteJobStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let connection = Connection::open(path)
            .with_context(|| format!("open SQLite job store at {}", path.display()))?;
        bootstrap_schema(&connection)
            .with_context(|| format!("bootstrap SQLite job store at {}", path.display()))?;
        Ok(Self {
            path: path.to_path_buf(),
        })
    }
}

fn bootstrap_schema(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        r#"
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            type TEXT NOT NULL,
            priority TEXT NOT NULL,
            status TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            callback_url TEXT,
            result_json TEXT,
            error_json TEXT,
            attempts INTEGER NOT NULL,
            max_attempts INTEGER NOT NULL,
            lease_json TEXT,
            created_at_unix_ms INTEGER NOT NULL,
            updated_at_unix_ms INTEGER NOT NULL,
            queue_position INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_jobs_queue
            ON jobs(status, priority, queue_position, created_at_unix_ms);

        CREATE INDEX IF NOT EXISTS idx_jobs_status
            ON jobs(status);
        "#,
    )?;
    connection.pragma_update(None, "user_version", JOB_STORE_SCHEMA_VERSION)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::OptionalExtension;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn bootstrap_memory_store_is_noop() {
        bootstrap_job_store(&JobStoreConfig::Memory).unwrap();
    }

    #[test]
    fn sqlite_store_bootstraps_schema() {
        let path = unique_db_path("schema");
        let store = SqliteJobStore::open(&path).unwrap();
        assert_eq!(store.path, path);

        let connection = Connection::open(&path).unwrap();
        let user_version: i32 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        let jobs_table: Option<String> = connection
            .query_row(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'jobs'",
                [],
                |row| row.get(0),
            )
            .optional()
            .unwrap();
        let queue_index: Option<String> = connection
            .query_row(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND name = 'idx_jobs_queue'",
                [],
                |row| row.get(0),
            )
            .optional()
            .unwrap();

        assert_eq!(user_version, JOB_STORE_SCHEMA_VERSION);
        assert_eq!(jobs_table.as_deref(), Some("jobs"));
        assert_eq!(queue_index.as_deref(), Some("idx_jobs_queue"));
    }

    #[test]
    fn sqlite_bootstrap_is_idempotent() {
        let path = unique_db_path("idempotent");
        SqliteJobStore::open(&path).unwrap();
        SqliteJobStore::open(&path).unwrap();

        let connection = Connection::open(&path).unwrap();
        let count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'jobs'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn bootstrap_job_store_opens_sqlite_path() {
        let path = unique_db_path("config");
        bootstrap_job_store(&JobStoreConfig::Sqlite {
            path: path.to_string_lossy().into_owned(),
        })
        .unwrap();
        assert!(path.exists());
    }

    fn unique_db_path(prefix: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("fairlead-{prefix}-{unique}.sqlite3"))
    }
}
