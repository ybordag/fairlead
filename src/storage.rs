use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

#[cfg(test)]
use crate::config::JobStoreConfig;
use crate::jobs::{JobQueues, JobRecord, JobRegistryInner, JobStatus};

const JOB_STORE_SCHEMA_VERSION: i32 = 4;

#[cfg(test)]
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

    pub(crate) fn load_registry_snapshot(&self) -> Result<JobRegistryInner> {
        let connection = Connection::open(&self.path)
            .with_context(|| format!("open SQLite job store at {}", self.path.display()))?;
        bootstrap_schema(&connection)
            .with_context(|| format!("bootstrap SQLite job store at {}", self.path.display()))?;

        let mut statement = connection.prepare(
            r#"
            SELECT id, type, priority, status, payload_json, callback_url, result_json,
                   error_json, attempts, max_attempts, lease_json, created_at_unix_ms,
                   updated_at_unix_ms, queue_position, callback_state_json, idempotency_key,
                   terminal_attempt_json
            FROM jobs
            ORDER BY order_position ASC, created_at_unix_ms ASC, id ASC
            "#,
        )?;

        let mut jobs = HashMap::new();
        let mut order = Vec::new();
        let mut queued = Vec::new();
        let mut next_id = 0;

        let rows = statement.query_map([], |row| {
            let id: String = row.get(0)?;
            let kind_json: String = row.get(1)?;
            let priority_json: String = row.get(2)?;
            let status_json: String = row.get(3)?;
            let payload_json: String = row.get(4)?;
            let result_json: Option<String> = row.get(6)?;
            let error_json: Option<String> = row.get(7)?;
            let lease_json: Option<String> = row.get(10)?;
            let created_at_unix_ms: i64 = row.get(11)?;
            let updated_at_unix_ms: i64 = row.get(12)?;
            let queue_position: Option<i64> = row.get(13)?;
            let callback_state_json: Option<String> = row.get(14)?;
            let idempotency_key: Option<String> = row.get(15)?;
            let terminal_attempt_json: Option<String> = row.get(16)?;

            let job = JobRecord {
                id,
                kind: serde_json::from_str(&format!("\"{kind_json}\"")).map_err(to_sql_error)?,
                priority: serde_json::from_str(&format!("\"{priority_json}\""))
                    .map_err(to_sql_error)?,
                status: serde_json::from_str(&format!("\"{status_json}\""))
                    .map_err(to_sql_error)?,
                payload: serde_json::from_str(&payload_json).map_err(to_sql_error)?,
                callback_url: row.get(5)?,
                idempotency_key,
                result: result_json
                    .map(|raw| serde_json::from_str(&raw).map_err(to_sql_error))
                    .transpose()?,
                error: error_json
                    .map(|raw| serde_json::from_str(&raw).map_err(to_sql_error))
                    .transpose()?,
                callback: callback_state_json
                    .map(|raw| serde_json::from_str(&raw).map_err(to_sql_error))
                    .transpose()?,
                attempts: row.get::<_, i64>(8)? as u32,
                max_attempts: row.get::<_, i64>(9)? as u32,
                lease: lease_json
                    .map(|raw| serde_json::from_str(&raw).map_err(to_sql_error))
                    .transpose()?,
                terminal_attempt: terminal_attempt_json
                    .map(|raw| serde_json::from_str(&raw).map_err(to_sql_error))
                    .transpose()?,
                created_at_unix_ms: created_at_unix_ms as u128,
                updated_at_unix_ms: updated_at_unix_ms as u128,
            };

            Ok((job, queue_position))
        })?;

        for row in rows {
            let (job, queue_position) = row?;
            next_id = next_id.max(parse_numeric_job_id(&job.id));
            if job.status == JobStatus::Queued {
                queued.push((
                    job.priority,
                    queue_position.unwrap_or(i64::MAX),
                    job.id.clone(),
                ));
            }
            order.push(job.id.clone());
            jobs.insert(job.id.clone(), job);
        }

        let mut queues = JobQueues::default();
        queued.sort_by_key(|(priority, queue_position, id)| {
            (priority_rank(*priority), *queue_position, id.clone())
        });
        for (priority, _queue_position, id) in queued {
            match priority {
                crate::config::Priority::Realtime => queues.realtime.push_back(id),
                crate::config::Priority::Batch => queues.batch.push_back(id),
                crate::config::Priority::Background => queues.background.push_back(id),
            }
        }
        let idempotency_keys = jobs
            .values()
            .filter_map(|job| {
                job.idempotency_key
                    .as_ref()
                    .map(|key| (key.clone(), job.id.clone()))
            })
            .collect();

        Ok(JobRegistryInner {
            next_id,
            jobs,
            idempotency_keys,
            order,
            queues,
        })
    }

    pub(crate) fn replace_registry_snapshot(&self, inner: &JobRegistryInner) -> Result<()> {
        let mut connection = Connection::open(&self.path)
            .with_context(|| format!("open SQLite job store at {}", self.path.display()))?;
        bootstrap_schema(&connection)
            .with_context(|| format!("bootstrap SQLite job store at {}", self.path.display()))?;
        let transaction = connection.transaction()?;
        transaction.execute("DELETE FROM jobs", [])?;

        let queue_positions = queue_positions(&inner.queues);
        for (order_position, id) in inner.order.iter().enumerate() {
            let Some(job) = inner.jobs.get(id) else {
                continue;
            };
            transaction.execute(
                r#"
                INSERT INTO jobs (
                    id, type, priority, status, payload_json, callback_url, result_json,
                    error_json, attempts, max_attempts, lease_json, created_at_unix_ms,
                    updated_at_unix_ms, queue_position, order_position, callback_state_json,
                    idempotency_key, terminal_attempt_json
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)
                "#,
                params![
                    job.id,
                    job.kind.as_str(),
                    job.priority.as_str(),
                    job.status.as_str(),
                    serde_json::to_string(&job.payload)?,
                    job.callback_url,
                    optional_json(&job.result)?,
                    optional_json(&job.error)?,
                    i64::from(job.attempts),
                    i64::from(job.max_attempts),
                    optional_json(&job.lease)?,
                    job.created_at_unix_ms as i64,
                    job.updated_at_unix_ms as i64,
                    queue_positions.get(&job.id).copied(),
                    order_position as i64,
                    optional_json(&job.callback)?,
                    job.idempotency_key,
                    optional_json(&job.terminal_attempt)?,
                ],
            )?;
        }

        transaction.commit()?;
        Ok(())
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
            queue_position INTEGER,
            order_position INTEGER NOT NULL DEFAULT 0,
            callback_state_json TEXT,
            idempotency_key TEXT,
            terminal_attempt_json TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_jobs_queue
            ON jobs(status, priority, queue_position, created_at_unix_ms);

        CREATE INDEX IF NOT EXISTS idx_jobs_status
            ON jobs(status);
        "#,
    )?;
    ensure_column(
        connection,
        "jobs",
        "order_position",
        "ALTER TABLE jobs ADD COLUMN order_position INTEGER NOT NULL DEFAULT 0",
    )?;
    ensure_column(
        connection,
        "jobs",
        "callback_state_json",
        "ALTER TABLE jobs ADD COLUMN callback_state_json TEXT",
    )?;
    ensure_column(
        connection,
        "jobs",
        "idempotency_key",
        "ALTER TABLE jobs ADD COLUMN idempotency_key TEXT",
    )?;
    ensure_column(
        connection,
        "jobs",
        "terminal_attempt_json",
        "ALTER TABLE jobs ADD COLUMN terminal_attempt_json TEXT",
    )?;
    connection.pragma_update(None, "user_version", JOB_STORE_SCHEMA_VERSION)?;
    Ok(())
}

fn ensure_column(
    connection: &Connection,
    table: &str,
    column: &str,
    alter_statement: &str,
) -> Result<()> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info({table})"))?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
    for existing in columns {
        if existing? == column {
            return Ok(());
        }
    }

    connection.execute(alter_statement, [])?;
    Ok(())
}

fn queue_positions(queues: &JobQueues) -> HashMap<String, i64> {
    let mut positions = HashMap::new();
    for queue in [&queues.realtime, &queues.batch, &queues.background] {
        for (position, id) in queue.iter().enumerate() {
            positions.insert(id.clone(), position as i64);
        }
    }
    positions
}

fn optional_json<T: serde::Serialize>(value: &Option<T>) -> Result<Option<String>> {
    value
        .as_ref()
        .map(serde_json::to_string)
        .transpose()
        .map_err(Into::into)
}

fn parse_numeric_job_id(id: &str) -> u64 {
    id.strip_prefix("job-")
        .and_then(|suffix| suffix.parse().ok())
        .unwrap_or_default()
}

fn priority_rank(priority: crate::config::Priority) -> u8 {
    match priority {
        crate::config::Priority::Realtime => 0,
        crate::config::Priority::Batch => 1,
        crate::config::Priority::Background => 2,
    }
}

fn to_sql_error(error: serde_json::Error) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
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
    fn sqlite_bootstrap_adds_missing_columns_to_existing_schema() {
        let path = unique_db_path("migrate");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                r#"
                CREATE TABLE jobs (
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
                "#,
            )
            .unwrap();
        drop(connection);

        SqliteJobStore::open(&path).unwrap();

        let connection = Connection::open(&path).unwrap();
        let columns: Vec<String> = connection
            .prepare("PRAGMA table_info(jobs)")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(1))
            .unwrap()
            .map(|column| column.unwrap())
            .collect();
        assert!(columns.contains(&"order_position".into()));
        assert!(columns.contains(&"callback_state_json".into()));
        assert!(columns.contains(&"idempotency_key".into()));
        assert!(columns.contains(&"terminal_attempt_json".into()));
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
