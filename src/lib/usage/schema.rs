use crate::{SessionRecord, USAGE_SCHEMA_VERSION};
use rusqlite::Connection;
use std::io;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UsageCleanupReport {
    pub removed_exec_metrics: usize,
    pub removed_exec_metric_issues: usize,
    pub removed_field_access_events: usize,
    pub removed_command_signatures: usize,
}

pub fn init_usage_schema(connection: &Connection) -> io::Result<()> {
    if usage_schema_version(connection)? != USAGE_SCHEMA_VERSION {
        reset_usage_schema(connection)?;
    }

    connection
        .execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS command_signatures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                command TEXT NOT NULL,
                domain TEXT NOT NULL DEFAULT '',
                details TEXT NOT NULL,
                UNIQUE(command, domain, details)
            );

            CREATE TABLE IF NOT EXISTS exec_metrics (
                ref_id TEXT PRIMARY KEY,
                created_at_unix_ms INTEGER NOT NULL,
                signature_id INTEGER NOT NULL,
                session_id INTEGER,
                ticket_id TEXT NOT NULL DEFAULT '',
                config_id TEXT NOT NULL DEFAULT '',
                config_path TEXT NOT NULL DEFAULT '',
                original_tokens INTEGER NOT NULL,
                filtered_tokens INTEGER NOT NULL,
                token_delta INTEGER NOT NULL,
                token_delta_pct REAL NOT NULL,
                FOREIGN KEY(signature_id) REFERENCES command_signatures(id)
            );

            CREATE TABLE IF NOT EXISTS exec_metric_issues (
                ref_id TEXT PRIMARY KEY,
                created_at_unix_ms INTEGER NOT NULL,
                signature_id INTEGER NOT NULL,
                session_id INTEGER,
                ticket_id TEXT NOT NULL DEFAULT '',
                config_id TEXT NOT NULL DEFAULT '',
                config_path TEXT NOT NULL DEFAULT '',
                issue_kind TEXT NOT NULL,
                original_tokens INTEGER NOT NULL,
                filtered_tokens INTEGER NOT NULL,
                token_delta INTEGER NOT NULL,
                token_delta_pct REAL NOT NULL,
                FOREIGN KEY(signature_id) REFERENCES command_signatures(id)
            );

            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id TEXT NOT NULL,
                started_at_unix_ms INTEGER NOT NULL,
                ended_at_unix_ms INTEGER
            );

            CREATE TABLE IF NOT EXISTS field_access_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ref_id TEXT NOT NULL,
                created_at_unix_ms INTEGER NOT NULL,
                signature_id INTEGER,
                session_id INTEGER,
                ticket_id TEXT NOT NULL DEFAULT '',
                config_id TEXT NOT NULL DEFAULT '',
                config_path TEXT NOT NULL DEFAULT '',
                field_path TEXT NOT NULL,
                access_kind TEXT NOT NULL DEFAULT 'retrieve',
                array_index INTEGER,
                all_items INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(signature_id) REFERENCES command_signatures(id)
            );

            CREATE INDEX IF NOT EXISTS idx_exec_metrics_created_at_unix_ms
                ON exec_metrics(created_at_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_exec_metrics_signature_id
                ON exec_metrics(signature_id);
            CREATE INDEX IF NOT EXISTS idx_exec_metrics_config_id
                ON exec_metrics(config_id);
            CREATE INDEX IF NOT EXISTS idx_exec_metric_issues_created_at_unix_ms
                ON exec_metric_issues(created_at_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_exec_metric_issues_signature_id
                ON exec_metric_issues(signature_id);
            CREATE INDEX IF NOT EXISTS idx_exec_metric_issues_config_id
                ON exec_metric_issues(config_id);
            CREATE INDEX IF NOT EXISTS idx_field_access_events_created_at_unix_ms
                ON field_access_events(created_at_unix_ms);
            CREATE INDEX IF NOT EXISTS idx_field_access_events_signature_id
                ON field_access_events(signature_id);
            CREATE INDEX IF NOT EXISTS idx_field_access_events_config_id
                ON field_access_events(config_id);
            CREATE INDEX IF NOT EXISTS idx_field_access_events_field_path
                ON field_access_events(field_path);
            CREATE INDEX IF NOT EXISTS idx_sessions_active
                ON sessions(ended_at_unix_ms, started_at_unix_ms);
            "#,
        )
        .map_err(|err| {
            io::Error::new(io::ErrorKind::Other, format!("create usage schema: {err}"))
        })?;

    connection
        .pragma_update(None, "user_version", USAGE_SCHEMA_VERSION)
        .map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("set usage schema version: {err}"),
            )
        })?;

    cleanup_usage_records(connection)?;

    Ok(())
}

pub(super) fn active_session(connection: &Connection) -> io::Result<Option<SessionRecord>> {
    let mut statement = connection
        .prepare(
            "SELECT id, ticket_id, started_at_unix_ms, ended_at_unix_ms
             FROM sessions
             WHERE ended_at_unix_ms IS NULL
             ORDER BY started_at_unix_ms DESC, id DESC
             LIMIT 1",
        )
        .map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("prepare session query: {err}"),
            )
        })?;

    let result = statement.query_row([], |row| {
        Ok(SessionRecord {
            id: row.get(0)?,
            ticket_id: row.get(1)?,
            started_at_unix_ms: row.get(2)?,
            ended_at_unix_ms: row.get(3)?,
        })
    });

    match result {
        Ok(session) => Ok(Some(session)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(err) => Err(io::Error::new(
            io::ErrorKind::Other,
            format!("read session: {err}"),
        )),
    }
}

pub(super) fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

fn usage_retention_cutoff_unix_ms() -> u128 {
    let retention_days = std::env::var("DTK_USAGE_RETENTION_DAYS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(crate::DEFAULT_USAGE_RETENTION_DAYS);
    let retention_ms = u128::from(retention_days) * 24 * 60 * 60 * 1000;
    now_unix_ms().saturating_sub(retention_ms)
}

fn cleanup_usage_records(connection: &Connection) -> io::Result<UsageCleanupReport> {
    let cutoff = i64::try_from(usage_retention_cutoff_unix_ms()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "usage retention cutoff does not fit into sqlite INTEGER",
        )
    })?;

    let removed_field_access_events = connection
        .execute(
            "DELETE FROM field_access_events WHERE created_at_unix_ms < ?1",
            rusqlite::params![cutoff],
        )
        .map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("cleanup usage field access: {err}"),
            )
        })?;
    let removed_exec_metric_issues = connection
        .execute(
            "DELETE FROM exec_metric_issues WHERE created_at_unix_ms < ?1",
            rusqlite::params![cutoff],
        )
        .map_err(|err| {
            io::Error::new(io::ErrorKind::Other, format!("cleanup usage issues: {err}"))
        })?;
    let removed_exec_metrics = connection
        .execute(
            "DELETE FROM exec_metrics WHERE created_at_unix_ms < ?1",
            rusqlite::params![cutoff],
        )
        .map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("cleanup usage metrics: {err}"),
            )
        })?;

    connection
        .execute(
            "DELETE FROM command_signatures
             WHERE id NOT IN (
                SELECT signature_id FROM exec_metrics
                UNION
                SELECT signature_id FROM exec_metric_issues
                UNION
                SELECT signature_id FROM field_access_events
             )",
            [],
        )
        .map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("cleanup usage signatures: {err}"),
            )
        })?;

    let removed_command_signatures = connection.changes() as usize;

    Ok(UsageCleanupReport {
        removed_exec_metrics,
        removed_exec_metric_issues,
        removed_field_access_events,
        removed_command_signatures,
    })
}

fn usage_schema_version(connection: &Connection) -> io::Result<i32> {
    connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("read usage schema version: {err}"),
            )
        })
}

fn reset_usage_schema(connection: &Connection) -> io::Result<()> {
    connection
        .execute_batch(
            r#"
            DROP TABLE IF EXISTS field_access_events;
            DROP TABLE IF EXISTS exec_metric_issues;
            DROP TABLE IF EXISTS exec_metrics;
            DROP TABLE IF EXISTS sessions;
            DROP TABLE IF EXISTS command_signatures;
            PRAGMA user_version = 0;
            "#,
        )
        .map_err(|err| {
            io::Error::new(io::ErrorKind::Other, format!("reset usage schema: {err}"))
        })?;
    Ok(())
}
