use crate::{
    field_is_allowlisted, filtered_payload_path, load_filter_config,
    normalize_field_path_for_config, normalize_repeated_field_path, runtime_usage_dir,
    usage_db_path, CommandSignatureInput, ConfigRecommendation, ExecMetricIssueInput,
    ExecMetricsInput, FieldAccessContext, FieldAccessRecordInput, RecommendationThresholds,
    RetrieveContext, SessionRecord, SESSION_TICKET_SEQUENCE, USAGE_SCHEMA_VERSION,
};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UsageCleanupReport {
    pub removed_exec_metrics: usize,
    pub removed_exec_metric_issues: usize,
    pub removed_field_access_events: usize,
    pub removed_command_signatures: usize,
}

pub fn record_exec_metrics(
    store_dir: impl AsRef<Path>,
    metrics: &ExecMetricsInput,
) -> io::Result<()> {
    let usage_dir = resolved_usage_dir(store_dir);
    fs::create_dir_all(&usage_dir)?;
    let db_path = usage_db_path(&usage_dir);
    let mut connection = Connection::open(db_path)
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("open usage db: {err}")))?;
    connection
        .pragma_update(None, "foreign_keys", true)
        .map_err(|err| {
            io::Error::new(io::ErrorKind::Other, format!("enable foreign keys: {err}"))
        })?;
    init_usage_schema(&connection)?;
    let active_session = active_session(&connection)?;

    let transaction = connection
        .transaction()
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("start usage tx: {err}")))?;
    let command = metrics.signature.command.as_str();
    let domain = metrics.signature.domain.as_str();
    let details = metrics.signature.details.as_str();

    transaction
        .execute(
            "INSERT OR IGNORE INTO command_signatures (command, domain, details) VALUES (?1, ?2, ?3)",
            params![command, domain, details],
        )
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("insert signature: {err}")))?;

    let signature_id: i64 = transaction
        .query_row(
            "SELECT id FROM command_signatures WHERE command = ?1 AND domain = ?2 AND details = ?3",
            params![command, domain, details],
            |row| row.get(0),
        )
        .map_err(|err| {
            io::Error::new(io::ErrorKind::Other, format!("resolve signature id: {err}"))
        })?;

    let created_at_unix_ms = i64::try_from(metrics.created_at_unix_ms).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "created_at_unix_ms does not fit into sqlite INTEGER",
        )
    })?;
    let original_tokens = i64::try_from(metrics.original_tokens).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "original_tokens does not fit into sqlite INTEGER",
        )
    })?;
    let filtered_tokens = i64::try_from(metrics.filtered_tokens).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "filtered_tokens does not fit into sqlite INTEGER",
        )
    })?;
    let token_delta = original_tokens - filtered_tokens;
    let token_delta_pct = if original_tokens > 0 {
        (token_delta as f64 / original_tokens as f64) * 100.0
    } else {
        0.0
    };
    let session_id = active_session.as_ref().map(|session| session.id);
    let ticket_id = active_session
        .as_ref()
        .map(|session| session.ticket_id.as_str())
        .unwrap_or("");

    transaction
        .execute(
            "INSERT OR REPLACE INTO exec_metrics (ref_id, created_at_unix_ms, signature_id, session_id, ticket_id, config_id, config_path, original_tokens, filtered_tokens, token_delta, token_delta_pct) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                metrics.ref_id.as_str(),
                created_at_unix_ms,
                signature_id,
                session_id,
                ticket_id,
                metrics.config_id.as_str(),
                metrics.config_path.as_str(),
                original_tokens,
                filtered_tokens,
                token_delta,
                token_delta_pct,
            ],
        )
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("insert exec metrics: {err}")))?;

    transaction
        .commit()
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("commit usage tx: {err}")))?;

    Ok(())
}

pub fn record_exec_metric_issue(
    store_dir: impl AsRef<Path>,
    issue: &ExecMetricIssueInput,
) -> io::Result<()> {
    let usage_dir = resolved_usage_dir(store_dir);
    fs::create_dir_all(&usage_dir)?;
    let db_path = usage_db_path(&usage_dir);
    let mut connection = Connection::open(db_path)
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("open usage db: {err}")))?;
    connection
        .pragma_update(None, "foreign_keys", true)
        .map_err(|err| {
            io::Error::new(io::ErrorKind::Other, format!("enable foreign keys: {err}"))
        })?;
    init_usage_schema(&connection)?;
    let active_session = active_session(&connection)?;

    let transaction = connection
        .transaction()
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("start usage tx: {err}")))?;
    let command = issue.signature.command.as_str();
    let domain = issue.signature.domain.as_str();
    let details = issue.signature.details.as_str();

    transaction
        .execute(
            "INSERT OR IGNORE INTO command_signatures (command, domain, details) VALUES (?1, ?2, ?3)",
            params![command, domain, details],
        )
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("insert signature: {err}")))?;

    let signature_id: i64 = transaction
        .query_row(
            "SELECT id FROM command_signatures WHERE command = ?1 AND domain = ?2 AND details = ?3",
            params![command, domain, details],
            |row| row.get(0),
        )
        .map_err(|err| {
            io::Error::new(io::ErrorKind::Other, format!("resolve signature id: {err}"))
        })?;

    let created_at_unix_ms = i64::try_from(issue.created_at_unix_ms).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "created_at_unix_ms does not fit into sqlite INTEGER",
        )
    })?;
    let original_tokens = i64::try_from(issue.original_tokens).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "original_tokens does not fit into sqlite INTEGER",
        )
    })?;
    let filtered_tokens = i64::try_from(issue.filtered_tokens).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "filtered_tokens does not fit into sqlite INTEGER",
        )
    })?;
    let token_delta = original_tokens - filtered_tokens;
    let token_delta_pct = if original_tokens > 0 {
        (token_delta as f64 / original_tokens as f64) * 100.0
    } else {
        0.0
    };
    let session_id = active_session.as_ref().map(|session| session.id);
    let ticket_id = active_session
        .as_ref()
        .map(|session| session.ticket_id.as_str())
        .unwrap_or("");

    transaction
        .execute(
            "INSERT OR REPLACE INTO exec_metric_issues (ref_id, created_at_unix_ms, signature_id, session_id, ticket_id, config_id, config_path, issue_kind, original_tokens, filtered_tokens, token_delta, token_delta_pct) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                issue.ref_id.as_str(),
                created_at_unix_ms,
                signature_id,
                session_id,
                ticket_id,
                issue.config_id.as_str(),
                issue.config_path.as_str(),
                issue.issue_kind.as_str(),
                original_tokens,
                filtered_tokens,
                token_delta,
                token_delta_pct,
            ],
        )
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("insert exec metric issue: {err}")))?;

    transaction
        .commit()
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("commit usage tx: {err}")))?;

    Ok(())
}

pub fn record_field_access(
    store_dir: impl AsRef<Path>,
    access: &FieldAccessRecordInput,
) -> io::Result<()> {
    if access.fields.is_empty() {
        return Ok(());
    }

    let store_dir = store_dir.as_ref();
    let usage_dir = resolved_usage_dir(store_dir);
    fs::create_dir_all(&usage_dir)?;
    let db_path = usage_db_path(&usage_dir);
    let mut connection = Connection::open(db_path)
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("open usage db: {err}")))?;
    connection
        .pragma_update(None, "foreign_keys", true)
        .map_err(|err| {
            io::Error::new(io::ErrorKind::Other, format!("enable foreign keys: {err}"))
        })?;
    init_usage_schema(&connection)?;

    let Some(context) = load_field_access_context(store_dir, &access.ref_id)? else {
        return Ok(());
    };

    let created_at_unix_ms = i64::try_from(access.created_at_unix_ms).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "created_at_unix_ms does not fit into sqlite INTEGER",
        )
    })?;
    let array_index = access
        .array_index
        .map(i64::try_from)
        .transpose()
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "array_index does not fit into sqlite INTEGER",
            )
        })?;
    let all_items = if access.all { 1_i64 } else { 0_i64 };

    let transaction = connection.transaction().map_err(|err| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("start field access tx: {err}"),
        )
    })?;

    let signature_id = match context.signature {
        Some(signature) => Some(resolve_signature_id(
            &transaction,
            &signature.command,
            &signature.domain,
            &signature.details,
        )?),
        None => None,
    };

    for field_path in dedup_field_paths(&access.fields) {
        let normalized_field_path =
            normalize_repeated_field_path(&field_path).unwrap_or(field_path.clone());
        transaction
            .execute(
                "INSERT INTO field_access_events (ref_id, created_at_unix_ms, signature_id, session_id, ticket_id, config_id, config_path, field_path, access_kind, array_index, all_items)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    access.ref_id.as_str(),
                    created_at_unix_ms,
                    signature_id,
                    context.session_id,
                    context.ticket_id.as_str(),
                    context.config_id.as_str(),
                    context.config_path.as_str(),
                    normalized_field_path.as_str(),
                    access.access_kind.as_str(),
                    array_index,
                    all_items,
                ],
            )
            .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("insert field access: {err}")))?;
    }

    transaction.commit().map_err(|err| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("commit field access tx: {err}"),
        )
    })?;

    Ok(())
}

pub fn start_session(
    store_dir: impl AsRef<Path>,
    ticket_id: Option<String>,
) -> io::Result<SessionRecord> {
    let usage_dir = resolved_usage_dir(store_dir);
    fs::create_dir_all(&usage_dir)?;
    let db_path = usage_db_path(&usage_dir);
    let connection = Connection::open(db_path)
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("open usage db: {err}")))?;
    connection
        .pragma_update(None, "foreign_keys", true)
        .map_err(|err| {
            io::Error::new(io::ErrorKind::Other, format!("enable foreign keys: {err}"))
        })?;
    init_usage_schema(&connection)?;

    if let Some(active) = active_session(&connection)? {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("session already active for ticketId {}", active.ticket_id),
        ));
    }

    let ticket_id = match ticket_id {
        Some(value) => {
            let value = value.trim();
            if value.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "ticketId cannot be empty",
                ));
            }
            value.to_string()
        }
        None => generate_session_ticket_id(),
    };

    let started_at_unix_ms = i64::try_from(now_unix_ms()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "started_at_unix_ms does not fit into sqlite INTEGER",
        )
    })?;

    connection
        .execute(
            "INSERT INTO sessions (ticket_id, started_at_unix_ms) VALUES (?1, ?2)",
            params![ticket_id.as_str(), started_at_unix_ms],
        )
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("start session: {err}")))?;

    let id = connection.last_insert_rowid();
    Ok(SessionRecord {
        id,
        ticket_id,
        started_at_unix_ms,
        ended_at_unix_ms: None,
    })
}

pub fn end_session(store_dir: impl AsRef<Path>) -> io::Result<SessionRecord> {
    let usage_dir = resolved_usage_dir(store_dir);
    let db_path = usage_db_path(&usage_dir);
    let connection = Connection::open(db_path)
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("open usage db: {err}")))?;
    connection
        .pragma_update(None, "foreign_keys", true)
        .map_err(|err| {
            io::Error::new(io::ErrorKind::Other, format!("enable foreign keys: {err}"))
        })?;
    init_usage_schema(&connection)?;

    let Some(active) = active_session(&connection)? else {
        return Err(io::Error::new(io::ErrorKind::NotFound, "no active session"));
    };

    let ended_at_unix_ms = i64::try_from(now_unix_ms()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "ended_at_unix_ms does not fit into sqlite INTEGER",
        )
    })?;

    connection
        .execute(
            "UPDATE sessions SET ended_at_unix_ms = ?1 WHERE id = ?2",
            params![ended_at_unix_ms, active.id],
        )
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("end session: {err}")))?;

    Ok(SessionRecord {
        ended_at_unix_ms: Some(ended_at_unix_ms),
        ..active
    })
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

pub fn load_config_recommendations(
    store_dir: impl AsRef<Path>,
    thresholds: RecommendationThresholds,
) -> io::Result<Vec<ConfigRecommendation>> {
    let usage_dir = resolved_usage_dir(store_dir);
    fs::create_dir_all(&usage_dir)?;
    let db_path = usage_db_path(&usage_dir);
    let connection = Connection::open(db_path)
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("open usage db: {err}")))?;
    init_usage_schema(&connection)?;

    let mut recommendations = Vec::new();
    recommendations.extend(load_expand_recommendations(&connection, thresholds)?);
    recommendations.extend(load_fallback_recommendations(&connection, thresholds)?);
    recommendations.sort_by(|left, right| {
        right
            .event_count
            .cmp(&left.event_count)
            .then_with(|| left.recommendation_kind.cmp(&right.recommendation_kind))
            .then_with(|| left.config_id.cmp(&right.config_id))
    });
    Ok(recommendations)
}

pub fn recommendation_notices_for_retrieve(
    store_dir: impl AsRef<Path>,
    ref_id: &str,
    fields: &[String],
) -> io::Result<Vec<String>> {
    let Some(context) = load_field_access_context(store_dir.as_ref(), ref_id)? else {
        return Ok(Vec::new());
    };

    let config = load_filter_config(&context.config_path).ok();
    let requested = dedup_field_paths(fields)
        .into_iter()
        .filter_map(|field| {
            let normalized = normalize_repeated_field_path(&field).unwrap_or(field);
            match config.as_ref() {
                Some(config) => normalize_field_path_for_config(&normalized, config),
                None => Some(normalized),
            }
        })
        .collect::<Vec<_>>();
    if requested.is_empty() {
        return Ok(Vec::new());
    }

    let recommendations =
        load_config_recommendations(store_dir, RecommendationThresholds::default())?;
    let mut notices = Vec::new();
    for recommendation in recommendations {
        if recommendation.recommendation_kind != "expand_allowlist" {
            continue;
        }
        if recommendation.config_id != context.config_id {
            continue;
        }
        let Some(field_path) = recommendation.field_path.as_deref() else {
            continue;
        };
        if !requested.iter().any(|field| field == field_path) {
            continue;
        }
        notices.push(format!(
            "DTK recommendation: ask the user whether to add `{field_path}` to config `{}`. If they agree, run `dtk config list` to confirm the target config id, then `dtk config allow add <config> <field>`. This field has been requested repeatedly for the same endpoint.",
            recommendation.config_id
        ));
    }

    notices.sort();
    notices.dedup();
    Ok(notices)
}

pub fn recommendation_notices_for_exec(
    store_dir: impl AsRef<Path>,
    config_id: &str,
    details: &str,
) -> io::Result<Vec<String>> {
    let recommendations =
        load_config_recommendations(store_dir, RecommendationThresholds::default())?;
    let mut notices = Vec::new();
    for recommendation in recommendations {
        if recommendation.config_id != config_id || recommendation.details != details {
            continue;
        }
        match recommendation.recommendation_kind.as_str() {
            "tighten_allowlist" => notices.push(format!(
                "DTK recommendation: ask the user whether to tighten config `{config_id}`. If they agree, run `dtk config list` to confirm the target config id, then use `dtk config allow add/remove <config> <field>` to tighten the config. DTK is falling back repeatedly for this endpoint."
            )),
            "remove_config" => notices.push(format!(
                "DTK recommendation: ask the user whether to remove or disable config `{config_id}` for this endpoint. If they agree, run `dtk config list` to confirm the target config id, then `dtk config delete <config>`. DTK is falling back repeatedly and may not be suitable here."
            )),
            _ => {}
        }
    }

    notices.sort();
    notices.dedup();
    Ok(notices)
}

fn usage_retention_cutoff_unix_ms() -> u128 {
    let retention_days = std::env::var("DTK_USAGE_RETENTION_DAYS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(crate::DEFAULT_USAGE_RETENTION_DAYS);
    let retention_ms = u128::from(retention_days) * 24 * 60 * 60 * 1000;
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    now.saturating_sub(retention_ms)
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
            params![cutoff],
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
            params![cutoff],
        )
        .map_err(|err| {
            io::Error::new(io::ErrorKind::Other, format!("cleanup usage issues: {err}"))
        })?;
    let removed_exec_metrics = connection
        .execute(
            "DELETE FROM exec_metrics WHERE created_at_unix_ms < ?1",
            params![cutoff],
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

fn resolved_usage_dir(preferred_store_dir: impl AsRef<Path>) -> PathBuf {
    if std::env::var("DTK_USAGE_DIR").is_ok() {
        return runtime_usage_dir();
    }

    let preferred = preferred_store_dir.as_ref();
    if crate::paths::dir_is_writable(preferred) {
        preferred.to_path_buf()
    } else {
        runtime_usage_dir()
    }
}

fn resolve_signature_id(
    connection: &Connection,
    command: &str,
    domain: &str,
    details: &str,
) -> io::Result<i64> {
    connection
        .execute(
            "INSERT OR IGNORE INTO command_signatures (command, domain, details) VALUES (?1, ?2, ?3)",
            params![command, domain, details],
        )
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("insert signature: {err}")))?;

    connection
        .query_row(
            "SELECT id FROM command_signatures WHERE command = ?1 AND domain = ?2 AND details = ?3",
            params![command, domain, details],
            |row| row.get(0),
        )
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("resolve signature id: {err}")))
}

fn load_field_access_context_from_usage_dir(
    usage_dir: &Path,
    ref_id: &str,
) -> io::Result<Option<FieldAccessContext>> {
    let db_path = usage_db_path(usage_dir);
    if !db_path.exists() {
        return Ok(None);
    }

    let connection = Connection::open(&db_path)
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("open usage db: {err}")))?;

    connection
        .query_row(
            "SELECT cs.command, cs.domain, cs.details, em.session_id, em.ticket_id, em.config_id, em.config_path
             FROM exec_metrics em
             JOIN command_signatures cs ON cs.id = em.signature_id
             WHERE em.ref_id = ?1",
            params![ref_id],
            |row| {
                Ok(FieldAccessContext {
                    signature: Some(CommandSignatureInput {
                        command: row.get(0)?,
                        domain: row.get(1)?,
                        details: row.get(2)?,
                    }),
                    session_id: row.get(3)?,
                    ticket_id: row.get(4)?,
                    config_id: row.get(5)?,
                    config_path: row.get(6)?,
                })
            },
        )
        .optional()
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("load field access context: {err}")))
}

fn load_field_access_context(
    preferred_store_dir: &Path,
    ref_id: &str,
) -> io::Result<Option<FieldAccessContext>> {
    if let Some(context) = load_field_access_context_from_usage_dir(preferred_store_dir, ref_id)? {
        return Ok(Some(context));
    }

    let fallback = resolved_usage_dir(preferred_store_dir);
    if fallback != preferred_store_dir {
        if let Some(context) = load_field_access_context_from_usage_dir(&fallback, ref_id)? {
            return Ok(Some(context));
        }
    }

    if let Some(retrieve_context) =
        load_retrieve_context_from_filtered_payload(preferred_store_dir, ref_id)?
    {
        return Ok(Some(FieldAccessContext {
            signature: None,
            session_id: None,
            ticket_id: String::new(),
            config_id: retrieve_context.config_id,
            config_path: retrieve_context.config_path,
        }));
    }

    Ok(None)
}

fn load_retrieve_context_from_filtered_payload(
    store_dir: &Path,
    ref_id: &str,
) -> io::Result<Option<RetrieveContext>> {
    let filtered_path = filtered_payload_path(store_dir, ref_id);
    if !filtered_path.exists() {
        return Ok(None);
    }

    let content = fs::read_to_string(&filtered_path)?;
    let value: Value = serde_json::from_str(&content).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("parse filtered payload: {err}"),
        )
    })?;

    let Some(metadata) = value.get("_dtk").and_then(Value::as_object) else {
        return Ok(None);
    };
    let Some(config_id) = metadata.get("config_id").and_then(Value::as_str) else {
        return Ok(None);
    };

    Ok(Some(RetrieveContext {
        config_id: config_id.to_string(),
        config_path: filtered_path.to_string_lossy().to_string(),
    }))
}

fn dedup_field_paths(fields: &[String]) -> Vec<String> {
    let mut deduped = Vec::new();
    for field in fields {
        let trimmed = field.trim();
        if trimmed.is_empty() {
            continue;
        }
        if deduped.iter().any(|existing: &String| existing == trimmed) {
            continue;
        }
        deduped.push(trimmed.to_string());
    }
    deduped
}

fn load_expand_recommendations(
    connection: &Connection,
    thresholds: RecommendationThresholds,
) -> io::Result<Vec<ConfigRecommendation>> {
    let mut statement = connection
        .prepare(
            "SELECT fa.config_id, fa.config_path, cs.command, cs.domain, cs.details, fa.field_path, COUNT(*) as access_count
             FROM field_access_events fa
             LEFT JOIN command_signatures cs ON cs.id = fa.signature_id
             WHERE fa.config_id != ''
             GROUP BY fa.config_id, fa.config_path, fa.signature_id, fa.field_path
             HAVING COUNT(*) >= ?1
             ORDER BY access_count DESC",
        )
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("prepare expand recommendations: {err}")))?;

    let rows = statement
        .query_map(params![thresholds.expand_field_access_count], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                row.get::<_, Option<String>>(4)?.unwrap_or_default(),
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
            ))
        })
        .map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("query expand recommendations: {err}"),
            )
        })?;

    let mut recommendations = Vec::new();
    for row in rows {
        let (config_id, config_path, command, domain, details, field_path, access_count) = row
            .map_err(|err| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("read expand recommendation: {err}"),
                )
            })?;
        let config = load_filter_config(&config_path).ok();
        if config
            .as_ref()
            .is_some_and(|config| field_is_allowlisted(config, &field_path))
        {
            continue;
        }
        let display_field_path = config
            .as_ref()
            .and_then(|config| normalize_field_path_for_config(&field_path, config))
            .unwrap_or_else(|| field_path.to_string());

        recommendations.push(ConfigRecommendation {
            config_id: config_id.clone(),
            config_path: config_path.clone(),
            command,
            domain,
            details,
            recommendation_kind: "expand_allowlist".to_string(),
            field_path: Some(display_field_path.clone()),
            event_count: access_count,
            summary: format!(
                "Field `{display_field_path}` was retrieved {access_count} times for config `{config_id}` and may belong in the allowlist."
            ),
        });
    }

    Ok(recommendations)
}

fn load_fallback_recommendations(
    connection: &Connection,
    thresholds: RecommendationThresholds,
) -> io::Result<Vec<ConfigRecommendation>> {
    let mut statement = connection
        .prepare(
            "SELECT emi.config_id, emi.config_path, cs.command, cs.domain, cs.details, COUNT(*) as issue_count
             FROM exec_metric_issues emi
             LEFT JOIN command_signatures cs ON cs.id = emi.signature_id
             WHERE emi.config_id != '' AND emi.issue_kind = 'filtered_larger_than_original'
             GROUP BY emi.config_id, emi.config_path, emi.signature_id
             HAVING COUNT(*) >= ?1
             ORDER BY issue_count DESC",
        )
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("prepare fallback recommendations: {err}")))?;

    let rows = statement
        .query_map(params![thresholds.tighten_fallback_count], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                row.get::<_, Option<String>>(4)?.unwrap_or_default(),
                row.get::<_, i64>(5)?,
            ))
        })
        .map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("query fallback recommendations: {err}"),
            )
        })?;

    let mut recommendations = Vec::new();
    for row in rows {
        let (config_id, config_path, command, domain, details, issue_count) =
            row.map_err(|err| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("read fallback recommendation: {err}"),
                )
            })?;
        let allow_count = load_filter_config(&config_path)
            .map(|config| config.allow.len())
            .unwrap_or(0);
        let recommendation_kind = if issue_count >= thresholds.remove_fallback_count {
            "remove_config"
        } else if allow_count >= thresholds.tighten_allow_count_min {
            "tighten_allowlist"
        } else {
            "remove_config"
        };
        let summary = if recommendation_kind == "tighten_allowlist" {
            format!(
                "Config `{config_id}` fell back {issue_count} times and exposes {allow_count} allowlist paths; consider shrinking it for this command signature."
            )
        } else {
            format!(
                "Config `{config_id}` fell back {issue_count} times for the same command signature and may not be suitable for this endpoint."
            )
        };

        recommendations.push(ConfigRecommendation {
            config_id,
            config_path,
            command,
            domain,
            details,
            recommendation_kind: recommendation_kind.to_string(),
            field_path: None,
            event_count: issue_count,
            summary,
        });
    }

    Ok(recommendations)
}

fn active_session(connection: &Connection) -> io::Result<Option<SessionRecord>> {
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

fn generate_session_ticket_id() -> String {
    let sequence = SESSION_TICKET_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("dtk-sess-{:x}-{sequence}", now_unix_ms())
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}
