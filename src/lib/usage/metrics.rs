use super::context::{load_field_access_context, resolve_signature_id, resolved_usage_dir};
use super::recommendations::dedup_field_paths;
use super::schema::{active_session, init_usage_schema};
use crate::{
    normalize_repeated_field_path, usage_db_path, ExecMetricIssueInput, ExecMetricsInput,
    FieldAccessRecordInput,
};
use rusqlite::{params, Connection};
use std::fs;
use std::io;

pub fn record_exec_metrics(
    store_dir: impl AsRef<std::path::Path>,
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
    store_dir: impl AsRef<std::path::Path>,
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
    store_dir: impl AsRef<std::path::Path>,
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
