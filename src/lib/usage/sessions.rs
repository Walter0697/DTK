use super::context::resolved_usage_dir;
use super::schema::{active_session, init_usage_schema, now_unix_ms};
use crate::{usage_db_path, SessionRecord, SESSION_TICKET_SEQUENCE};
use rusqlite::{params, Connection};
use std::fs;
use std::io;
use std::sync::atomic::Ordering;

pub fn start_session(
    store_dir: impl AsRef<std::path::Path>,
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

pub fn end_session(store_dir: impl AsRef<std::path::Path>) -> io::Result<SessionRecord> {
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

pub(super) fn generate_session_ticket_id() -> String {
    let sequence = SESSION_TICKET_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("dtk-sess-{:x}-{sequence}", now_unix_ms())
}
