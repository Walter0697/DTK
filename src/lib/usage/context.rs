use crate::{
    filtered_payload_path, usage_db_path, CommandSignatureInput, FieldAccessContext,
    RetrieveContext,
};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::Value;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

pub(super) fn resolved_usage_dir(preferred_store_dir: impl AsRef<Path>) -> PathBuf {
    if std::env::var("DTK_USAGE_DIR").is_ok() {
        return crate::runtime_usage_dir();
    }

    let preferred = preferred_store_dir.as_ref();
    if crate::paths::dir_is_writable(preferred) {
        preferred.to_path_buf()
    } else {
        crate::runtime_usage_dir()
    }
}

pub(super) fn resolve_signature_id(
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

pub(super) fn load_field_access_context(
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
