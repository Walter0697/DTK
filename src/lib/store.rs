use crate::{
    parse_structured_payload, retrieve_json_payload, stable_ref_id, StoreIndexEntry,
    STORE_REF_SEQUENCE,
};
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CleanupReport {
    pub removed_count: usize,
    pub remaining_count: usize,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CleanupPreview {
    pub expired_ref_ids: Vec<String>,
    pub remaining_count: usize,
}

pub fn store_original_payload(
    raw_payload: &str,
    store_dir: impl AsRef<Path>,
) -> io::Result<String> {
    store_original_payload_with_retention(raw_payload, store_dir, None)
}

pub fn store_original_payload_with_retention(
    raw_payload: &str,
    store_dir: impl AsRef<Path>,
    retention_days: Option<u64>,
) -> io::Result<String> {
    let content_ref_id = stable_ref_id(raw_payload).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "input is not a structured payload",
        )
    })?;
    let ref_id = execution_ref_id(&content_ref_id);

    let store_dir = store_dir.as_ref();
    let refs_dir = store_dir.join("refs");
    fs::create_dir_all(&refs_dir)?;

    let payload_path = refs_dir.join(format!("{ref_id}.json"));
    fs::write(&payload_path, raw_payload)?;

    let index_path = store_dir.join("index.json");
    let mut index = load_store_index(&index_path)?;
    let created_at_unix_ms = now_unix_ms();
    let expires_at_unix_ms = retention_days
        .map(|days| created_at_unix_ms.saturating_add(days as u128 * 24 * 60 * 60 * 1000));
    index.insert(
        ref_id.clone(),
        StoreIndexEntry {
            path: payload_path.to_string_lossy().to_string(),
            created_at_unix_ms,
            retention_days,
            expires_at_unix_ms,
        },
    );
    write_store_index(&index_path, &index)?;

    Ok(ref_id)
}

pub fn store_filtered_payload(
    filtered_json: &serde_json::Value,
    store_dir: impl AsRef<Path>,
    ref_id: &str,
) -> io::Result<PathBuf> {
    let store_dir = store_dir.as_ref();
    let filtered_dir = store_dir.join("filtered");
    fs::create_dir_all(&filtered_dir)?;

    let filtered_path = filtered_dir.join(format!("{ref_id}.json"));
    let content = serde_json::to_string_pretty(filtered_json).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid filtered json: {err}"),
        )
    })?;
    fs::write(&filtered_path, content)?;

    Ok(filtered_path)
}

pub fn recover_original_payload(ref_id: &str, store_dir: impl AsRef<Path>) -> io::Result<String> {
    let store_dir = store_dir.as_ref();
    let index_path = store_dir.join("index.json");
    let index = load_store_index(&index_path)?;

    let path = index
        .get(ref_id)
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("unknown ref_id: {ref_id}"))
        })?
        .path
        .clone();

    fs::read_to_string(path)
}

pub fn retrieve_original_payload(
    ref_id: &str,
    store_dir: impl AsRef<Path>,
    fields: &[String],
    array_index: Option<usize>,
    all: bool,
) -> io::Result<serde_json::Value> {
    let payload = recover_original_payload(ref_id, store_dir)?;
    let value = parse_structured_payload(&payload).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "stored payload is not a structured object or array",
        )
    })?;

    retrieve_json_payload(&value, fields, array_index, all).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "no matching values found for the requested fields",
        )
    })
}

pub fn cleanup_expired_payloads(store_dir: impl AsRef<Path>) -> io::Result<CleanupReport> {
    let store_dir = store_dir.as_ref();
    let index_path = store_dir.join("index.json");
    let mut index = load_store_index(&index_path)?;
    let expired_ids = expired_ref_ids(&index);
    let mut removed_count = 0;

    for ref_id in expired_ids {
        if let Some(entry) = index.remove(&ref_id) {
            let _ = fs::remove_file(&entry.path);
            let _ = fs::remove_file(crate::paths::filtered_payload_path(store_dir, &ref_id));
            removed_count += 1;
        }
    }

    write_store_index(&index_path, &index)?;

    Ok(CleanupReport {
        removed_count,
        remaining_count: index.len(),
    })
}

pub fn preview_expired_payloads(store_dir: impl AsRef<Path>) -> io::Result<CleanupPreview> {
    let store_dir = store_dir.as_ref();
    let index_path = store_dir.join("index.json");
    let index = load_store_index(&index_path)?;

    Ok(CleanupPreview {
        expired_ref_ids: expired_ref_ids(&index),
        remaining_count: index.len(),
    })
}

pub fn read_store_index(
    store_dir: impl AsRef<Path>,
) -> io::Result<BTreeMap<String, StoreIndexEntry>> {
    let store_dir = store_dir.as_ref();
    let index_path = store_dir.join("index.json");
    load_store_index(&index_path)
}

fn execution_ref_id(content_ref_id: &str) -> String {
    let created_at_unix_ms = now_unix_ms();
    let sequence = STORE_REF_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{content_ref_id}_{created_at_unix_ms}_{sequence}")
}

fn now_unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

fn load_store_index(path: &Path) -> io::Result<BTreeMap<String, StoreIndexEntry>> {
    if !path.exists() {
        return Ok(BTreeMap::new());
    }

    let content = fs::read_to_string(path)?;
    let index =
        serde_json::from_str::<BTreeMap<String, StoreIndexEntry>>(&content).map_err(|err| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("invalid store index: {err}"),
            )
        })?;
    Ok(index)
}

fn expired_ref_ids(index: &BTreeMap<String, StoreIndexEntry>) -> Vec<String> {
    let now = now_unix_ms();
    index
        .iter()
        .filter_map(|(ref_id, entry)| {
            let expired = entry
                .expires_at_unix_ms
                .map(|expires_at| expires_at <= now)
                .unwrap_or(false);
            if expired {
                Some(ref_id.clone())
            } else {
                None
            }
        })
        .collect()
}

fn write_store_index(path: &Path, index: &BTreeMap<String, StoreIndexEntry>) -> io::Result<()> {
    let content = serde_json::to_string_pretty(index).map_err(|err| {
        io::Error::new(io::ErrorKind::InvalidData, format!("invalid index: {err}"))
    })?;
    fs::write(path, content)
}
