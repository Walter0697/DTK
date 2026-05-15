use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

const DTK_GUIDE: &str = include_str!("../DTK.md");
const DTK_CONFIG_ASSISTANT_SKILL: &str = include_str!("../skills/dtk/SKILL.md");
const DUMMYJSON_USERS_CONFIG: &str = include_str!("../samples/config.dummyjson.users.json");
const DEFAULT_USAGE_RETENTION_DAYS: u64 = 30;
const USAGE_SCHEMA_VERSION: i32 = 2;
pub const DEFAULT_SAMPLE_CONFIG_NAME: &str = "dummyjson_users.json";
static STORE_REF_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static SESSION_TICKET_SEQUENCE: AtomicU64 = AtomicU64::new(0);
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct FilterConfig {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub request: Option<String>,
    #[serde(default)]
    pub notes: Option<String>,
    #[serde(default)]
    pub content_path: Option<String>,
    #[serde(default)]
    pub allow: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default, PartialEq, Eq)]
pub struct HookRules {
    #[serde(default)]
    pub rules: Vec<HookRule>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct HookRule {
    pub name: Option<String>,
    pub config: Option<String>,
    #[serde(default)]
    pub command_prefix: Option<String>,
    #[serde(default)]
    pub command_contains: Vec<String>,
    #[serde(default)]
    pub retention_days: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoreIndexEntry {
    pub path: String,
    pub created_at_unix_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_days: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at_unix_ms: Option<u128>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSignatureInput {
    pub command: String,
    pub domain: String,
    pub details: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecMetricsInput {
    pub ref_id: String,
    pub created_at_unix_ms: u128,
    pub signature: CommandSignatureInput,
    pub config_id: String,
    pub config_path: String,
    pub original_tokens: usize,
    pub filtered_tokens: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExecMetricIssueInput {
    pub ref_id: String,
    pub created_at_unix_ms: u128,
    pub signature: CommandSignatureInput,
    pub config_id: String,
    pub config_path: String,
    pub original_tokens: usize,
    pub filtered_tokens: usize,
    pub issue_kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldAccessRecordInput {
    pub ref_id: String,
    pub created_at_unix_ms: u128,
    pub fields: Vec<String>,
    pub array_index: Option<usize>,
    pub all: bool,
    pub access_kind: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigRecommendation {
    pub config_id: String,
    pub config_path: String,
    pub command: String,
    pub domain: String,
    pub details: String,
    pub recommendation_kind: String,
    pub field_path: Option<String>,
    pub event_count: i64,
    pub summary: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecommendationThresholds {
    pub expand_field_access_count: i64,
    pub tighten_fallback_count: i64,
    pub remove_fallback_count: i64,
    pub tighten_allow_count_min: usize,
}

impl Default for RecommendationThresholds {
    fn default() -> Self {
        Self {
            expand_field_access_count: 3,
            tighten_fallback_count: 3,
            remove_fallback_count: 6,
            tighten_allow_count_min: 6,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FieldAccessContext {
    signature: Option<CommandSignatureInput>,
    session_id: Option<i64>,
    ticket_id: String,
    config_id: String,
    config_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RetrieveContext {
    config_id: String,
    config_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRecord {
    pub id: i64,
    pub ticket_id: String,
    pub started_at_unix_ms: i64,
    pub ended_at_unix_ms: Option<i64>,
}

pub fn is_json_payload(text: &str) -> bool {
    let stripped = text.trim();
    if stripped.is_empty() {
        return false;
    }

    match serde_json::from_str::<Value>(stripped) {
        Ok(Value::Object(_)) | Ok(Value::Array(_)) => true,
        Ok(_) => false,
        Err(_) => false,
    }
}

pub fn parse_json_payload(text: &str) -> Option<Value> {
    let stripped = text.trim();
    if stripped.is_empty() {
        return None;
    }

    match serde_json::from_str::<Value>(stripped) {
        Ok(value @ Value::Object(_)) | Ok(value @ Value::Array(_)) => Some(value),
        Ok(_) => None,
        Err(_) => None,
    }
}

pub fn collect_field_paths(value: &Value) -> Vec<String> {
    let mut paths = Vec::new();
    collect_field_paths_inner(value, "", &mut paths);
    paths.sort();
    paths
}

pub fn load_filter_config(path: impl AsRef<Path>) -> std::io::Result<FilterConfig> {
    let content = fs::read_to_string(path)?;
    let config = serde_json::from_str::<FilterConfig>(&content).map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid config: {err}"),
        )
    })?;
    Ok(config)
}

pub fn write_filter_config(path: impl AsRef<Path>, config: &FilterConfig) -> io::Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let content = serde_json::to_string_pretty(config).map_err(|err| {
        io::Error::new(io::ErrorKind::InvalidData, format!("invalid config: {err}"))
    })?;
    fs::write(path, format!("{content}\n"))
}

pub fn load_hook_rules(path: impl AsRef<Path>) -> std::io::Result<HookRules> {
    let content = fs::read_to_string(path)?;
    let rules = serde_json::from_str::<HookRules>(&content).map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid hook rules: {err}"),
        )
    })?;
    Ok(rules)
}

pub fn write_hook_rules(path: impl AsRef<Path>, rules: &HookRules) -> std::io::Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let content = serde_json::to_string_pretty(rules).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid hook rules: {err}"),
        )
    })?;
    fs::write(path, content)
}

pub fn add_or_update_hook_rule(path: impl AsRef<Path>, rule: HookRule) -> std::io::Result<bool> {
    let path = path.as_ref();
    let mut rules = match load_hook_rules(path) {
        Ok(rules) => rules,
        Err(err) if err.kind() == io::ErrorKind::NotFound => HookRules::default(),
        Err(err) => return Err(err),
    };

    let mut changed = false;
    let mut replaced = false;
    for existing in &mut rules.rules {
        if existing.name == rule.name || existing.config == rule.config {
            if existing != &rule {
                *existing = rule.clone();
                changed = true;
            }
            replaced = true;
            break;
        }
    }

    if !replaced {
        rules.rules.push(rule);
        changed = true;
    }

    if changed {
        write_hook_rules(path, &rules)?;
    }

    Ok(changed)
}

pub fn remove_hook_rules_for_config(
    path: impl AsRef<Path>,
    config_identifier: &str,
) -> io::Result<bool> {
    let path = path.as_ref();
    let mut rules = match load_hook_rules(path) {
        Ok(rules) => rules,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    };

    let before = rules.rules.len();
    rules.rules.retain(|rule| {
        rule.config.as_deref() != Some(config_identifier)
            && rule.name.as_deref() != Some(config_identifier)
    });
    if rules.rules.len() == before {
        return Ok(false);
    }

    write_hook_rules(path, &rules)?;
    Ok(true)
}

pub fn default_store_dir() -> PathBuf {
    std::env::var("DTK_STORE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| platform_data_dir().join("dtk").join("store"))
}

pub fn runtime_store_dir() -> PathBuf {
    default_store_dir()
}

pub fn default_usage_dir() -> PathBuf {
    std::env::var("DTK_USAGE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| default_store_dir())
}

pub fn runtime_usage_dir() -> PathBuf {
    if std::env::var("DTK_USAGE_DIR").is_ok() {
        return default_usage_dir();
    }

    let preferred = default_usage_dir();
    if dir_is_writable(&preferred) {
        preferred
    } else {
        std::env::temp_dir().join("dtk").join("usage")
    }
}

pub fn default_config_dir() -> PathBuf {
    std::env::var("DTK_CONFIG_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| platform_config_dir().join("dtk"))
}

pub fn platform_data_dir() -> PathBuf {
    if cfg!(windows) {
        windows_data_dir()
    } else {
        xdg_data_dir()
    }
}

pub fn platform_config_dir() -> PathBuf {
    if cfg!(windows) {
        windows_config_dir()
    } else {
        xdg_config_dir()
    }
}

pub fn xdg_data_dir() -> PathBuf {
    std::env::var("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("HOME").map(|home| PathBuf::from(home).join(".local/share")))
        .unwrap_or_else(|_| PathBuf::from(".local/share"))
}

pub fn xdg_config_dir() -> PathBuf {
    std::env::var("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("HOME").map(|home| PathBuf::from(home).join(".config")))
        .unwrap_or_else(|_| PathBuf::from(".config"))
}

pub fn windows_data_dir() -> PathBuf {
    std::env::var("LOCALAPPDATA")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("APPDATA").map(PathBuf::from))
        .unwrap_or_else(|_| PathBuf::from("."))
}

pub fn windows_config_dir() -> PathBuf {
    std::env::var("APPDATA")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("LOCALAPPDATA").map(PathBuf::from))
        .unwrap_or_else(|_| PathBuf::from("."))
}

pub fn resolve_config_path(path: impl AsRef<Path>) -> PathBuf {
    let path = path.as_ref();
    if path.is_absolute() {
        return path.to_path_buf();
    }

    let global_path = default_config_dir().join("configs").join(path);
    if global_path.exists() {
        return global_path;
    }

    if path.exists() {
        return path.to_path_buf();
    }

    global_path
}

pub fn resolve_filter_config_id(config: &FilterConfig, config_path: impl AsRef<Path>) -> String {
    if let Some(id) = config
        .id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return id.to_string();
    }

    if let Some(name) = config
        .name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return name.to_string();
    }

    let path = config_path.as_ref();
    if let Some(stem) = path.file_stem().and_then(|value| value.to_str()) {
        let stem = stem.trim();
        if !stem.is_empty() {
            return stem.to_string();
        }
    }

    let rendered = path.to_string_lossy().trim().to_string();
    if !rendered.is_empty() {
        return rendered;
    }

    "dtk_config".to_string()
}

pub fn stable_ref_id(raw_json: &str) -> Option<String> {
    let value = parse_json_payload(raw_json)?;
    let canonical = serde_json::to_string(&value).ok()?;
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    let digest = hasher.finalize();
    Some(format!("dtk_{}", hex_string(&digest[..16])))
}

pub fn store_original_payload(raw_json: &str, store_dir: impl AsRef<Path>) -> io::Result<String> {
    store_original_payload_with_retention(raw_json, store_dir, None)
}

pub fn store_original_payload_with_retention(
    raw_json: &str,
    store_dir: impl AsRef<Path>,
    retention_days: Option<u64>,
) -> io::Result<String> {
    let content_ref_id = stable_ref_id(raw_json).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "input is not structured JSON")
    })?;
    let ref_id = execution_ref_id(&content_ref_id);

    let store_dir = store_dir.as_ref();
    let refs_dir = store_dir.join("refs");
    fs::create_dir_all(&refs_dir)?;

    let payload_path = refs_dir.join(format!("{ref_id}.json"));
    fs::write(&payload_path, raw_json)?;

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

fn execution_ref_id(content_ref_id: &str) -> String {
    let created_at_unix_ms = now_unix_ms();
    let sequence = STORE_REF_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!("{content_ref_id}_{created_at_unix_ms}_{sequence}")
}

pub fn store_filtered_payload(
    filtered_json: &Value,
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
) -> io::Result<Value> {
    let payload = recover_original_payload(ref_id, store_dir)?;
    let value = parse_json_payload(&payload)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "stored payload is not JSON"))?;

    retrieve_json_payload(&value, fields, array_index, all).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "no matching values found for the requested fields",
        )
    })
}

pub fn retrieve_json_payload(
    value: &Value,
    fields: &[String],
    array_index: Option<usize>,
    all: bool,
) -> Option<Value> {
    match value {
        Value::Array(items) if all => {
            let allow_patterns = retrieve_patterns(fields, true);
            let mut projected = Vec::new();
            for item in items {
                let Some(selected) = project_selected_value(item, &allow_patterns) else {
                    continue;
                };
                projected.push(selected);
            }
            Some(Value::Array(projected))
        }
        Value::Array(items) => {
            let index = array_index?;
            let item = items.get(index)?;
            let allow_patterns = retrieve_patterns(fields, true);
            project_selected_value(item, &allow_patterns)
        }
        _ if array_index.is_some() || all => None,
        _ => {
            let allow_patterns = retrieve_patterns(fields, false);
            project_selected_value(value, &allow_patterns)
        }
    }
}

fn project_selected_value(value: &Value, allow_patterns: &[PathPattern]) -> Option<Value> {
    if allow_patterns.is_empty() {
        Some(value.clone())
    } else {
        project_value(value, &[], allow_patterns, true)
    }
}

fn retrieve_patterns(fields: &[String], strip_leading_any_index: bool) -> Vec<PathPattern> {
    fields
        .iter()
        .map(|field| PathPattern::parse(field.trim()))
        .filter_map(|pattern| {
            if pattern.segments.is_empty() {
                None
            } else if strip_leading_any_index {
                Some(pattern.strip_first_any_index())
            } else {
                Some(pattern)
            }
        })
        .collect()
}

fn normalize_repeated_field_path(field: &str) -> Option<String> {
    let mut pattern = PathPattern::parse(field.trim());
    if pattern.segments.is_empty() {
        return None;
    }

    for segment in &mut pattern.segments {
        if matches!(segment, PathSegment::Index(_)) {
            *segment = PathSegment::AnyIndex;
        }
    }

    Some(render_field_path(&pattern.segments))
}

fn normalize_path_pattern_for_config(
    pattern: PathPattern,
    config: &FilterConfig,
) -> Option<PathPattern> {
    let rendered = render_field_path(&pattern.segments);
    let normalized = normalize_field_path_for_config(&rendered, config)?;
    let pattern = PathPattern::parse(&normalized);
    if pattern.segments.is_empty() {
        None
    } else {
        Some(pattern)
    }
}

fn normalize_field_path_for_config(field_path: &str, config: &FilterConfig) -> Option<String> {
    let mut normalized = field_path.trim();
    if let Some(content_path) = config
        .content_path
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if let Some(remainder) = normalized.strip_prefix(content_path) {
            if remainder.is_empty() || remainder.starts_with('.') || remainder.starts_with('[') {
                normalized = remainder;
            }
        }
    }

    let normalized = normalized.trim();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized.to_string())
    }
}

fn project_value(
    value: &Value,
    current_path: &[PathSegment],
    patterns: &[PathPattern],
    is_root: bool,
) -> Option<Value> {
    let is_exact = patterns
        .iter()
        .any(|pattern| pattern.matches_exact(current_path));
    let has_descendant = patterns
        .iter()
        .any(|pattern| pattern.path_is_prefix(current_path));

    if is_exact {
        return Some(value.clone());
    }

    if !has_descendant && !is_root {
        return None;
    }

    match value {
        Value::Object(map) => {
            let mut projected = serde_json::Map::new();

            for (key, child) in map {
                let mut child_path = current_path.to_vec();
                child_path.push(PathSegment::Key(key.clone()));

                if let Some(selected) = project_value(child, &child_path, patterns, false) {
                    projected.insert(key.clone(), selected);
                }
            }

            if projected.is_empty() && !is_root {
                None
            } else {
                Some(Value::Object(projected))
            }
        }
        Value::Array(items) => {
            let mut projected = Vec::new();

            for (index, child) in items.iter().enumerate() {
                let mut child_path = current_path.to_vec();
                child_path.push(PathSegment::Index(index));

                if let Some(selected) = project_value(child, &child_path, patterns, false) {
                    projected.push(selected);
                }
            }

            if projected.is_empty() && !is_root {
                None
            } else {
                Some(Value::Array(projected))
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => Some(value.clone()),
    }
}

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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct UsageCleanupReport {
    pub removed_exec_metrics: usize,
    pub removed_exec_metric_issues: usize,
    pub removed_field_access_events: usize,
    pub removed_command_signatures: usize,
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
            let _ = fs::remove_file(store_dir.join("filtered").join(format!("{ref_id}.json")));
            removed_count += 1;
        }
    }

    write_store_index(&index_path, &index)?;

    Ok(CleanupReport {
        removed_count,
        remaining_count: index.len(),
    })
}

fn usage_retention_cutoff_unix_ms() -> u128 {
    let retention_days = std::env::var("DTK_USAGE_RETENTION_DAYS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_USAGE_RETENTION_DAYS);
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

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AgentInstallReport {
    pub changed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentTarget {
    All,
    Codex,
    Claude,
    Cursor,
}

impl AgentTarget {
    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "all" => Some(Self::All),
            "codex" => Some(Self::Codex),
            "claude" => Some(Self::Claude),
            "cursor" => Some(Self::Cursor),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Codex => "codex",
            Self::Claude => "claude",
            Self::Cursor => "cursor",
        }
    }
}

pub fn install_agent_guidance(target: AgentTarget) -> io::Result<AgentInstallReport> {
    let mut changed = false;

    match target {
        AgentTarget::All => {
            changed |= install_codex_guidance()?;
            changed |= install_codex_agents_attachment()?;
            changed |= install_claude_guidance()?;
            changed |= install_cursor_guidance()?;
        }
        AgentTarget::Codex => {
            changed |= install_codex_guidance()?;
            changed |= install_codex_agents_attachment()?;
        }
        AgentTarget::Claude => {
            changed |= install_claude_guidance()?;
        }
        AgentTarget::Cursor => {
            changed |= install_cursor_guidance()?;
        }
    }

    changed |= install_sample_configs()?;

    Ok(AgentInstallReport { changed })
}

pub fn uninstall_agent_guidance(target: AgentTarget) -> io::Result<AgentInstallReport> {
    let mut changed = false;

    match target {
        AgentTarget::All => {
            changed |= uninstall_codex_guidance()?;
            changed |= uninstall_codex_agents_attachment()?;
            changed |= uninstall_claude_guidance()?;
            changed |= uninstall_cursor_guidance()?;
        }
        AgentTarget::Codex => {
            changed |= uninstall_codex_guidance()?;
            changed |= uninstall_codex_agents_attachment()?;
        }
        AgentTarget::Claude => {
            changed |= uninstall_claude_guidance()?;
        }
        AgentTarget::Cursor => {
            changed |= uninstall_cursor_guidance()?;
        }
    }

    Ok(AgentInstallReport { changed })
}

pub fn install_config_skill(target: AgentTarget) -> io::Result<bool> {
    match target {
        AgentTarget::All => {
            let mut changed = false;
            changed |= install_codex_skill()?;
            changed |= install_claude_skill()?;
            changed |= install_cursor_skill()?;
            Ok(changed)
        }
        AgentTarget::Codex => install_codex_skill(),
        AgentTarget::Claude => install_claude_skill(),
        AgentTarget::Cursor => install_cursor_skill(),
    }
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

pub fn filtered_payload_path(store_dir: impl AsRef<Path>, ref_id: &str) -> PathBuf {
    store_dir
        .as_ref()
        .join("filtered")
        .join(format!("{ref_id}.json"))
}

pub fn usage_db_path(store_dir: impl AsRef<Path>) -> PathBuf {
    store_dir.as_ref().join("usage.sqlite3")
}

pub fn summarize_command_signature(command_args: &[String]) -> Option<CommandSignatureInput> {
    let command = command_args.first()?.to_string();
    let details = format_command_details(command_args);
    let domain = if command == "curl" {
        extract_curl_domain(command_args).unwrap_or_default()
    } else {
        String::new()
    };

    Some(CommandSignatureInput {
        command,
        domain,
        details,
    })
}

fn format_command_details(command_args: &[String]) -> String {
    command_args
        .iter()
        .map(|arg| shell_quote_argument(arg))
        .collect::<Vec<_>>()
        .join(" ")
}

fn shell_quote_argument(arg: &str) -> String {
    if arg.is_empty()
        || arg.chars().any(|ch| {
            ch.is_whitespace()
                || matches!(
                    ch,
                    '\'' | '"'
                        | '\\'
                        | '$'
                        | '`'
                        | '!'
                        | '('
                        | ')'
                        | '{'
                        | '}'
                        | '['
                        | ']'
                        | ';'
                        | '&'
                        | '|'
                        | '<'
                        | '>'
                        | '*'
                        | '?'
                        | '~'
                )
        })
    {
        let escaped = arg.replace('\'', r#"'"'"'"#);
        format!("'{escaped}'")
    } else {
        arg.to_string()
    }
}

pub fn token_count_for_content(content: &str) -> usize {
    let normalized = serde_json::from_str::<Value>(content)
        .ok()
        .and_then(|value| serde_json::to_string(&value).ok())
        .unwrap_or_else(|| content.to_string());

    let mut count = 0usize;
    let mut in_word = false;

    for ch in normalized.chars() {
        if ch.is_whitespace() {
            if in_word {
                in_word = false;
            }
            continue;
        }

        if ch.is_alphanumeric() || ch == '_' {
            if !in_word {
                count += 1;
                in_word = true;
            }
        } else {
            if in_word {
                in_word = false;
            }
            count += 1;
        }
    }

    count
}

pub fn token_count_for_path(path: impl AsRef<Path>) -> io::Result<usize> {
    let content = fs::read_to_string(path)?;
    Ok(token_count_for_content(&content))
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

fn dir_is_writable(path: &Path) -> bool {
    if fs::create_dir_all(path).is_err() {
        return false;
    }

    let test_path = path.join(".dtk-write-test");
    match fs::write(&test_path, b"ok") {
        Ok(()) => {
            let _ = fs::remove_file(&test_path);
            true
        }
        Err(_) => false,
    }
}

fn resolved_usage_dir(preferred_store_dir: impl AsRef<Path>) -> PathBuf {
    if std::env::var("DTK_USAGE_DIR").is_ok() {
        return runtime_usage_dir();
    }

    let preferred = preferred_store_dir.as_ref();
    if dir_is_writable(preferred) {
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

fn field_is_allowlisted(config: &FilterConfig, field_path: &str) -> bool {
    let Some(actual) = normalize_path_pattern_for_config(PathPattern::parse(field_path), config)
    else {
        return false;
    };
    config
        .allow
        .iter()
        .filter_map(|pattern| {
            normalize_path_pattern_for_config(PathPattern::parse(pattern), config)
        })
        .any(|pattern| pattern.covers_path(&actual.segments))
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
            .unwrap_or_else(|| field_path.clone());

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

fn extract_curl_domain(command_args: &[String]) -> Option<String> {
    let mut iter = command_args.iter().skip(1).peekable();
    while let Some(arg) = iter.next() {
        if arg == "--url" {
            if let Some(url) = iter.next() {
                if let Some(domain) = domain_from_url(url) {
                    return Some(domain);
                }
            }
            continue;
        }

        if let Some(domain) = domain_from_url(arg) {
            return Some(domain);
        }
    }

    None
}

fn domain_from_url(value: &str) -> Option<String> {
    let remainder = value
        .strip_prefix("https://")
        .or_else(|| value.strip_prefix("http://"))?;

    let host_port = remainder.split(['/', '?', '#']).next().unwrap_or("");
    let host = host_port.rsplit('@').next().unwrap_or(host_port);
    let host = if let Some(stripped) = host.strip_prefix('[') {
        stripped.split(']').next().unwrap_or("")
    } else {
        host.split(':').next().unwrap_or("")
    };

    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
}

pub fn filter_json_payload(value: &Value, config: &FilterConfig) -> Option<Value> {
    let allow_patterns: Vec<PathPattern> =
        config.allow.iter().map(|s| PathPattern::parse(s)).collect();

    if let Some(content_path) = config.content_path.as_deref() {
        let mut wrapped = value.clone();
        let subtree = resolve_content_path_mut(&mut wrapped, content_path)?;
        let filtered_subtree = filter_value(subtree, &[], &allow_patterns, true)?;
        *subtree = filtered_subtree;
        Some(wrapped)
    } else {
        filter_value(value, &[], &allow_patterns, true)
    }
}

pub fn filter_json_payload_with_metadata(value: &Value, config: &FilterConfig) -> Option<Value> {
    let filtered = filter_json_payload(value, config)?;
    Some(apply_filter_metadata(value, &filtered, None, Some(config)))
}

pub fn filter_json_payload_with_ref(
    value: &Value,
    config: &FilterConfig,
    ref_id: &str,
) -> Option<Value> {
    let filtered = filter_json_payload(value, config)?;
    Some(apply_filter_metadata(
        value,
        &filtered,
        Some(ref_id),
        Some(config),
    ))
}

fn collect_field_paths_inner(value: &Value, prefix: &str, paths: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                let path = if prefix.is_empty() {
                    key.to_string()
                } else {
                    format!("{prefix}.{key}")
                };
                paths.push(path.clone());
                collect_field_paths_inner(child, &path, paths);
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                let path = if prefix.is_empty() {
                    format!("[{index}]")
                } else {
                    format!("{prefix}[{index}]")
                };
                paths.push(path.clone());
                collect_field_paths_inner(child, &path, paths);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn resolve_content_path_mut<'a>(value: &'a mut Value, content_path: &str) -> Option<&'a mut Value> {
    let trimmed = content_path.trim();
    if trimmed.is_empty() {
        return Some(value);
    }

    let mut current = value;
    for segment in trimmed.split('.').filter(|part| !part.is_empty()) {
        current = match current {
            Value::Object(map) => map.get_mut(segment)?,
            _ => return None,
        };
    }

    Some(current)
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PathSegment {
    Key(String),
    AnyIndex,
    Index(usize),
    AnyDescendant,
}

#[derive(Debug, Clone)]
struct PathPattern {
    segments: Vec<PathSegment>,
}

impl PathPattern {
    fn parse(pattern: &str) -> Self {
        let mut segments = Vec::new();

        for part in pattern.split('.') {
            if part.is_empty() {
                continue;
            }

            if part == "**" {
                segments.push(PathSegment::AnyDescendant);
                break;
            }

            let mut remainder = part;
            loop {
                if let Some(open) = remainder.find('[') {
                    let (prefix, rest) = remainder.split_at(open);
                    if !prefix.is_empty() {
                        segments.push(PathSegment::Key(prefix.to_string()));
                    }

                    if let Some(close) = rest.find(']') {
                        let index_text = &rest[1..close];
                        if index_text.is_empty() {
                            segments.push(PathSegment::AnyIndex);
                        } else if let Ok(index) = index_text.parse::<usize>() {
                            segments.push(PathSegment::Index(index));
                        } else {
                            segments.push(PathSegment::Key(rest[..=close].to_string()));
                        }
                        remainder = &rest[close + 1..];
                        if remainder.is_empty() {
                            break;
                        }
                    } else {
                        if !remainder.is_empty() {
                            segments.push(PathSegment::Key(remainder.to_string()));
                        }
                        break;
                    }
                } else {
                    segments.push(PathSegment::Key(remainder.to_string()));
                    break;
                }
            }
        }

        Self { segments }
    }

    fn strip_first_any_index(mut self) -> Self {
        if matches!(self.segments.first(), Some(PathSegment::AnyIndex)) {
            self.segments.remove(0);
        }
        self
    }

    fn covers_path(&self, path: &[PathSegment]) -> bool {
        match self.segments.last() {
            Some(PathSegment::AnyDescendant) => {
                let fixed_len = self.segments.len().saturating_sub(1);
                path.len() >= fixed_len
                    && self.segments[..fixed_len]
                        .iter()
                        .zip(path.iter())
                        .all(|(pattern, actual)| segment_matches(pattern, actual))
            }
            _ => {
                path.len() >= self.segments.len()
                    && self
                        .segments
                        .iter()
                        .zip(path.iter())
                        .all(|(pattern, actual)| segment_matches(pattern, actual))
            }
        }
    }

    fn matches_exact(&self, path: &[PathSegment]) -> bool {
        match self.segments.last() {
            Some(PathSegment::AnyDescendant) => {
                let fixed_len = self.segments.len().saturating_sub(1);
                path.len() >= fixed_len
                    && self.segments[..fixed_len]
                        .iter()
                        .zip(path.iter())
                        .all(|(pattern, actual)| segment_matches(pattern, actual))
            }
            _ => {
                self.segments.len() == path.len()
                    && self
                        .segments
                        .iter()
                        .zip(path.iter())
                        .all(|(pattern, actual)| segment_matches(pattern, actual))
            }
        }
    }

    fn path_is_prefix(&self, path: &[PathSegment]) -> bool {
        match self.segments.last() {
            Some(PathSegment::AnyDescendant) => {
                let fixed_len = self.segments.len().saturating_sub(1);
                path.len() >= fixed_len
                    && self.segments[..fixed_len]
                        .iter()
                        .zip(path.iter())
                        .all(|(pattern, actual)| segment_matches(pattern, actual))
            }
            _ => {
                path.len() <= self.segments.len()
                    && path
                        .iter()
                        .zip(self.segments.iter())
                        .all(|(actual, pattern)| segment_matches(pattern, actual))
            }
        }
    }
}

fn segment_matches(pattern: &PathSegment, actual: &PathSegment) -> bool {
    match (pattern, actual) {
        (PathSegment::AnyIndex, PathSegment::AnyIndex) => true,
        (PathSegment::AnyIndex, PathSegment::Index(_)) => true,
        (PathSegment::AnyDescendant, _) => true,
        (PathSegment::Index(left), PathSegment::Index(right)) => left == right,
        (PathSegment::Key(left), PathSegment::Key(right)) => left == right,
        _ => false,
    }
}

fn surface_metadata(value: &Value) -> Value {
    match value {
        Value::Object(map) => serde_json::json!({
            "root_kind": "object",
            "available_fields": available_fields_for_object(map),
            "content_path": content_path_for_value(value),
            "store_hint": "local"
        }),
        Value::Array(items) => {
            serde_json::json!({
                "root_kind": "array",
                "item_kind": array_item_kind(value).unwrap_or_else(|| "unknown".to_string()),
                "available_fields": available_fields_for_array(items),
                "content_path": content_path_for_value(value),
                "store_hint": "local"
            })
        }
        other => serde_json::json!({
            "root_kind": json_kind(other),
            "available_fields": [],
            "content_path": content_path_for_value(value),
            "store_hint": "local"
        }),
    }
}

fn available_fields_for_object(map: &serde_json::Map<String, Value>) -> Vec<String> {
    let mut fields = Vec::new();
    collect_available_fields_inner(&Value::Object(map.clone()), &[], &mut fields, 3);
    fields.sort();
    fields.dedup();
    fields
}

fn available_fields_for_array(items: &[Value]) -> Vec<String> {
    let mut fields = Vec::new();
    collect_available_fields_inner(&Value::Array(items.to_vec()), &[], &mut fields, 3);
    fields.sort();
    fields.dedup();
    fields
}

fn collect_available_fields_inner(
    value: &Value,
    current_path: &[PathSegment],
    fields: &mut Vec<String>,
    max_path_len: usize,
) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                let mut path = current_path.to_vec();
                path.push(PathSegment::Key(key.clone()));
                fields.push(render_field_path(&path));
                if path.len() < max_path_len {
                    collect_available_fields_inner(child, &path, fields, max_path_len);
                }
            }
        }
        Value::Array(items) => {
            let mut path = current_path.to_vec();
            path.push(PathSegment::AnyIndex);
            fields.push(render_field_path(&path));
            if path.len() < max_path_len {
                for child in items {
                    collect_available_fields_inner(child, &path, fields, max_path_len);
                }
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn render_field_path(path: &[PathSegment]) -> String {
    let mut out = String::new();
    for segment in path {
        match segment {
            PathSegment::Key(key) => {
                if out.is_empty() {
                    out.push_str(key);
                } else {
                    out.push('.');
                    out.push_str(key);
                }
            }
            PathSegment::AnyIndex => {
                out.push_str("[]");
            }
            PathSegment::Index(index) => {
                out.push('[');
                out.push_str(&index.to_string());
                out.push(']');
            }
            PathSegment::AnyDescendant => {
                if !out.is_empty() {
                    out.push('.');
                }
                out.push_str("**");
            }
        }
    }
    out
}

fn content_path_for_value(value: &Value) -> Option<String> {
    match value {
        Value::Object(map) => {
            let mut best: Option<(String, usize)> = None;
            for (key, child) in map {
                if !matches!(child, Value::Object(_) | Value::Array(_)) {
                    continue;
                }

                let path = key.to_string();
                if let Some(candidate) = content_path_for_subtree(child, &path) {
                    choose_best_content_path(&mut best, candidate);
                }
            }
            best.map(|(path, _)| path)
        }
        Value::Array(items) => {
            let path = "[]".to_string();
            let mut best = Some((path.clone(), 1));
            for item in items {
                if !matches!(item, Value::Object(_) | Value::Array(_)) {
                    continue;
                }

                if let Some(candidate) = content_path_for_subtree(item, &path) {
                    choose_best_content_path(&mut best, candidate);
                }
            }
            best.map(|(path, _)| path)
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => None,
    }
}

fn content_path_for_subtree(value: &Value, path: &str) -> Option<(String, usize)> {
    match value {
        Value::Object(map) => {
            let mut best = Some((path.to_string(), 1));
            for (key, child) in map {
                if !matches!(child, Value::Object(_) | Value::Array(_)) {
                    continue;
                }

                let next_path = if path.is_empty() {
                    key.to_string()
                } else {
                    format!("{path}.{key}")
                };

                if let Some(candidate) = content_path_for_subtree(child, &next_path) {
                    choose_best_content_path(&mut best, candidate);
                }
            }
            best
        }
        Value::Array(items) => {
            let mut score = 1;
            for item in items {
                if !matches!(item, Value::Object(_) | Value::Array(_)) {
                    continue;
                }

                if let Some((_, item_score)) = content_path_for_subtree(item, path) {
                    score = score.max(item_score + 1);
                }
            }
            Some((path.to_string(), score))
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => None,
    }
}

fn choose_best_content_path(best: &mut Option<(String, usize)>, candidate: (String, usize)) {
    match best {
        Some((_, best_score)) if *best_score >= candidate.1 => {}
        _ => *best = Some(candidate),
    }
}

fn json_kind(value: &Value) -> &'static str {
    match value {
        Value::Object(_) => "object",
        Value::Array(_) => "array",
        Value::String(_) => "string",
        Value::Number(_) => "number",
        Value::Bool(_) => "boolean",
        Value::Null => "null",
    }
}

fn array_item_kind(value: &Value) -> Option<String> {
    match value {
        Value::Array(items) => {
            let mut item_kinds: Vec<String> = Vec::new();
            for item in items {
                let kind = json_kind(item).to_string();
                if !item_kinds.iter().any(|existing| existing == &kind) {
                    item_kinds.push(kind);
                }
            }

            match item_kinds.len() {
                0 => Some("unknown".to_string()),
                1 => Some(item_kinds.remove(0)),
                _ => Some("mixed".to_string()),
            }
        }
        _ => None,
    }
}

fn merge_runtime_metadata(metadata: Value, ref_id: Option<&str>, config_id: Option<&str>) -> Value {
    match metadata {
        Value::Object(map) => {
            let mut map = map;
            if let Some(ref_id) = ref_id {
                map.insert("ref_id".to_string(), Value::String(ref_id.to_string()));
            }
            if let Some(config_id) = config_id {
                map.insert(
                    "config_id".to_string(),
                    Value::String(config_id.to_string()),
                );
            }
            Value::Object(map)
        }
        other => other,
    }
}

pub fn codex_dir() -> PathBuf {
    std::env::var("DTK_CODEX_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| platform_codex_dir())
}

pub fn claude_dir() -> PathBuf {
    std::env::var("DTK_CLAUDE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| home_dir().join(".claude"))
}

pub fn cursor_dir() -> PathBuf {
    std::env::var("DTK_CURSOR_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| home_dir().join(".cursor"))
}

fn install_codex_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= install_text_file(codex_dir().join("DTK.md"), DTK_GUIDE)?;
    Ok(changed)
}

fn uninstall_codex_guidance() -> io::Result<bool> {
    remove_if_exists(codex_dir().join("DTK.md"))
}

fn install_codex_agents_attachment() -> io::Result<bool> {
    let path = codex_dir().join("AGENTS.md");
    let guide_path = codex_dir().join("DTK.md");
    let include_line = format!("@{}", guide_path.display());
    normalize_codex_agents(path, Some(include_line), None)
}

fn uninstall_codex_agents_attachment() -> io::Result<bool> {
    let path = codex_dir().join("AGENTS.md");
    let guide_path = codex_dir().join("DTK.md");
    let remove_line = format!("@{}", guide_path.display());
    normalize_codex_agents(path, None, Some(remove_line))
}

fn install_codex_skill() -> io::Result<bool> {
    install_text_file(
        codex_dir().join("skills").join("dtk").join("SKILL.md"),
        DTK_CONFIG_ASSISTANT_SKILL,
    )
}

fn install_claude_skill() -> io::Result<bool> {
    install_text_file(
        claude_dir().join("skills").join("dtk").join("SKILL.md"),
        DTK_CONFIG_ASSISTANT_SKILL,
    )
}

fn install_cursor_skill() -> io::Result<bool> {
    install_text_file(
        cursor_dir().join("skills").join("dtk").join("SKILL.md"),
        DTK_CONFIG_ASSISTANT_SKILL,
    )
}

fn install_claude_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= install_text_file(claude_dir().join("DTK.md"), DTK_GUIDE)?;
    changed |= ensure_claude_instructions()?;
    Ok(changed)
}

fn uninstall_claude_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= remove_if_exists(claude_dir().join("DTK.md"))?;
    changed |= remove_claude_instructions()?;
    changed |= remove_if_exists(claude_dir().join("hooks").join("dtk-rewrite.sh"))?;
    changed |= remove_claude_hooks()?;
    Ok(changed)
}

fn install_cursor_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= install_text_file(cursor_dir().join("DTK.md"), DTK_GUIDE)?;
    Ok(changed)
}

fn install_sample_configs() -> io::Result<bool> {
    install_text_file(
        default_config_dir()
            .join("configs")
            .join("dummyjson_users.json"),
        DUMMYJSON_USERS_CONFIG,
    )
}

fn uninstall_cursor_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= remove_if_exists(cursor_dir().join("DTK.md"))?;
    changed |= remove_cursor_hooks()?;
    changed |= remove_if_exists(cursor_dir().join("hooks").join("dtk-rewrite.sh"))?;
    Ok(changed)
}

fn platform_codex_dir() -> PathBuf {
    if cfg!(windows) {
        windows_codex_dir()
    } else {
        unix_codex_dir()
    }
}

fn unix_codex_dir() -> PathBuf {
    std::env::var("HOME")
        .map(|home| PathBuf::from(home).join(".codex"))
        .or_else(|_| std::env::var("XDG_CONFIG_HOME").map(PathBuf::from))
        .unwrap_or_else(|_| PathBuf::from(".codex"))
}

fn windows_codex_dir() -> PathBuf {
    std::env::var("APPDATA")
        .map(PathBuf::from)
        .map(|path| path.join("Codex"))
        .or_else(|_| {
            std::env::var("LOCALAPPDATA")
                .map(PathBuf::from)
                .map(|path| path.join("Codex"))
        })
        .unwrap_or_else(|_| PathBuf::from(".codex"))
}

fn install_text_file(path: PathBuf, content: &str) -> io::Result<bool> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let changed = match fs::read_to_string(&path) {
        Ok(existing) if existing == content => false,
        _ => {
            fs::write(&path, content)?;
            true
        }
    };

    Ok(changed)
}

fn remove_if_exists(path: PathBuf) -> io::Result<bool> {
    match fs::remove_file(&path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

fn ensure_claude_instructions() -> io::Result<bool> {
    let claude_md = claude_dir().join("CLAUDE.md");
    let line = "@DTK.md";
    let existing = fs::read_to_string(&claude_md).unwrap_or_default();
    if existing
        .lines()
        .any(|existing_line| existing_line.trim() == line)
    {
        return Ok(false);
    }

    let mut next = existing.trim_end().to_string();
    if !next.is_empty() {
        next.push('\n');
    }
    next.push_str(line);
    next.push('\n');
    install_text_file(claude_md, &next)
}

fn remove_claude_hooks() -> io::Result<bool> {
    let settings_path = claude_dir().join("settings.json");
    let mut root = match load_json_file(&settings_path) {
        Ok(value) => value,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    };

    let Some(hooks) = root
        .as_object_mut()
        .and_then(|map| map.get_mut("hooks"))
        .and_then(Value::as_object_mut)
    else {
        return Ok(false);
    };

    let Some(pre_tool_use) = hooks.get_mut("PreToolUse") else {
        return Ok(false);
    };
    let Some(entries) = pre_tool_use.as_array_mut() else {
        return Ok(false);
    };

    let before = entries.len();
    entries.retain(|entry| {
        let Some(hooks) = entry.get("hooks").and_then(Value::as_array) else {
            return true;
        };
        !hooks.iter().any(|hook| {
            hook.get("command")
                .and_then(Value::as_str)
                .map(|command| command.contains("dtk-rewrite.sh"))
                .unwrap_or(false)
        })
    });

    if entries.len() == before {
        return Ok(false);
    }

    if hooks_are_empty(hooks) {
        if settings_path.exists() {
            fs::remove_file(&settings_path)?;
        }
        return Ok(true);
    }

    write_json_file(&settings_path, &root)?;
    Ok(true)
}

fn remove_claude_instructions() -> io::Result<bool> {
    let claude_md = claude_dir().join("CLAUDE.md");
    let existing = match fs::read_to_string(&claude_md) {
        Ok(text) => text,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    };

    let filtered: Vec<&str> = existing
        .lines()
        .filter(|line| line.trim() != "@DTK.md")
        .collect();

    let next = filtered.join("\n");
    if next.trim().is_empty() {
        if claude_md.exists() {
            fs::remove_file(&claude_md)?;
            return Ok(true);
        }
        return Ok(false);
    }

    let next = format!("{next}\n");
    install_text_file(claude_md, &next)
}

fn remove_cursor_hooks() -> io::Result<bool> {
    let hooks_path = cursor_dir().join("hooks.json");
    let mut root = match load_json_file(&hooks_path) {
        Ok(value) => value,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    };

    let Some(hooks) = root
        .as_object_mut()
        .and_then(|map| map.get_mut("hooks"))
        .and_then(Value::as_object_mut)
    else {
        return Ok(false);
    };

    let Some(pre_tool_use) = hooks.get_mut("preToolUse") else {
        return Ok(false);
    };
    let Some(entries) = pre_tool_use.as_array_mut() else {
        return Ok(false);
    };

    let before = entries.len();
    entries.retain(|entry| {
        entry
            .get("command")
            .and_then(Value::as_str)
            .map(|command| command != "./hooks/dtk-rewrite.sh")
            .unwrap_or(true)
    });

    if entries.len() == before {
        return Ok(false);
    }

    if hooks_are_empty(hooks) {
        if hooks_path.exists() {
            fs::remove_file(&hooks_path)?;
        }
        return Ok(true);
    }

    write_json_file(&hooks_path, &root)?;
    Ok(true)
}

fn hooks_are_empty(hooks: &serde_json::Map<String, Value>) -> bool {
    hooks.values().all(|value| match value {
        Value::Array(items) => items.is_empty(),
        _ => false,
    })
}

fn load_json_file(path: &Path) -> io::Result<Value> {
    let content = fs::read_to_string(path)?;
    serde_json::from_str::<Value>(&content)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, format!("invalid json: {err}")))
}

fn write_json_file(path: &Path, value: &Value) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let content = serde_json::to_string_pretty(value).map_err(|err| {
        io::Error::new(io::ErrorKind::InvalidData, format!("invalid json: {err}"))
    })?;
    fs::write(path, content)
}

fn home_dir() -> PathBuf {
    std::env::var("HOME")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("USERPROFILE").map(PathBuf::from))
        .unwrap_or_else(|_| PathBuf::from("."))
}

fn normalize_codex_agents(
    path: PathBuf,
    include_line: Option<String>,
    remove_line: Option<String>,
) -> io::Result<bool> {
    let existing = fs::read_to_string(&path).unwrap_or_default();
    let Some(next) =
        normalize_codex_agents_content(&existing, include_line.as_deref(), remove_line.as_deref())
    else {
        match fs::remove_file(&path) {
            Ok(()) => return Ok(true),
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(err) => return Err(err),
        };
    };
    if next == existing {
        return Ok(false);
    }

    fs::write(&path, next)?;
    Ok(true)
}

fn normalize_codex_agents_content(
    existing: &str,
    include_line: Option<&str>,
    remove_line: Option<&str>,
) -> Option<String> {
    let mut lines: Vec<String> = existing
        .lines()
        .map(str::trim)
        .filter(|line| {
            !line.is_empty()
                && *line != "@@DTK-START@@"
                && *line != "@@DTK-END@@"
                && *line != "<!-- DTK-START -->"
                && *line != "<!-- DTK-END -->"
        })
        .map(|line| line.to_string())
        .collect();

    if let Some(remove_line) = remove_line {
        lines.retain(|line| line != remove_line);
    }

    if let Some(include_line) = include_line {
        lines.retain(|line| line != include_line);
        lines.push(include_line.to_string());
    }

    if lines.is_empty() {
        return None;
    }

    Some(format!("{}\n", lines.join("\n")))
}

fn hex_string(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
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

fn apply_filter_metadata(
    original: &Value,
    filtered: &Value,
    ref_id: Option<&str>,
    config: Option<&FilterConfig>,
) -> Value {
    let metadata = surface_metadata(original);
    let config_id = config.and_then(|config| {
        config
            .id
            .as_deref()
            .or(config.name.as_deref())
            .map(str::trim)
            .filter(|value| !value.is_empty())
    });

    match filtered {
        Value::Object(map) => {
            let mut map = map.clone();
            map.insert(
                "_dtk".to_string(),
                merge_runtime_metadata(metadata, ref_id, config_id),
            );
            Value::Object(map)
        }
        _ => serde_json::json!({
            "result": filtered,
            "_dtk": merge_runtime_metadata(metadata, ref_id, config_id)
        }),
    }
}

fn filter_value(
    value: &Value,
    current_path: &[PathSegment],
    allow_patterns: &[PathPattern],
    is_root: bool,
) -> Option<Value> {
    let allow_active = !allow_patterns.is_empty();
    let is_allowed_exact = allow_patterns
        .iter()
        .any(|pattern| pattern.matches_exact(current_path));
    let is_allowed_descendant = allow_patterns
        .iter()
        .any(|pattern| pattern.path_is_prefix(current_path));

    if is_allowed_exact {
        return Some(value.clone());
    }

    if allow_active && !is_allowed_descendant && !is_root {
        return None;
    }

    match value {
        Value::Object(map) => {
            let mut filtered = serde_json::Map::new();

            for (key, child) in map {
                let mut child_path = current_path.to_vec();
                child_path.push(PathSegment::Key(key.clone()));

                let child_allowed_exact = allow_patterns
                    .iter()
                    .any(|pattern| pattern.matches_exact(&child_path));
                let child_allowed_descendant = allow_patterns
                    .iter()
                    .any(|pattern| pattern.path_is_prefix(&child_path));

                if allow_active && !child_allowed_exact && !child_allowed_descendant {
                    continue;
                }

                if let Some(filtered_child) =
                    filter_value(child, &child_path, allow_patterns, false)
                {
                    filtered.insert(key.clone(), filtered_child);
                }
            }

            if filtered.is_empty() && !is_root {
                None
            } else {
                Some(Value::Object(filtered))
            }
        }
        Value::Array(items) => {
            let mut filtered = Vec::new();

            for child in items {
                let mut child_path = current_path.to_vec();
                child_path.push(PathSegment::AnyIndex);

                let child_allowed_exact = allow_patterns
                    .iter()
                    .any(|pattern| pattern.matches_exact(&child_path));
                let child_allowed_descendant = allow_patterns
                    .iter()
                    .any(|pattern| pattern.path_is_prefix(&child_path));

                if allow_active && !child_allowed_exact && !child_allowed_descendant {
                    continue;
                }

                if let Some(filtered_child) =
                    filter_value(child, &child_path, allow_patterns, false)
                {
                    filtered.push(filtered_child);
                }
            }

            if filtered.is_empty() && !is_root {
                None
            } else {
                Some(Value::Array(filtered))
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => Some(value.clone()),
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{
        cleanup_expired_payloads, collect_field_paths, default_store_dir, end_session,
        field_is_allowlisted, filter_json_payload, filter_json_payload_with_metadata,
        is_json_payload, load_config_recommendations, load_filter_config,
        normalize_field_path_for_config, parse_json_payload, platform_data_dir,
        preview_expired_payloads, recommendation_notices_for_exec,
        recommendation_notices_for_retrieve, record_exec_metrics, record_field_access,
        recover_original_payload, resolve_filter_config_id, retrieve_json_payload,
        retrieve_original_payload, runtime_store_dir, stable_ref_id, start_session,
        store_original_payload, store_original_payload_with_retention, summarize_command_signature,
        usage_db_path, windows_data_dir, xdg_data_dir, ExecMetricsInput, FieldAccessRecordInput,
        FilterConfig, RecommendationThresholds,
    };

    fn temp_store_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join("dtk-tests").join(name)
    }

    fn now_unix_ms() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or(0)
    }

    #[test]
    fn detects_object() {
        assert!(is_json_payload(r#"{"name":"dtk","count":1}"#));
    }

    #[test]
    fn detects_array() {
        assert!(is_json_payload(r#"[1, 2, 3]"#));
    }

    #[test]
    fn rejects_plain_text() {
        assert!(!is_json_payload("hello world"));
    }

    #[test]
    fn rejects_empty_text() {
        assert!(!is_json_payload("   \n\t  "));
    }

    #[test]
    fn rejects_json_primitives() {
        assert!(!is_json_payload(r#""string""#));
        assert!(!is_json_payload("42"));
        assert!(!is_json_payload("true"));
    }

    #[test]
    fn parses_object_payload() {
        let parsed = parse_json_payload(r#"{"name":"dtk","nested":{"id":1}}"#);
        assert!(parsed.is_some());
    }

    #[test]
    fn parses_array_payload() {
        let parsed = parse_json_payload(r#"[{"id":1},{"id":2}]"#);
        assert!(parsed.is_some());
    }

    #[test]
    fn rejects_non_structured_json() {
        assert!(parse_json_payload(r#""string""#).is_none());
        assert!(parse_json_payload("42").is_none());
    }

    #[test]
    fn collects_object_field_paths() {
        let value = parse_json_payload(r#"{"user":{"id":1,"email":"a@b.com"},"status":"ok"}"#)
            .expect("expected structured json");
        let paths = collect_field_paths(&value);
        assert_eq!(
            paths,
            vec![
                "status".to_string(),
                "user".to_string(),
                "user.email".to_string(),
                "user.id".to_string()
            ]
        );
    }

    #[test]
    fn collects_array_field_paths() {
        let value = parse_json_payload(r#"[{"id":1,"name":"a"},{"id":2,"name":"b"}]"#)
            .expect("expected structured json");
        let paths = collect_field_paths(&value);
        assert_eq!(
            paths,
            vec![
                "[0]".to_string(),
                "[0].id".to_string(),
                "[0].name".to_string(),
                "[1]".to_string(),
                "[1].id".to_string(),
                "[1].name".to_string()
            ]
        );
    }

    #[test]
    fn filters_array_payload_by_allowlist() {
        let value = parse_json_payload(
            r#"[{"id":7,"title":"AlienBot Configuration","description":"","created_by":{"username":"waltercheng","id":1}}]"#,
        )
        .expect("expected structured json");

        let config = FilterConfig {
            id: None,
            name: None,
            source: None,
            request: None,
            notes: None,
            content_path: None,
            allow: vec![
                "[].id".to_string(),
                "[].title".to_string(),
                "[].created_by.username".to_string(),
            ],
        };

        let filtered = filter_json_payload(&value, &config).expect("expected filtered json");
        let rendered = serde_json::to_value(filtered).expect("expected json value");

        assert_eq!(
            rendered,
            serde_json::json!([
                {
                    "id": 7,
                    "title": "AlienBot Configuration",
                    "created_by": {
                        "username": "waltercheng"
                    }
                }
            ])
        );
    }

    #[test]
    fn allowlist_filters_only_explicit_fields() {
        let value = parse_json_payload(r#"{"title":"hello","secret":"x"}"#)
            .expect("expected structured json");

        let config = FilterConfig {
            id: None,
            name: None,
            source: None,
            request: None,
            notes: None,
            content_path: None,
            allow: vec!["title".to_string()],
        };

        let filtered = filter_json_payload(&value, &config).expect("expected filtered json");
        assert_eq!(filtered, serde_json::json!({"title":"hello"}));
    }

    #[test]
    fn adds_metadata_to_object_payload() {
        let value = parse_json_payload(r#"{"title":"hello","secret":"x"}"#)
            .expect("expected structured json");

        let config = FilterConfig {
            id: None,
            name: None,
            source: None,
            request: None,
            notes: None,
            content_path: None,
            allow: vec!["title".to_string()],
        };

        let filtered =
            filter_json_payload_with_metadata(&value, &config).expect("expected filtered json");
        assert_eq!(
            filtered,
            serde_json::json!({
                "title": "hello",
                "_dtk": {
                    "root_kind": "object",
                    "available_fields": ["secret", "title"],
                    "content_path": null,
                    "store_hint": "local"
                }
            })
        );
    }

    #[test]
    fn wraps_array_payload_with_metadata() {
        let value = parse_json_payload(r#"[{"id":1,"title":"a","updated":"x"}]"#)
            .expect("expected structured json");

        let config = FilterConfig {
            id: None,
            name: None,
            source: None,
            request: None,
            notes: None,
            content_path: None,
            allow: vec!["[].id".to_string(), "[].title".to_string()],
        };

        let filtered =
            filter_json_payload_with_metadata(&value, &config).expect("expected filtered json");
        assert_eq!(
            filtered,
            serde_json::json!({
                "result": [
                    {
                        "id": 1,
                        "title": "a"
                    }
                ],
                "_dtk": {
                    "root_kind": "array",
                    "item_kind": "object",
                    "available_fields": ["[]", "[].id", "[].title", "[].updated"],
                    "content_path": "[]",
                    "store_hint": "local"
                }
            })
        );
    }

    #[test]
    fn exposes_nested_available_fields_in_metadata() {
        let value = parse_json_payload(
            r#"{"limit":30,"users":[{"id":1,"firstName":"Jane","lastName":"Doe","hair":{"color":"black","type":"wavy"},"address":{"city":"Austin","state":"TX"}}]}"#,
        )
        .expect("expected structured json");

        let config = FilterConfig {
            id: None,
            name: None,
            source: None,
            request: None,
            notes: None,
            content_path: None,
            allow: vec!["users[].id".to_string()],
        };

        let filtered =
            filter_json_payload_with_metadata(&value, &config).expect("expected filtered json");

        assert_eq!(
            filtered,
            serde_json::json!({
                "users": [
                    {
                        "id": 1
                    }
                ],
                "_dtk": {
                    "root_kind": "object",
                    "available_fields": [
                        "limit",
                        "users",
                        "users[]",
                        "users[].address",
                        "users[].firstName",
                        "users[].hair",
                        "users[].id",
                        "users[].lastName"
                    ],
                    "content_path": "users",
                    "store_hint": "local"
                }
            })
        );
    }

    #[test]
    fn filters_content_path_subtree_while_preserving_envelope() {
        let value = parse_json_payload(
            r#"{"limit":30,"skip":0,"total":1,"users":[{"id":1,"firstName":"Jane","lastName":"Doe","secret":"x"}]}"#,
        )
        .expect("expected structured json");

        let config = FilterConfig {
            id: None,
            name: None,
            source: None,
            request: None,
            notes: None,
            content_path: Some("users".to_string()),
            allow: vec![
                "[].id".to_string(),
                "[].firstName".to_string(),
                "[].lastName".to_string(),
            ],
        };

        let filtered =
            filter_json_payload_with_metadata(&value, &config).expect("expected filtered json");

        assert_eq!(
            filtered,
            serde_json::json!({
                "limit": 30,
                "skip": 0,
                "total": 1,
                "users": [
                    {
                        "id": 1,
                        "firstName": "Jane",
                        "lastName": "Doe"
                    }
                ],
                "_dtk": {
                    "root_kind": "object",
                    "available_fields": [
                        "limit",
                        "skip",
                        "total",
                        "users",
                        "users[]",
                        "users[].firstName",
                        "users[].id",
                        "users[].lastName",
                        "users[].secret"
                    ],
                    "content_path": "users",
                    "store_hint": "local"
                }
            })
        );
    }

    #[test]
    fn filters_content_path_nested_subtree_when_parent_field_is_allowed() {
        let value = parse_json_payload(
            r#"{"users":[{"id":1,"hair":{"color":"black","type":"wavy"},"secret":"x"}]}"#,
        )
        .expect("expected structured json");

        let config = FilterConfig {
            id: None,
            name: None,
            source: None,
            request: None,
            notes: None,
            content_path: Some("users".to_string()),
            allow: vec!["[].hair".to_string()],
        };

        let filtered = filter_json_payload(&value, &config).expect("expected filtered json");
        assert_eq!(
            filtered,
            serde_json::json!({
                "users": [
                    {
                        "hair": {
                            "color": "black",
                            "type": "wavy"
                        }
                    }
                ]
            })
        );
    }

    #[test]
    fn filters_wildcard_subtree_for_dynamic_object_keys() {
        let value = parse_json_payload(
            r#"{"connections":{"Alpha":{"main":[[{"node":"A"}]]},"Beta":{"ai_tool":[[{"node":"B"}]]}},"name":"wf"}"#,
        )
        .expect("expected structured json");

        let config = FilterConfig {
            id: None,
            name: None,
            source: None,
            request: None,
            notes: None,
            content_path: None,
            allow: vec!["connections.**".to_string()],
        };

        let filtered = filter_json_payload(&value, &config).expect("expected filtered json");

        assert_eq!(
            filtered,
            serde_json::json!({
                "connections": {
                    "Alpha": {"main": [[{"node": "A"}]]},
                    "Beta": {"ai_tool": [[{"node": "B"}]]}
                }
            })
        );
    }

    #[test]
    fn stable_ref_id_is_deterministic() {
        let left = stable_ref_id(r#"{"a":1,"b":2}"#).expect("expected ref id");
        let right = stable_ref_id(r#"{"a":1,"b":2}"#).expect("expected ref id");
        assert_eq!(left, right);
        assert!(left.starts_with("dtk_"));
    }

    #[test]
    fn summarizes_curl_command_with_domain() {
        let args = vec![
            "curl".to_string(),
            "-sS".to_string(),
            "https://dummyjson.com/users".to_string(),
        ];

        let signature = summarize_command_signature(&args).expect("expected signature");

        assert_eq!(signature.command, "curl");
        assert_eq!(signature.domain, "dummyjson.com");
        assert_eq!(signature.details, "curl -sS https://dummyjson.com/users");
    }

    #[test]
    fn summarizes_non_network_command_without_domain() {
        let args = vec!["git".to_string(), "status".to_string()];

        let signature = summarize_command_signature(&args).expect("expected signature");

        assert_eq!(signature.command, "git");
        assert_eq!(signature.domain, "");
        assert_eq!(signature.details, "git status");
    }

    #[test]
    fn records_exec_metrics_with_deduplicated_signatures() {
        let store_dir = temp_store_dir("unit-test-usage");
        let created_at_unix_ms = now_unix_ms();
        let first = ExecMetricsInput {
            ref_id: "dtk_abc_1".to_string(),
            created_at_unix_ms,
            signature: summarize_command_signature(&[
                "curl".to_string(),
                "-sS".to_string(),
                "https://dummyjson.com/users".to_string(),
            ])
            .expect("expected signature"),
            config_id: "dummyjson_users".to_string(),
            config_path: "/tmp/dummyjson_users.json".to_string(),
            original_tokens: 120,
            filtered_tokens: 30,
        };
        let second = ExecMetricsInput {
            ref_id: "dtk_abc_2".to_string(),
            created_at_unix_ms: created_at_unix_ms + 1,
            signature: summarize_command_signature(&[
                "curl".to_string(),
                "-sS".to_string(),
                "https://dummyjson.com/users".to_string(),
            ])
            .expect("expected signature"),
            config_id: "dummyjson_users".to_string(),
            config_path: "/tmp/dummyjson_users.json".to_string(),
            original_tokens: 220,
            filtered_tokens: 40,
        };

        record_exec_metrics(&store_dir, &first).expect("expected usage write");
        record_exec_metrics(&store_dir, &second).expect("expected usage write");

        let connection =
            rusqlite::Connection::open(usage_db_path(&store_dir)).expect("expected usage db");
        let signature_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM command_signatures", [], |row| {
                row.get(0)
            })
            .expect("expected signature count");
        let metric_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM exec_metrics", [], |row| row.get(0))
            .expect("expected metric count");
        let domain: String = connection
            .query_row("SELECT domain FROM command_signatures LIMIT 1", [], |row| {
                row.get(0)
            })
            .expect("expected domain");

        assert_eq!(signature_count, 1);
        assert_eq!(metric_count, 2);
        assert_eq!(domain, "dummyjson.com");
        let _ = std::fs::remove_dir_all(store_dir);
    }

    #[test]
    fn prunes_old_usage_rows_while_retaining_recent_entries() {
        let store_dir = temp_store_dir("unit-test-usage-prune");
        let stale_created_at_unix_ms = now_unix_ms().saturating_sub(31_u128 * 24 * 60 * 60 * 1000);
        let fresh_created_at_unix_ms = now_unix_ms();

        let stale = ExecMetricsInput {
            ref_id: "dtk_stale".to_string(),
            created_at_unix_ms: stale_created_at_unix_ms,
            signature: summarize_command_signature(&["git".to_string(), "status".to_string()])
                .expect("expected signature"),
            config_id: "stale_cfg".to_string(),
            config_path: "/tmp/stale.json".to_string(),
            original_tokens: 10,
            filtered_tokens: 5,
        };
        let fresh = ExecMetricsInput {
            ref_id: "dtk_fresh".to_string(),
            created_at_unix_ms: fresh_created_at_unix_ms,
            signature: summarize_command_signature(&[
                "curl".to_string(),
                "-sS".to_string(),
                "https://dummyjson.com/users".to_string(),
            ])
            .expect("expected signature"),
            config_id: "fresh_cfg".to_string(),
            config_path: "/tmp/fresh.json".to_string(),
            original_tokens: 20,
            filtered_tokens: 10,
        };

        record_exec_metrics(&store_dir, &stale).expect("stale usage write");
        record_exec_metrics(&store_dir, &fresh).expect("fresh usage write");

        let connection =
            rusqlite::Connection::open(usage_db_path(&store_dir)).expect("expected usage db");
        let metric_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM exec_metrics", [], |row| row.get(0))
            .expect("expected metric count");
        let signature_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM command_signatures", [], |row| {
                row.get(0)
            })
            .expect("expected signature count");

        assert_eq!(metric_count, 1);
        assert_eq!(signature_count, 1);
        let _ = std::fs::remove_dir_all(store_dir);
    }

    #[test]
    fn resets_old_usage_schema_without_migrating_columns() {
        let store_dir = temp_store_dir("unit-test-usage-reset");
        let db_path = usage_db_path(&store_dir);
        std::fs::create_dir_all(&store_dir).expect("create store dir");

        {
            let connection = rusqlite::Connection::open(&db_path).expect("open old db");
            connection
                .execute_batch(
                    r#"
                    CREATE TABLE exec_metrics (
                        ref_id TEXT PRIMARY KEY,
                        created_at_unix_ms INTEGER NOT NULL
                    );
                    PRAGMA user_version = 1;
                    "#,
                )
                .expect("seed old schema");
        }

        let connection = rusqlite::Connection::open(&db_path).expect("open db");
        super::init_usage_schema(&connection).expect("reset schema");

        let mut statement = connection
            .prepare("PRAGMA table_info(exec_metrics)")
            .expect("prepare table info");
        let mut rows = statement.query([]).expect("query table info");
        let mut has_config_id = false;
        while let Some(row) = rows.next().expect("read table info") {
            let name: String = row.get(1).expect("read column name");
            if name == "config_id" {
                has_config_id = true;
                break;
            }
        }
        let schema_version: i32 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .expect("schema version");

        assert!(has_config_id);
        assert_eq!(schema_version, super::USAGE_SCHEMA_VERSION);
        let _ = std::fs::remove_dir_all(store_dir);
    }

    #[test]
    fn start_and_end_session_round_trip() {
        let store_dir = temp_store_dir("unit-test-usage-session");

        let started = start_session(&store_dir, None).expect("expected session start");
        assert!(started.ticket_id.starts_with("dtk-sess-"));
        assert!(started.ended_at_unix_ms.is_none());

        let ended = end_session(&store_dir).expect("expected session end");
        assert_eq!(ended.id, started.id);
        assert_eq!(ended.ticket_id, started.ticket_id);
        assert!(ended.ended_at_unix_ms.is_some());

        let connection =
            rusqlite::Connection::open(usage_db_path(&store_dir)).expect("expected usage db");
        let session_count: i64 = connection
            .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
            .expect("expected session count");
        let active_count: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM sessions WHERE ended_at_unix_ms IS NULL",
                [],
                |row| row.get(0),
            )
            .expect("expected active session count");

        assert_eq!(session_count, 1);
        assert_eq!(active_count, 0);
        let _ = std::fs::remove_dir_all(store_dir);
    }

    #[test]
    fn records_exec_metrics_with_active_ticket_id() {
        let store_dir = temp_store_dir("unit-test-usage-ticket");
        let session = start_session(&store_dir, Some("ticket-123".to_string())).expect("start");
        assert_eq!(session.ticket_id, "ticket-123");
        let created_at_unix_ms = now_unix_ms();

        let metrics = ExecMetricsInput {
            ref_id: "dtk_abc_3".to_string(),
            created_at_unix_ms,
            signature: summarize_command_signature(&[
                "curl".to_string(),
                "-sS".to_string(),
                "https://dummyjson.com/users".to_string(),
            ])
            .expect("expected signature"),
            config_id: "dummyjson_users".to_string(),
            config_path: "/tmp/dummyjson_users.json".to_string(),
            original_tokens: 200,
            filtered_tokens: 50,
        };

        record_exec_metrics(&store_dir, &metrics).expect("expected usage write");

        let connection =
            rusqlite::Connection::open(usage_db_path(&store_dir)).expect("expected usage db");
        let ticket_id: String = connection
            .query_row(
                "SELECT ticket_id FROM exec_metrics WHERE ref_id = ?1",
                ["dtk_abc_3"],
                |row| row.get(0),
            )
            .expect("expected ticket id");
        let session_id: i64 = connection
            .query_row(
                "SELECT session_id FROM exec_metrics WHERE ref_id = ?1",
                ["dtk_abc_3"],
                |row| row.get(0),
            )
            .expect("expected session id");

        assert_eq!(ticket_id, "ticket-123");
        assert_eq!(session_id, session.id);
        let _ = std::fs::remove_dir_all(store_dir);
    }

    #[test]
    fn resolves_filter_config_id_from_explicit_id_then_name_then_path() {
        let with_id = FilterConfig {
            id: Some("cfg-users".to_string()),
            name: Some("users".to_string()),
            source: None,
            request: None,
            notes: None,
            content_path: None,
            allow: vec![],
        };
        let with_name = FilterConfig {
            id: None,
            name: Some("users-name".to_string()),
            source: None,
            request: None,
            notes: None,
            content_path: None,
            allow: vec![],
        };
        let anonymous = FilterConfig {
            id: None,
            name: None,
            source: None,
            request: None,
            notes: None,
            content_path: None,
            allow: vec![],
        };

        assert_eq!(
            resolve_filter_config_id(&with_id, "/tmp/example.json"),
            "cfg-users"
        );
        assert_eq!(
            resolve_filter_config_id(&with_name, "/tmp/example.json"),
            "users-name"
        );
        assert_eq!(
            resolve_filter_config_id(&anonymous, "/tmp/example.json"),
            "example"
        );
    }

    #[test]
    fn records_field_access_and_generates_expand_recommendation() {
        let store_dir = temp_store_dir("unit-test-field-access-expand");
        let _ = std::fs::remove_dir_all(&store_dir);
        std::fs::create_dir_all(&store_dir).expect("create store dir");
        let config_path = store_dir.join("users-config.json");
        std::fs::write(
            &config_path,
            serde_json::to_string_pretty(&serde_json::json!({
                "id": "users_cfg",
                "name": "users_cfg",
                "allow": ["users[].id"]
            }))
            .expect("config json"),
        )
        .expect("write config");
        let created_at_unix_ms = now_unix_ms();

        let metrics = ExecMetricsInput {
            ref_id: "dtk_expand_1".to_string(),
            created_at_unix_ms,
            signature: summarize_command_signature(&[
                "curl".to_string(),
                "-sS".to_string(),
                "https://dummyjson.com/users".to_string(),
            ])
            .expect("expected signature"),
            config_id: "users_cfg".to_string(),
            config_path: config_path.to_string_lossy().to_string(),
            original_tokens: 200,
            filtered_tokens: 50,
        };
        record_exec_metrics(&store_dir, &metrics).expect("metrics");

        for created_at_unix_ms in [
            created_at_unix_ms + 1,
            created_at_unix_ms + 2,
            created_at_unix_ms + 3,
        ] {
            let access = FieldAccessRecordInput {
                ref_id: "dtk_expand_1".to_string(),
                created_at_unix_ms,
                fields: vec!["users[].email".to_string()],
                array_index: None,
                all: false,
                access_kind: "retrieve".to_string(),
            };
            record_field_access(&store_dir, &access).expect("field access");
        }

        let recommendations = load_config_recommendations(
            &store_dir,
            RecommendationThresholds {
                expand_field_access_count: 3,
                tighten_fallback_count: 3,
                remove_fallback_count: 6,
                tighten_allow_count_min: 6,
            },
        )
        .expect("recommendations");

        assert!(recommendations.iter().any(|recommendation| {
            recommendation.recommendation_kind == "expand_allowlist"
                && recommendation.config_id == "users_cfg"
                && recommendation.field_path.as_deref() == Some("users[].email")
        }));
        let notices = recommendation_notices_for_retrieve(
            &store_dir,
            "dtk_expand_1",
            &["users[].email".to_string()],
        )
        .expect("notices");
        assert!(notices
            .iter()
            .any(|notice| notice.contains("add `users[].email` to config `users_cfg`")));
        let _ = std::fs::remove_dir_all(store_dir);
    }

    #[test]
    fn broader_allowlist_covers_deeper_fields() {
        let config = FilterConfig {
            id: None,
            name: None,
            source: None,
            request: None,
            notes: None,
            content_path: Some("users".to_string()),
            allow: vec!["[].hair".to_string()],
        };

        assert!(field_is_allowlisted(&config, "users[].hair.color"));
        assert!(field_is_allowlisted(&config, "users[0].hair.color"));
    }

    #[test]
    fn broader_allowlist_suppresses_nested_recommendation_notice() {
        let store_dir = temp_store_dir("unit-test-broader-allowlist-notice");
        let _ = std::fs::remove_dir_all(&store_dir);
        std::fs::create_dir_all(&store_dir).expect("create store dir");
        let config_path = store_dir.join("users-config.json");
        std::fs::write(
            &config_path,
            serde_json::to_string_pretty(&serde_json::json!({
                "id": "users_cfg",
                "name": "users_cfg",
                "content_path": "users",
                "allow": ["[].hair"]
            }))
            .expect("config json"),
        )
        .expect("write config");
        let config = load_filter_config(&config_path).expect("load config");
        assert_eq!(
            normalize_field_path_for_config("users[].hair.color", &config),
            Some("[].hair.color".to_string())
        );
        assert!(field_is_allowlisted(&config, "users[].hair.color"));
        let created_at_unix_ms = now_unix_ms();

        let metrics = ExecMetricsInput {
            ref_id: "dtk_hair_1".to_string(),
            created_at_unix_ms,
            signature: summarize_command_signature(&[
                "curl".to_string(),
                "-sS".to_string(),
                "https://dummyjson.com/users".to_string(),
            ])
            .expect("expected signature"),
            config_id: "users_cfg".to_string(),
            config_path: config_path.to_string_lossy().to_string(),
            original_tokens: 200,
            filtered_tokens: 50,
        };
        record_exec_metrics(&store_dir, &metrics).expect("metrics");

        for (offset, field_path) in [
            "users[0].hair.color",
            "users[1].hair.color",
            "users[2].hair.color",
        ]
        .iter()
        .enumerate()
        {
            let access = FieldAccessRecordInput {
                ref_id: "dtk_hair_1".to_string(),
                created_at_unix_ms: created_at_unix_ms + offset as u128 + 1,
                fields: vec![(*field_path).to_string()],
                array_index: None,
                all: false,
                access_kind: "retrieve".to_string(),
            };
            record_field_access(&store_dir, &access).expect("field access");
        }

        let recommendations =
            load_config_recommendations(&store_dir, RecommendationThresholds::default())
                .expect("recommendations");
        assert!(!recommendations.iter().any(|recommendation| {
            recommendation.config_id == "users_cfg"
                && recommendation.recommendation_kind == "expand_allowlist"
        }));

        let notices = recommendation_notices_for_retrieve(
            &store_dir,
            "dtk_hair_1",
            &["users[3].hair.color".to_string()],
        )
        .expect("notices");
        assert!(notices.is_empty());
        let _ = std::fs::remove_dir_all(store_dir);
    }

    #[test]
    fn treats_indexed_retrievals_as_the_same_repeat_pattern() {
        let store_dir = temp_store_dir("unit-test-index-normalization");
        let _ = std::fs::remove_dir_all(&store_dir);
        std::fs::create_dir_all(&store_dir).expect("create store dir");
        let config_path = store_dir.join("users-config.json");
        std::fs::write(
            &config_path,
            serde_json::to_string_pretty(&serde_json::json!({
                "id": "users_cfg",
                "name": "users_cfg",
                "allow": ["users[].id"]
            }))
            .expect("config json"),
        )
        .expect("write config");

        let metrics = ExecMetricsInput {
            ref_id: "dtk_index_1".to_string(),
            created_at_unix_ms: now_unix_ms(),
            signature: summarize_command_signature(&[
                "curl".to_string(),
                "-sS".to_string(),
                "https://dummyjson.com/users".to_string(),
            ])
            .expect("expected signature"),
            config_id: "users_cfg".to_string(),
            config_path: config_path.to_string_lossy().to_string(),
            original_tokens: 200,
            filtered_tokens: 50,
        };
        record_exec_metrics(&store_dir, &metrics).expect("metrics");

        for (offset, field_path) in [
            "users[0].hair.color",
            "users[1].hair.color",
            "users[2].hair.color",
        ]
        .iter()
        .enumerate()
        {
            let access = FieldAccessRecordInput {
                ref_id: "dtk_index_1".to_string(),
                created_at_unix_ms: now_unix_ms() + offset as u128,
                fields: vec![(*field_path).to_string()],
                array_index: None,
                all: false,
                access_kind: "retrieve".to_string(),
            };
            record_field_access(&store_dir, &access).expect("field access");
        }

        let recommendations =
            load_config_recommendations(&store_dir, RecommendationThresholds::default())
                .expect("recommendations");
        assert!(recommendations.iter().any(|recommendation| {
            recommendation.recommendation_kind == "expand_allowlist"
                && recommendation.field_path.as_deref() == Some("users[].hair.color")
        }));

        let notices = recommendation_notices_for_retrieve(
            &store_dir,
            "dtk_index_1",
            &["users[3].hair.color".to_string()],
        )
        .expect("notices");
        assert!(notices
            .iter()
            .any(|notice| notice.contains("add `users[].hair.color` to config `users_cfg`")));
        let _ = std::fs::remove_dir_all(store_dir);
    }

    #[test]
    fn generates_tighten_or_remove_recommendation_for_repeated_fallbacks() {
        let store_dir = temp_store_dir("unit-test-fallback-recommendation");
        let _ = std::fs::remove_dir_all(&store_dir);
        std::fs::create_dir_all(&store_dir).expect("create store dir");
        let config_path = store_dir.join("wide-config.json");
        std::fs::write(
            &config_path,
            serde_json::to_string_pretty(&serde_json::json!({
                "id": "wide_cfg",
                "allow": [
                    "users[].id",
                    "users[].name",
                    "users[].email",
                    "users[].phone",
                    "users[].address",
                    "users[].company"
                ]
            }))
            .expect("config json"),
        )
        .expect("write config");
        let created_at_unix_ms = now_unix_ms();

        for (offset, ref_id) in ["dtk_fb_1", "dtk_fb_2", "dtk_fb_3"].iter().enumerate() {
            let metrics = ExecMetricsInput {
                ref_id: (*ref_id).to_string(),
                created_at_unix_ms: created_at_unix_ms + offset as u128,
                signature: summarize_command_signature(&[
                    "curl".to_string(),
                    "-sS".to_string(),
                    "https://dummyjson.com/users".to_string(),
                ])
                .expect("expected signature"),
                config_id: "wide_cfg".to_string(),
                config_path: config_path.to_string_lossy().to_string(),
                original_tokens: 100,
                filtered_tokens: 100,
            };
            record_exec_metrics(&store_dir, &metrics).expect("metrics");
            let issue = super::ExecMetricIssueInput {
                ref_id: (*ref_id).to_string(),
                created_at_unix_ms: created_at_unix_ms + 100 + offset as u128,
                signature: metrics.signature.clone(),
                config_id: "wide_cfg".to_string(),
                config_path: config_path.to_string_lossy().to_string(),
                original_tokens: 100,
                filtered_tokens: 140,
                issue_kind: "filtered_larger_than_original".to_string(),
            };
            super::record_exec_metric_issue(&store_dir, &issue).expect("issue");
        }

        let recommendations = load_config_recommendations(
            &store_dir,
            RecommendationThresholds {
                expand_field_access_count: 3,
                tighten_fallback_count: 3,
                remove_fallback_count: 6,
                tighten_allow_count_min: 6,
            },
        )
        .expect("recommendations");

        assert!(recommendations.iter().any(|recommendation| {
            recommendation.config_id == "wide_cfg"
                && recommendation.recommendation_kind == "tighten_allowlist"
        }));
        let notices = recommendation_notices_for_exec(
            &store_dir,
            "wide_cfg",
            "curl -sS https://dummyjson.com/users",
        )
        .expect("notices");
        assert!(notices
            .iter()
            .any(|notice| notice.contains("tighten config `wide_cfg`")));
        let _ = std::fs::remove_dir_all(store_dir);
    }

    #[test]
    fn stores_and_recovers_payload() {
        let store_dir = temp_store_dir("unit-test-store");
        let payload = r#"{"hello":"world"}"#;

        let ref_id =
            store_original_payload(payload, &store_dir).expect("expected store to succeed");
        let recovered = recover_original_payload(&ref_id, &store_dir).expect("expected recovery");

        assert_eq!(recovered, payload);
        let _ = std::fs::remove_dir_all(store_dir);
    }

    #[test]
    fn retrieves_requested_fields_from_object_payload() {
        let store_dir = temp_store_dir("unit-test-retrieve-object");
        let payload = r#"{"users":[{"age":30,"address":{"city":"Austin","state":"TX"},"name":"Ada"}],"total":1}"#;
        let ref_id =
            store_original_payload(payload, &store_dir).expect("expected store to succeed");

        let retrieved = retrieve_original_payload(
            &ref_id,
            &store_dir,
            &["users[].age".to_string(), "users[].address".to_string()],
            None,
            false,
        )
        .expect("expected retrieve to succeed");

        assert_eq!(
            retrieved,
            serde_json::json!({
                "users": [
                    {
                        "age": 30,
                        "address": {
                            "city": "Austin",
                            "state": "TX"
                        }
                    }
                ]
            })
        );

        let _ = std::fs::remove_dir_all(store_dir);
    }

    #[test]
    fn retrieves_array_item_by_index() {
        let value = parse_json_payload(r#"[{"name":"first","age":1},{"name":"second","age":2}]"#)
            .expect("expected structured json");

        let retrieved = retrieve_json_payload(&value, &["name".to_string()], Some(1), false)
            .expect("expected retrieval");

        assert_eq!(retrieved, serde_json::json!({"name":"second"}));
    }

    #[test]
    fn retrieves_all_items_from_array() {
        let value = parse_json_payload(r#"[{"name":"first","age":1},{"name":"second","age":2}]"#)
            .expect("expected structured json");

        let retrieved = retrieve_json_payload(&value, &["name".to_string()], None, true)
            .expect("expected retrieval");

        assert_eq!(
            retrieved,
            serde_json::json!([
                {"name":"first"},
                {"name":"second"}
            ])
        );
    }

    #[test]
    fn retrieves_nested_array_item_by_index_path() {
        let value = parse_json_payload(
            r#"{"users":[{"firstName":"Ada","lastName":"Lovelace"},{"firstName":"Grace","lastName":"Hopper"}]}"#,
        )
        .expect("expected structured json");

        let retrieved =
            retrieve_json_payload(&value, &["users[0].firstName".to_string()], None, false)
                .expect("expected retrieval");

        assert_eq!(
            retrieved,
            serde_json::json!({
                "users": [
                    {
                        "firstName": "Ada"
                    }
                ]
            })
        );
    }

    #[test]
    fn stores_same_payload_as_distinct_runs() {
        let store_dir = temp_store_dir("unit-test-store-duplicate-runs");
        let _ = std::fs::remove_dir_all(&store_dir);
        let payload = r#"{"hello":"world"}"#;

        let first_ref =
            store_original_payload(payload, &store_dir).expect("expected first store to succeed");
        let second_ref =
            store_original_payload(payload, &store_dir).expect("expected second store to succeed");

        assert_ne!(first_ref, second_ref);
        assert!(recover_original_payload(&first_ref, &store_dir).is_ok());
        assert!(recover_original_payload(&second_ref, &store_dir).is_ok());

        let refs_dir = store_dir.join("refs");
        let refs_count = std::fs::read_dir(&refs_dir)
            .expect("expected refs dir")
            .count();
        assert_eq!(refs_count, 2);

        let _ = std::fs::remove_dir_all(store_dir);
    }

    #[test]
    fn stores_retention_metadata_in_index() {
        let store_dir = temp_store_dir("unit-test-store-retention");
        let payload = r#"{"hello":"world"}"#;

        let ref_id = store_original_payload_with_retention(payload, &store_dir, Some(7))
            .expect("expected store to succeed");
        let index_text =
            std::fs::read_to_string(store_dir.join("index.json")).expect("expected index");
        let index: serde_json::Value = serde_json::from_str(&index_text).expect("expected json");
        let entry = &index[&ref_id];

        assert_eq!(entry["retention_days"], 7);
        assert!(entry["created_at_unix_ms"].as_u64().is_some());
        assert!(entry["expires_at_unix_ms"].as_u64().is_some());
        let _ = std::fs::remove_dir_all(store_dir);
    }

    #[test]
    fn cleanup_removes_expired_payloads() {
        let store_dir = temp_store_dir("unit-test-cleanup");
        let expired_payload = r#"{"expired":true}"#;
        let active_payload = r#"{"active":true}"#;

        let expired_ref =
            store_original_payload_with_retention(expired_payload, &store_dir, Some(0))
                .expect("expected expired store to succeed");
        let active_ref = store_original_payload_with_retention(active_payload, &store_dir, Some(7))
            .expect("expected active store to succeed");

        let report = cleanup_expired_payloads(&store_dir).expect("expected cleanup");

        assert_eq!(report.removed_count, 1);
        assert_eq!(report.remaining_count, 1);
        assert!(recover_original_payload(&active_ref, &store_dir).is_ok());
        assert!(recover_original_payload(&expired_ref, &store_dir).is_err());
        let _ = std::fs::remove_dir_all(store_dir);
    }

    #[test]
    fn filters_nested_array_fields_with_object_prefix() {
        let value = parse_json_payload(
            r#"{"data":[{"id":"a","name":"alpha","secret":"x"}],"nextCursor":"abc"}"#,
        )
        .expect("expected structured json");

        let config = FilterConfig {
            id: None,
            name: None,
            source: None,
            request: None,
            notes: None,
            content_path: None,
            allow: vec!["data[].id".to_string(), "nextCursor".to_string()],
        };

        let filtered =
            filter_json_payload_with_metadata(&value, &config).expect("expected filtered json");
        assert_eq!(
            filtered,
            serde_json::json!({
                "data": [
                    {
                        "id": "a"
                    }
                ],
                "nextCursor": "abc",
                "_dtk": {
                    "available_fields": [
                        "data",
                        "data[]",
                        "data[].id",
                        "data[].name",
                        "data[].secret",
                        "nextCursor"
                    ],
                    "content_path": "data",
                    "root_kind": "object",
                    "store_hint": "local"
                }
            })
        );
    }

    #[test]
    fn preview_lists_expired_payloads_without_removing() {
        let store_dir = temp_store_dir("unit-test-preview");
        let payload = r#"{"expired":true}"#;

        let ref_id = store_original_payload_with_retention(payload, &store_dir, Some(0))
            .expect("expected store to succeed");
        let preview = preview_expired_payloads(&store_dir).expect("expected preview");

        assert_eq!(preview.expired_ref_ids, vec![ref_id.clone()]);
        assert_eq!(preview.remaining_count, 1);
        assert!(recover_original_payload(&ref_id, &store_dir).is_ok());
        let _ = std::fs::remove_dir_all(store_dir);
    }

    #[test]
    fn default_store_dir_is_user_scoped() {
        let store_dir = default_store_dir();
        let data_root = platform_data_dir();
        assert!(store_dir.starts_with(&data_root));
    }

    #[test]
    fn runtime_store_dir_honors_explicit_override() {
        let store_dir = temp_store_dir("runtime-store-override");
        std::env::set_var("DTK_STORE_DIR", &store_dir);

        let resolved = runtime_store_dir();

        assert_eq!(resolved, store_dir);

        std::env::remove_var("DTK_STORE_DIR");
        let _ = std::fs::remove_dir_all(store_dir);
    }

    #[test]
    fn platform_data_dir_selects_expected_root() {
        let data_dir = platform_data_dir();
        if cfg!(windows) {
            assert!(data_dir.starts_with(windows_data_dir()) || data_dir == PathBuf::from("."));
        } else {
            assert_eq!(data_dir, xdg_data_dir());
        }
    }

    #[test]
    fn loads_config_metadata_and_rules() {
        let config = serde_json::from_value::<FilterConfig>(serde_json::json!({
            "name": "n8n_workflows_list",
            "source": "n8n",
            "request": "curl -sS ...",
            "notes": "workflow list",
            "allow": ["data[].id", "nextCursor"]
        }))
        .expect("expected config to deserialize");

        assert_eq!(config.name.as_deref(), Some("n8n_workflows_list"));
        assert_eq!(config.source.as_deref(), Some("n8n"));
        assert_eq!(config.allow, vec!["data[].id", "nextCursor"]);
    }

    #[test]
    fn codex_agents_normalization_drops_markers_and_keeps_plain_lines() {
        let existing = "@/home/walter/.codex/RTK.md\n\n<!-- DTK-START -->\n@/home/walter/.codex/DTK.md\n<!-- DTK-END -->\n";
        let next = super::normalize_codex_agents_content(
            existing,
            Some("@/home/walter/.codex/DTK.md"),
            None,
        )
        .expect("expected content");
        assert_eq!(
            next,
            "@/home/walter/.codex/RTK.md\n@/home/walter/.codex/DTK.md\n"
        );
    }

    #[test]
    fn codex_agents_normalization_removes_target_line_on_uninstall() {
        let existing = "@/home/walter/.codex/RTK.md\n@/home/walter/.codex/DTK.md\n";
        let next = super::normalize_codex_agents_content(
            existing,
            None,
            Some("@/home/walter/.codex/DTK.md"),
        )
        .expect("expected content");
        assert_eq!(next, "@/home/walter/.codex/RTK.md\n");
    }
}
