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
const DTK_SKILL: &str = include_str!("../skills/dtk/SKILL.md");
const DUMMYJSON_USERS_CONFIG: &str = include_str!("../samples/config.dummyjson.users.json");
pub const DEFAULT_SAMPLE_CONFIG_NAME: &str = "dummyjson_users.json";
static STORE_REF_SEQUENCE: AtomicU64 = AtomicU64::new(0);
#[derive(Debug, Clone, Deserialize, Default)]
pub struct FilterConfig {
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

pub fn default_store_dir() -> PathBuf {
    std::env::var("DTK_STORE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| platform_data_dir().join("dtk").join("store"))
}

pub fn runtime_store_dir() -> PathBuf {
    default_store_dir()
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
    Some(apply_filter_metadata(value, &filtered, None))
}

pub fn filter_json_payload_with_ref(
    value: &Value,
    config: &FilterConfig,
    ref_id: &str,
) -> Option<Value> {
    let filtered = filter_json_payload(value, config)?;
    Some(apply_filter_metadata(value, &filtered, Some(ref_id)))
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

    fn matches_exact(&self, path: &[PathSegment]) -> bool {
        self.segments.len() == path.len()
            && self
                .segments
                .iter()
                .zip(path.iter())
                .all(|(pattern, actual)| segment_matches(pattern, actual))
    }

    fn path_is_prefix(&self, path: &[PathSegment]) -> bool {
        path.len() <= self.segments.len()
            && path
                .iter()
                .zip(self.segments.iter())
                .all(|(actual, pattern)| segment_matches(pattern, actual))
    }
}

fn segment_matches(pattern: &PathSegment, actual: &PathSegment) -> bool {
    match (pattern, actual) {
        (PathSegment::AnyIndex, PathSegment::AnyIndex) => true,
        (PathSegment::AnyIndex, PathSegment::Index(_)) => true,
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

fn merge_ref_id(metadata: Value, ref_id: Option<&str>) -> Value {
    let Some(ref_id) = ref_id else {
        return metadata;
    };

    match metadata {
        Value::Object(map) => {
            let mut map = map;
            map.insert("ref_id".to_string(), Value::String(ref_id.to_string()));
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
        DTK_SKILL,
    )
}

fn install_claude_skill() -> io::Result<bool> {
    install_text_file(
        claude_dir().join("skills").join("dtk").join("SKILL.md"),
        DTK_SKILL,
    )
}

fn install_cursor_skill() -> io::Result<bool> {
    install_text_file(
        cursor_dir().join("skills").join("dtk").join("SKILL.md"),
        DTK_SKILL,
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

fn apply_filter_metadata(original: &Value, filtered: &Value, ref_id: Option<&str>) -> Value {
    let metadata = surface_metadata(original);

    match filtered {
        Value::Object(map) => {
            let mut map = map.clone();
            map.insert("_dtk".to_string(), merge_ref_id(metadata, ref_id));
            Value::Object(map)
        }
        _ => serde_json::json!({
            "result": filtered,
            "_dtk": merge_ref_id(metadata, ref_id)
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

    if allow_active && !is_allowed_exact && !is_allowed_descendant && !is_root {
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
        cleanup_expired_payloads, collect_field_paths, default_store_dir, filter_json_payload,
        filter_json_payload_with_metadata, is_json_payload, parse_json_payload, platform_data_dir,
        preview_expired_payloads, recover_original_payload, retrieve_json_payload,
        retrieve_original_payload, runtime_store_dir, stable_ref_id, store_original_payload,
        store_original_payload_with_retention, windows_data_dir, xdg_data_dir, FilterConfig,
    };

    fn temp_store_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join("dtk-tests").join(name)
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
    fn stable_ref_id_is_deterministic() {
        let left = stable_ref_id(r#"{"a":1,"b":2}"#).expect("expected ref id");
        let right = stable_ref_id(r#"{"a":1,"b":2}"#).expect("expected ref id");
        assert_eq!(left, right);
        assert!(left.starts_with("dtk_"));
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
