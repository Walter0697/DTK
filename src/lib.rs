#[path = "lib/config.rs"]
mod config;
#[path = "lib/filter.rs"]
mod filter;
#[path = "lib/install.rs"]
mod install;
#[path = "lib/paths.rs"]
mod paths;
#[path = "lib/store.rs"]
mod store;
#[path = "lib/usage.rs"]
mod usage;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use std::io;
use std::path::Path;
use std::sync::atomic::AtomicU64;

use sha2::{Digest, Sha256};

const DTK_GUIDE: &str = include_str!("../DTK.md");
const DTK_CONFIG_ASSISTANT_SKILL: &str = include_str!("../skills/dtk/SKILL.md");
const DUMMYJSON_USERS_CONFIG: &str = include_str!("../samples/config.dummyjson.users.json");
const CARGO_LOCK_PACKAGES_CONFIG: &str =
    include_str!("../samples/config.cargo_lock_packages.toml.json");
const CARGO_LOCK_PACKAGES_PAYLOAD: &str =
    include_str!("../samples/payload.cargo_lock_packages.toml");
const PYPROJECT_MANIFEST_CONFIG: &str =
    include_str!("../samples/config.pyproject_manifest.toml.json");
const PYPROJECT_MANIFEST_PAYLOAD: &str = include_str!("../samples/payload.pyproject_manifest.toml");
const CSV_INVENTORY_EXPORT_CONFIG: &str =
    include_str!("../samples/config.csv_inventory_export.csv.json");
const CSV_INVENTORY_EXPORT_PAYLOAD: &str =
    include_str!("../samples/payload.csv_inventory_export.csv");
const XAML_RESOURCE_DICTIONARY_CONFIG: &str =
    include_str!("../samples/config.xaml_resource_dictionary.xaml.json");
const XAML_RESOURCE_DICTIONARY_PAYLOAD: &str =
    include_str!("../samples/payload.xaml_resource_dictionary.xaml");
const KUBERNETES_DEPLOYMENT_YAML_CONFIG: &str =
    include_str!("../samples/config.kubernetes.deployment.yaml.json");
const KUBERNETES_DEPLOYMENT_YAML_PAYLOAD: &str =
    include_str!("../samples/payload.kubernetes.deployment.yaml");
const DEFAULT_USAGE_RETENTION_DAYS: u64 = 30;
const USAGE_SCHEMA_VERSION: i32 = 2;
pub const DEFAULT_SAMPLE_CONFIG_NAME: &str = "dummyjson_users.json";
pub const CARGO_LOCK_SAMPLE_CONFIG_NAME: &str = "cargo_lock_packages.toml.json";
pub const PYPROJECT_SAMPLE_CONFIG_NAME: &str = "pyproject_manifest.toml.json";
pub const CSV_INVENTORY_EXPORT_SAMPLE_CONFIG_NAME: &str = "csv_inventory_export.csv.json";
pub const XAML_RESOURCE_DICTIONARY_SAMPLE_CONFIG_NAME: &str = "xaml_resource_dictionary.xaml.json";
pub const YAML_SAMPLE_CONFIG_NAME: &str = "kubernetes_deployment.yaml.json";
static STORE_REF_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static SESSION_TICKET_SEQUENCE: AtomicU64 = AtomicU64::new(0);

pub use config::{
    add_or_update_hook_rule, load_filter_config, load_hook_rules, remove_hook_rules_for_config,
    resolve_config_path, resolve_filter_config_id, write_filter_config, write_hook_rules,
};
use filter::normalize_repeated_field_path;
pub use filter::{
    collect_field_paths, field_is_allowlisted, filter_json_payload,
    filter_json_payload_with_metadata, filter_json_payload_with_ref,
    normalize_field_path_for_config, retrieve_json_payload,
};
#[cfg(test)]
use install::normalize_codex_agents_content;
pub use install::{
    claude_dir, codex_dir, cursor_dir, install_agent_guidance,
    install_agent_guidance_with_dummy_samples, install_config_skill, uninstall_agent_guidance,
};
pub use paths::{
    default_config_dir, default_store_dir, default_usage_dir, filtered_payload_path,
    platform_config_dir, platform_data_dir, runtime_store_dir, runtime_usage_dir, usage_db_path,
    windows_config_dir, windows_data_dir, xdg_config_dir, xdg_data_dir,
};
pub use store::{
    cleanup_expired_payloads, preview_expired_payloads, read_store_index, recover_original_payload,
    retrieve_original_payload, store_filtered_payload, store_original_payload,
    store_original_payload_with_retention, CleanupPreview, CleanupReport,
};
pub use usage::{
    end_session, init_usage_schema, load_config_recommendations, recommendation_notices_for_exec,
    recommendation_notices_for_retrieve, record_exec_metric_issue, record_exec_metrics,
    record_field_access, start_session, UsageCleanupReport,
};
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
    pub format: Option<String>,
    #[serde(default)]
    pub content_path: Option<String>,
    #[serde(default)]
    pub allow: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StructuredFormat {
    Json,
    Yaml,
    Toml,
    Csv,
    Xaml,
}

impl StructuredFormat {
    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "json" => Some(Self::Json),
            "yaml" | "yml" => Some(Self::Yaml),
            "toml" => Some(Self::Toml),
            "csv" => Some(Self::Csv),
            "xaml" | "xml" => Some(Self::Xaml),
            _ => None,
        }
    }
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

pub fn is_structured_payload(text: &str) -> bool {
    parse_structured_payload(text).is_some()
}

pub fn parse_structured_format(value: &str) -> Option<StructuredFormat> {
    StructuredFormat::parse(value)
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

pub fn parse_structured_payload(text: &str) -> Option<Value> {
    parse_structured_payload_with_hint(text, None)
}

pub fn parse_structured_payload_with_hint(
    text: &str,
    format: Option<StructuredFormat>,
) -> Option<Value> {
    match format {
        Some(StructuredFormat::Json) => parse_json_payload(text),
        Some(StructuredFormat::Yaml) => parse_yaml_payload(text),
        Some(StructuredFormat::Toml) => parse_toml_payload(text),
        Some(StructuredFormat::Csv) => parse_csv_payload(text),
        Some(StructuredFormat::Xaml) => parse_xaml_payload(text),
        None => parse_json_payload(text)
            .or_else(|| parse_yaml_payload(text))
            .or_else(|| parse_toml_payload(text))
            .or_else(|| parse_xaml_payload(text))
            .or_else(|| parse_csv_payload(text)),
    }
}

fn parse_yaml_payload(text: &str) -> Option<Value> {
    let stripped = text.trim();
    if stripped.is_empty() {
        return None;
    }

    match serde_yaml::from_str::<Value>(stripped) {
        Ok(value @ Value::Object(_)) | Ok(value @ Value::Array(_)) => Some(value),
        Ok(_) => None,
        Err(_) => None,
    }
}

fn parse_toml_payload(text: &str) -> Option<Value> {
    let stripped = text.trim();
    if stripped.is_empty() {
        return None;
    }

    match toml::from_str::<toml::Value>(stripped) {
        Ok(value) => toml_value_to_json(value),
        Err(_) => None,
    }
}

fn parse_csv_payload(text: &str) -> Option<Value> {
    let stripped = text.trim();
    if stripped.is_empty() {
        return None;
    }

    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_reader(stripped.as_bytes());
    let headers = reader.headers().ok()?.clone();
    if headers.is_empty() {
        return None;
    }

    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record.ok()?;
        let mut row = serde_json::Map::new();

        for (index, header) in headers.iter().enumerate() {
            let value = record.get(index).unwrap_or_default();
            row.insert(header.to_string(), Value::String(value.to_string()));
        }

        rows.push(Value::Object(row));
    }

    if rows.is_empty() {
        return None;
    }

    Some(Value::Object(serde_json::Map::from_iter([(
        "rows".to_string(),
        Value::Array(rows),
    )])))
}

fn toml_value_to_json(value: toml::Value) -> Option<Value> {
    match value {
        toml::Value::String(value) => Some(Value::String(value)),
        toml::Value::Integer(value) => Some(Value::Number(value.into())),
        toml::Value::Float(value) => serde_json::Number::from_f64(value).map(Value::Number),
        toml::Value::Boolean(value) => Some(Value::Bool(value)),
        toml::Value::Datetime(value) => Some(Value::String(value.to_string())),
        toml::Value::Array(values) => values
            .into_iter()
            .map(toml_value_to_json)
            .collect::<Option<Vec<_>>>()
            .map(Value::Array),
        toml::Value::Table(table) => table
            .into_iter()
            .map(|(key, value)| toml_value_to_json(value).map(|json| (key, json)))
            .collect::<Option<serde_json::Map<_, _>>>()
            .map(Value::Object),
    }
}

fn parse_xaml_payload(text: &str) -> Option<Value> {
    let stripped = text.trim();
    if stripped.is_empty() {
        return None;
    }

    let element = xmltree::Element::parse(stripped.as_bytes()).ok()?;
    let value = xml_element_to_json(&element)?;
    Some(Value::Object(serde_json::Map::from_iter([(
        normalize_xml_name(&element.name),
        value,
    )])))
}

fn xml_element_to_json(element: &xmltree::Element) -> Option<Value> {
    let mut map = serde_json::Map::new();

    for (key, value) in &element.attributes {
        if key.starts_with("xmlns") {
            continue;
        }

        map.insert(normalize_xml_name(key), Value::String(value.clone()));
    }

    if let Some(text) = element.get_text() {
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            map.insert("text".to_string(), Value::String(trimmed.to_string()));
        }
    }

    let mut grouped_children: std::collections::BTreeMap<String, Vec<Value>> =
        std::collections::BTreeMap::new();
    for child in &element.children {
        let xmltree::XMLNode::Element(child_element) = child else {
            continue;
        };

        let child_value = xml_element_to_json(child_element)?;
        grouped_children
            .entry(normalize_xml_name(&child_element.name))
            .or_default()
            .push(child_value);
    }

    for (name, values) in grouped_children {
        if values.len() == 1 {
            map.insert(name, values.into_iter().next().unwrap_or(Value::Null));
        } else {
            map.insert(name, Value::Array(values));
        }
    }

    Some(Value::Object(map))
}

fn normalize_xml_name(name: &str) -> String {
    name.trim()
        .replace(':', "_")
        .replace('.', "_")
        .replace('/', "_")
}

pub fn stable_ref_id(raw_payload: &str) -> Option<String> {
    let value = parse_structured_payload(raw_payload)?;
    let canonical = serde_json::to_string(&value).ok()?;
    let mut hasher = Sha256::new();
    hasher.update(canonical.as_bytes());
    let digest = hasher.finalize();
    Some(format!("dtk_{}", hex_string(&digest[..16])))
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

fn hex_string(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

#[cfg(test)]
#[path = "lib/tests.rs"]
mod tests;
