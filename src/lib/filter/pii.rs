use super::patterns::{
    normalize_path_pattern_for_config, render_field_path, PathPattern, PathSegment,
};
use crate::{FilterConfig, PiiAction, PiiRule, PiiUuidMethod};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static PII_RANDOM_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
struct CompiledPiiRule {
    pattern: PathPattern,
    rule: PiiRule,
    order: usize,
    specificity: RuleSpecificity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RuleSpecificity {
    descendant_wildcards: usize,
    wildcard_segments: usize,
    pattern_len: usize,
}

#[derive(Debug, Clone)]
struct TemplateContext {
    value: String,
    path: String,
    hash: String,
    uuid: String,
    sources: BTreeMap<String, String>,
}

pub fn apply_pii_transform(value: &Value, config: &FilterConfig) -> Value {
    let rules = compile_rules(config);
    if rules.is_empty() {
        return value.clone();
    }

    transform_value(value, &[], None, &rules)
}

fn compile_rules(config: &FilterConfig) -> Vec<CompiledPiiRule> {
    config
        .pii
        .iter()
        .enumerate()
        .filter_map(|(order, rule)| {
            let pattern =
                normalize_path_pattern_for_config(PathPattern::parse(&rule.path), config)?;
            Some(CompiledPiiRule {
                specificity: rule_specificity(&pattern),
                pattern,
                rule: rule.clone(),
                order,
            })
        })
        .collect()
}

fn rule_specificity(pattern: &PathPattern) -> RuleSpecificity {
    let descendant_wildcards = pattern
        .segments
        .iter()
        .filter(|segment| matches!(segment, PathSegment::AnyDescendant))
        .count();
    let wildcard_segments = pattern
        .segments
        .iter()
        .filter(|segment| matches!(segment, PathSegment::AnyIndex | PathSegment::AnyDescendant))
        .count();

    RuleSpecificity {
        descendant_wildcards,
        wildcard_segments,
        pattern_len: pattern.segments.len(),
    }
}

fn transform_value(
    value: &Value,
    current_path: &[PathSegment],
    parent_context: Option<&Value>,
    rules: &[CompiledPiiRule],
) -> Value {
    match value {
        Value::Object(map) => {
            let mut transformed = serde_json::Map::new();
            for (key, child) in map {
                let mut child_path = current_path.to_vec();
                child_path.push(PathSegment::Key(key.clone()));
                transformed.insert(
                    key.clone(),
                    transform_value(child, &child_path, Some(value), rules),
                );
            }
            Value::Object(transformed)
        }
        Value::Array(items) => {
            let mut transformed = Vec::with_capacity(items.len());
            for (index, child) in items.iter().enumerate() {
                let mut child_path = current_path.to_vec();
                child_path.push(PathSegment::Index(index));
                transformed.push(transform_value(child, &child_path, Some(value), rules));
            }
            Value::Array(transformed)
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            match select_best_rule(current_path, rules) {
                Some(rule) => transform_scalar(value, current_path, parent_context, rule),
                None => value.clone(),
            }
        }
    }
}

fn select_best_rule<'a>(
    current_path: &[PathSegment],
    rules: &'a [CompiledPiiRule],
) -> Option<&'a CompiledPiiRule> {
    let mut best: Option<&CompiledPiiRule> = None;

    for rule in rules {
        if !rule.pattern.covers_path(current_path) {
            continue;
        }

        best = match best {
            Some(existing) if !rule_better(rule, existing) => Some(existing),
            _ => Some(rule),
        };
    }

    best
}

fn rule_better(left: &CompiledPiiRule, right: &CompiledPiiRule) -> bool {
    if left.specificity.descendant_wildcards != right.specificity.descendant_wildcards {
        return left.specificity.descendant_wildcards < right.specificity.descendant_wildcards;
    }
    if left.specificity.wildcard_segments != right.specificity.wildcard_segments {
        return left.specificity.wildcard_segments < right.specificity.wildcard_segments;
    }
    if left.specificity.pattern_len != right.specificity.pattern_len {
        return left.specificity.pattern_len > right.specificity.pattern_len;
    }

    left.order > right.order
}

fn transform_scalar(
    value: &Value,
    current_path: &[PathSegment],
    parent_context: Option<&Value>,
    rule: &CompiledPiiRule,
) -> Value {
    match rule.rule.action {
        PiiAction::Mask => Value::String(
            rule.rule
                .replacement
                .clone()
                .unwrap_or_else(|| "[PII INFORMATION]".to_string()),
        ),
        PiiAction::Uuid => Value::String(render_uuid_value(value, current_path, rule)),
        PiiAction::Replace => Value::String(render_replace_value(
            value,
            current_path,
            parent_context,
            rule,
        )),
    }
}

fn render_uuid_value(
    value: &Value,
    current_path: &[PathSegment],
    rule: &CompiledPiiRule,
) -> String {
    let raw_value = scalar_to_string(value);
    let path = render_field_path(current_path);
    let base_seed = format!("{}|{}|{}", rule.rule.path.trim(), path, raw_value);
    let deterministic_uuid = uuid_like_from_seed(&base_seed);

    match rule.rule.method.clone().unwrap_or(PiiUuidMethod::Default) {
        PiiUuidMethod::Default => deterministic_uuid,
        PiiUuidMethod::Random => {
            let sequence = PII_RANDOM_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or(0);
            let random_seed = format!("{base_seed}|{now}|{sequence}");
            uuid_like_from_seed(&random_seed)
        }
        PiiUuidMethod::Template => {
            let context = TemplateContext {
                value: raw_value,
                path,
                hash: hash_hex(&base_seed),
                uuid: deterministic_uuid,
                sources: BTreeMap::new(),
            };
            render_template(rule.rule.template.as_deref().unwrap_or("{uuid}"), &context)
        }
    }
}

fn render_replace_value(
    value: &Value,
    current_path: &[PathSegment],
    parent_context: Option<&Value>,
    rule: &CompiledPiiRule,
) -> String {
    let raw_value = scalar_to_string(value);
    let path = render_field_path(current_path);
    let base_seed = format!("{}|{}|{}", rule.rule.path.trim(), path, raw_value);
    let sources = resolve_source_fields(parent_context, &rule.rule.source_fields);

    let context = TemplateContext {
        value: raw_value,
        path,
        hash: hash_hex(&base_seed),
        uuid: uuid_like_from_seed(&base_seed),
        sources,
    };

    let template = rule.rule.template.as_deref().unwrap_or_default();
    if template.trim().is_empty() {
        if context.sources.is_empty() {
            context.value
        } else {
            context
                .sources
                .values()
                .cloned()
                .collect::<Vec<_>>()
                .join(" ")
        }
    } else {
        render_template(template, &context)
    }
}

fn resolve_source_fields(
    parent_context: Option<&Value>,
    source_fields: &[String],
) -> BTreeMap<String, String> {
    let mut resolved = BTreeMap::new();
    let Some(parent_context) = parent_context else {
        return resolved;
    };

    for field in source_fields {
        let key = field.trim();
        if key.is_empty() {
            continue;
        }
        if let Some(value) = resolve_relative_value(parent_context, key) {
            resolved.insert(key.to_string(), value);
        }
    }

    resolved
}

fn resolve_relative_value(value: &Value, field: &str) -> Option<String> {
    let pattern = PathPattern::parse(field);
    let resolved = resolve_pattern_value(value, &pattern.segments)?;
    Some(scalar_to_string(&resolved))
}

fn resolve_pattern_value<'a>(value: &'a Value, segments: &[PathSegment]) -> Option<&'a Value> {
    let mut current = value;
    for segment in segments {
        current = match (segment, current) {
            (PathSegment::Key(key), Value::Object(map)) => map.get(key)?,
            (PathSegment::Index(index), Value::Array(items)) => items.get(*index)?,
            (PathSegment::AnyIndex, Value::Array(items)) => items.first()?,
            (PathSegment::AnyDescendant, _) => return Some(current),
            _ => return None,
        };
    }
    Some(current)
}

fn render_template(template: &str, context: &TemplateContext) -> String {
    let mut output = String::new();
    let mut chars = template.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '{' => {
                if chars.peek() == Some(&'{') {
                    chars.next();
                    output.push('{');
                    continue;
                }

                let mut placeholder = String::new();
                while let Some(next) = chars.next() {
                    if next == '}' {
                        break;
                    }
                    placeholder.push(next);
                }
                output.push_str(&render_placeholder(&placeholder, context));
            }
            '}' => {
                if chars.peek() == Some(&'}') {
                    chars.next();
                    output.push('}');
                } else {
                    output.push('}');
                }
            }
            other => output.push(other),
        }
    }

    output
}

fn render_placeholder(placeholder: &str, context: &TemplateContext) -> String {
    let (name, format_spec) = placeholder
        .split_once(':')
        .map(|(name, spec)| (name.trim(), Some(spec.trim())))
        .unwrap_or((placeholder.trim(), None));

    match name {
        "value" => format_value_placeholder(&context.value, format_spec),
        "path" => context.path.clone(),
        "hash" => context.hash.clone(),
        "uuid" => context.uuid.clone(),
        other => context
            .sources
            .get(other)
            .cloned()
            .unwrap_or_else(|| format!("{{{placeholder}}}")),
    }
}

fn format_value_placeholder(value: &str, format_spec: Option<&str>) -> String {
    let Some(spec) = format_spec else {
        return value.to_string();
    };

    let width = spec.parse::<usize>().ok();
    if let Some(width) = width {
        if let Ok(parsed) = value.parse::<i128>() {
            return format!("{parsed:0width$}");
        }
        if let Ok(parsed) = value.parse::<u128>() {
            return format!("{parsed:0width$}");
        }
        if let Some(stripped) = value.strip_prefix('-') {
            if let Ok(parsed) = stripped.parse::<u128>() {
                return format!("-{:0width$}", parsed);
            }
        }
    }

    value.to_string()
}

fn scalar_to_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null => "null".to_string(),
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

fn uuid_like_from_seed(seed: &str) -> String {
    let digest = Sha256::digest(seed.as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;

    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    )
}

fn hash_hex(seed: &str) -> String {
    let digest = Sha256::digest(seed.as_bytes());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}
