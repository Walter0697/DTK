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

    transform_value(value, &[], None, &[], &rules, config)
}

pub fn field_is_pii_covered(config: &FilterConfig, field_path: &str) -> bool {
    let normalized =
        normalize_current_path_for_config(&PathPattern::parse(field_path).segments, config);

    let rules = compile_rules(config);
    rules
        .iter()
        .any(|rule| rule.pattern.covers_path(&normalized))
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
    ancestors: &[&Value],
    rules: &[CompiledPiiRule],
    config: &FilterConfig,
) -> Value {
    match value {
        Value::Object(map) => {
            let mut next_ancestors = ancestors.to_vec();
            next_ancestors.push(value);
            let mut transformed = serde_json::Map::new();
            for (key, child) in map {
                let mut child_path = current_path.to_vec();
                child_path.push(PathSegment::Key(key.clone()));
                transformed.insert(
                    key.clone(),
                    transform_value(
                        child,
                        &child_path,
                        Some(value),
                        &next_ancestors,
                        rules,
                        config,
                    ),
                );
            }
            Value::Object(transformed)
        }
        Value::Array(items) => {
            let mut next_ancestors = ancestors.to_vec();
            next_ancestors.push(value);
            let mut transformed = Vec::with_capacity(items.len());
            for (index, child) in items.iter().enumerate() {
                let mut child_path = current_path.to_vec();
                child_path.push(PathSegment::Index(index));
                transformed.push(transform_value(
                    child,
                    &child_path,
                    Some(value),
                    &next_ancestors,
                    rules,
                    config,
                ));
            }
            Value::Array(transformed)
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {
            match select_best_rule(current_path, config, rules) {
                Some(rule) => {
                    transform_scalar(value, current_path, parent_context, ancestors, rule, config)
                }
                None => value.clone(),
            }
        }
    }
}

fn select_best_rule<'a>(
    current_path: &[PathSegment],
    config: &FilterConfig,
    rules: &'a [CompiledPiiRule],
) -> Option<&'a CompiledPiiRule> {
    let normalized_current_path = normalize_current_path_for_config(current_path, config);
    let mut best: Option<&CompiledPiiRule> = None;

    for rule in rules {
        if !rule.pattern.covers_path(&normalized_current_path) {
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
    ancestors: &[&Value],
    rule: &CompiledPiiRule,
    config: &FilterConfig,
) -> Value {
    match rule.rule.action {
        PiiAction::Mask => Value::String(
            rule.rule
                .replacement
                .clone()
                .unwrap_or_else(|| "[PII INFORMATION]".to_string()),
        ),
        PiiAction::Uuid => Value::String(render_uuid_value(
            value,
            current_path,
            ancestors,
            rule,
            config,
        )),
        PiiAction::Replace => Value::String(render_replace_value(
            value,
            current_path,
            parent_context,
            ancestors,
            rule,
            config,
        )),
    }
}

fn render_uuid_value(
    value: &Value,
    current_path: &[PathSegment],
    ancestors: &[&Value],
    rule: &CompiledPiiRule,
    config: &FilterConfig,
) -> String {
    let raw_value = scalar_to_string(value);
    let normalized_current_path = normalize_current_path_for_config(current_path, config);
    let path = render_field_path(&normalized_current_path);
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
            render_template(
                rule.rule.template.as_deref().unwrap_or("{uuid}"),
                &context,
                ancestors,
                None,
            )
        }
    }
}

fn render_replace_value(
    value: &Value,
    current_path: &[PathSegment],
    parent_context: Option<&Value>,
    ancestors: &[&Value],
    rule: &CompiledPiiRule,
    config: &FilterConfig,
) -> String {
    let raw_value = scalar_to_string(value);
    let normalized_current_path = normalize_current_path_for_config(current_path, config);
    let path = render_field_path(&normalized_current_path);
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
        render_template(template, &context, ancestors, parent_context)
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

fn normalize_current_path_for_config(
    current_path: &[PathSegment],
    config: &FilterConfig,
) -> Vec<PathSegment> {
    let rendered = render_field_path(current_path);
    let normalized = match config
        .content_path
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(content_path) => {
            if let Some(remainder) = rendered.strip_prefix(content_path) {
                if remainder.is_empty() || remainder.starts_with('.') || remainder.starts_with('[')
                {
                    remainder.trim_start_matches('.').to_string()
                } else {
                    rendered
                }
            } else {
                rendered
            }
        }
        None => rendered,
    };

    PathPattern::parse(&normalized).segments
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

fn render_template(
    template: &str,
    context: &TemplateContext,
    ancestors: &[&Value],
    parent_context: Option<&Value>,
) -> String {
    let mut output = String::new();
    let mut chars = template.chars().peekable();

    while let Some(ch) = chars.next() {
        match ch {
            '{' | '[' => {
                let close = if ch == '{' { '}' } else { ']' };
                if chars.peek() == Some(&ch) {
                    chars.next();
                    output.push(ch);
                    continue;
                }

                let mut placeholder = String::new();
                while let Some(next) = chars.next() {
                    if next == close {
                        break;
                    }
                    placeholder.push(next);
                }
                output.push_str(&render_placeholder(
                    ch,
                    close,
                    &placeholder,
                    context,
                    ancestors,
                    parent_context,
                ));
            }
            '}' | ']' => {
                if chars.peek() == Some(&ch) {
                    chars.next();
                    output.push(ch);
                } else {
                    output.push(ch);
                }
            }
            other => output.push(other),
        }
    }

    output
}

fn render_placeholder(
    open: char,
    close: char,
    placeholder: &str,
    context: &TemplateContext,
    ancestors: &[&Value],
    parent_context: Option<&Value>,
) -> String {
    let placeholder = placeholder.trim();
    if placeholder.is_empty() {
        return format!("{open}{close}");
    }

    let mut parts = placeholder
        .split('|')
        .map(str::trim)
        .filter(|part| !part.is_empty());
    let Some(base) = parts.next() else {
        return format!("{open}{close}");
    };
    let filters: Vec<_> = parts.collect();

    let (name, format_spec) = base
        .split_once(':')
        .map(|(name, spec)| (name.trim(), Some(spec.trim())))
        .unwrap_or((base, None));

    let mut value = match name {
        "value" => Some(format_value_placeholder(&context.value, format_spec)),
        "path" => Some(context.path.clone()),
        "hash" => Some(context.hash.clone()),
        "uuid" => Some(context.uuid.clone()),
        other => context
            .sources
            .get(other)
            .cloned()
            .or_else(|| {
                ancestors
                    .iter()
                    .rev()
                    .find_map(|value| resolve_relative_value(value, other))
            })
            .or_else(|| parent_context.and_then(|value| resolve_relative_value(value, other)))
            .or_else(|| {
                if placeholder_matches_current_path(&context.path, other) {
                    Some(context.value.clone())
                } else {
                    None
                }
            }),
    };

    for filter in filters {
        let (filter_name, filter_spec) = filter
            .split_once(':')
            .map(|(name, spec)| (name.trim(), Some(spec.trim())))
            .unwrap_or((filter, None));

        if filter_name.eq_ignore_ascii_case("default") {
            let fallback = filter_spec.unwrap_or_default().to_string();
            if value.as_ref().map(|value| value.is_empty()).unwrap_or(true) {
                value = Some(fallback);
            }
            continue;
        }

        let Some(current) = value.as_deref() else {
            continue;
        };
        let Some(filtered) = apply_string_filter(current, filter_name, filter_spec) else {
            return format!("{open}{placeholder}{close}");
        };
        value = Some(filtered);
    }

    value.unwrap_or_else(|| format!("{open}{placeholder}{close}"))
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

fn apply_string_filter(value: &str, filter_name: &str, spec: Option<&str>) -> Option<String> {
    match filter_name.to_ascii_lowercase().as_str() {
        "lower" => Some(value.to_lowercase()),
        "upper" => Some(value.to_uppercase()),
        "trim" => Some(value.trim().to_string()),
        "kebab" => Some(join_words(split_words(value), "-")),
        "camel" => Some(to_camel_case(&split_words(value))),
        "snake" => Some(join_words(split_words(value), "_")),
        "substring" => apply_substring_filter(value, spec),
        _ => None,
    }
}

fn apply_substring_filter(value: &str, spec: Option<&str>) -> Option<String> {
    let spec = spec?;
    let mut parts = spec.split(',').map(str::trim);
    let start = parts.next()?.parse::<usize>().ok()?;
    let len = match parts.next() {
        Some(value) if !value.is_empty() => Some(value.parse::<usize>().ok()?),
        Some(_) => None,
        None => None,
    };
    if parts.next().is_some() {
        return None;
    }

    let chars: Vec<char> = value.chars().collect();
    let start = start.min(chars.len());
    let end = len
        .map(|len| (start + len).min(chars.len()))
        .unwrap_or(chars.len());
    Some(chars[start..end].iter().collect())
}

fn placeholder_matches_current_path(current_path: &str, placeholder: &str) -> bool {
    let current = PathPattern::parse(current_path);
    let target = PathPattern::parse(placeholder);
    !target.segments.is_empty() && current.segments.ends_with(&target.segments)
}

fn split_words(value: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut previous_is_lower_or_digit = false;

    for ch in value.chars() {
        if ch.is_alphanumeric() {
            let is_upper = ch.is_uppercase();
            if is_upper && previous_is_lower_or_digit && !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
            current.push(ch);
            previous_is_lower_or_digit = ch.is_lowercase() || ch.is_numeric();
        } else {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
            previous_is_lower_or_digit = false;
        }
    }

    if !current.is_empty() {
        words.push(current);
    }

    words
        .into_iter()
        .flat_map(|word| split_camel_token(&word))
        .filter(|word| !word.is_empty())
        .collect()
}

fn split_camel_token(word: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut chars = word.chars().peekable();

    while let Some(ch) = chars.next() {
        let next_is_lower = chars
            .peek()
            .map(|next| next.is_lowercase())
            .unwrap_or(false);
        let split_here = ch.is_uppercase()
            && !current.is_empty()
            && current
                .chars()
                .last()
                .is_some_and(|prev| prev.is_lowercase() || prev.is_numeric())
            && next_is_lower;
        if split_here {
            parts.push(std::mem::take(&mut current));
        }
        current.push(ch);
    }

    if !current.is_empty() {
        parts.push(current);
    }

    parts
}

fn join_words(words: Vec<String>, separator: &str) -> String {
    words
        .into_iter()
        .map(|word| word.to_lowercase())
        .collect::<Vec<_>>()
        .join(separator)
}

fn to_camel_case(words: &[String]) -> String {
    let mut rendered = String::new();

    for (index, word) in words.iter().enumerate() {
        let lower = word.to_lowercase();
        if index == 0 {
            rendered.push_str(&lower);
            continue;
        }

        let mut chars = lower.chars();
        if let Some(first) = chars.next() {
            rendered.extend(first.to_uppercase());
            rendered.push_str(chars.as_str());
        }
    }

    rendered
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
