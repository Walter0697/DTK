use super::metadata::apply_filter_metadata;
use super::patterns::{normalize_path_pattern_for_config, PathPattern, PathSegment};
use super::pii::apply_pii_transform;
use crate::{FilterConfig, StructuredFormat};
use serde_json::Value;

pub fn field_is_allowlisted(config: &FilterConfig, field_path: &str) -> bool {
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
    let filtered = apply_pii_transform(&filter_json_payload(value, config)?, config);
    Some(apply_filter_metadata(value, &filtered, None, Some(config), None))
}

pub fn filter_json_payload_with_ref(
    value: &Value,
    config: &FilterConfig,
    ref_id: &str,
) -> Option<Value> {
    filter_json_payload_with_ref_and_format(value, config, ref_id, None)
}

pub fn filter_json_payload_with_ref_and_format(
    value: &Value,
    config: &FilterConfig,
    ref_id: &str,
    format: Option<StructuredFormat>,
) -> Option<Value> {
    let filtered = apply_pii_transform(&filter_json_payload(value, config)?, config);
    Some(apply_filter_metadata(
        value,
        &filtered,
        Some(ref_id),
        Some(config),
        format,
    ))
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
