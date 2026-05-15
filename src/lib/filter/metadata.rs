use super::patterns::{render_field_path, PathSegment};
use crate::FilterConfig;
use serde_json::Value;

pub(super) fn apply_filter_metadata(
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
