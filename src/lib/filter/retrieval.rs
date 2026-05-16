use super::patterns::{PathPattern, PathSegment};
use serde_json::Value;

pub fn collect_field_paths(value: &Value) -> Vec<String> {
    let mut paths = Vec::new();
    collect_field_paths_inner(value, "", &mut paths);
    paths.sort();
    paths
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
