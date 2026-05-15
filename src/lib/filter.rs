use crate::FilterConfig;
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

pub fn normalize_field_path_for_config(field_path: &str, config: &FilterConfig) -> Option<String> {
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

pub(crate) fn normalize_repeated_field_path(field: &str) -> Option<String> {
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
