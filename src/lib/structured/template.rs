use serde_json::Value;

pub(crate) trait StructuredParser {
    fn parse(text: &str) -> Option<Value>;
}

pub(crate) fn parse_trimmed(
    text: &str,
    parse: impl FnOnce(&str) -> Option<Value>,
) -> Option<Value> {
    let stripped = text.trim();
    if stripped.is_empty() {
        None
    } else {
        parse(stripped)
    }
}

pub(crate) fn json_object_or_array(value: Value) -> Option<Value> {
    match value {
        Value::Object(_) | Value::Array(_) => Some(value),
        _ => None,
    }
}

pub(crate) fn push_named_value(map: &mut serde_json::Map<String, Value>, key: &str, value: Value) {
    match map.entry(key.to_string()) {
        serde_json::map::Entry::Vacant(entry) => {
            entry.insert(value);
        }
        serde_json::map::Entry::Occupied(mut entry) => match entry.get_mut() {
            Value::Array(values) => values.push(value),
            existing => {
                let previous = std::mem::replace(existing, Value::Null);
                *existing = Value::Array(vec![previous, value]);
            }
        },
    }
}

pub(crate) fn parse_scalar_value(value: &str) -> Value {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Value::String(String::new());
    }

    if (trimmed.starts_with('"') && trimmed.ends_with('"'))
        || (trimmed.starts_with('\'') && trimmed.ends_with('\''))
    {
        return Value::String(trimmed[1..trimmed.len().saturating_sub(1)].to_string());
    }

    if trimmed.eq_ignore_ascii_case("true") {
        return Value::Bool(true);
    }

    if trimmed.eq_ignore_ascii_case("false") {
        return Value::Bool(false);
    }

    if let Ok(value) = trimmed.parse::<i64>() {
        return Value::Number(value.into());
    }

    if let Ok(value) = trimmed.parse::<f64>() {
        if let Some(number) = serde_json::Number::from_f64(value) {
            return Value::Number(number);
        }
    }

    Value::String(trimmed.to_string())
}
