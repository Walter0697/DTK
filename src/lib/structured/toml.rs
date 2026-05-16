use super::template::{parse_trimmed, StructuredParser};
use serde_json::Value;

pub(crate) struct TomlParser;

impl StructuredParser for TomlParser {
    fn parse(text: &str) -> Option<Value> {
        parse_trimmed(text, |stripped| {
            toml::from_str::<toml::Value>(stripped)
                .ok()
                .and_then(toml_value_to_json)
        })
    }
}

pub(crate) fn parse(text: &str) -> Option<Value> {
    TomlParser::parse(text)
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
