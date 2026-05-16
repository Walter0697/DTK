use super::template::{json_object_or_array, parse_trimmed, StructuredParser};
use serde_json::Value;

pub(crate) struct JsonParser;

impl StructuredParser for JsonParser {
    fn parse(text: &str) -> Option<Value> {
        parse_trimmed(text, |stripped| {
            serde_json::from_str::<Value>(stripped)
                .ok()
                .and_then(json_object_or_array)
        })
    }
}

pub(crate) fn parse(text: &str) -> Option<Value> {
    JsonParser::parse(text)
}
