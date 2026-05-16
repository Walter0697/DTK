use super::template::{parse_scalar_value, parse_trimmed, push_named_value, StructuredParser};
use crate::StructuredFormat;
use serde_json::Value;

pub(crate) struct IniParser;

impl StructuredParser for IniParser {
    const FORMAT: StructuredFormat = StructuredFormat::Ini;

    fn parse(text: &str) -> Option<Value> {
        parse_trimmed(text, parse_ini_payload)
    }
}

pub(crate) fn parse(text: &str) -> Option<Value> {
    IniParser::parse(text)
}

fn parse_ini_payload(stripped: &str) -> Option<Value> {
    let mut root = serde_json::Map::new();
    let mut current_section_name: Option<String> = None;
    let mut current_section = serde_json::Map::new();
    let mut saw_content = false;

    for raw_line in stripped.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with(';') || line.starts_with('#') {
            continue;
        }

        if let Some(section_name) = parse_ini_section_name(line) {
            if let Some(previous_section_name) = current_section_name.replace(section_name) {
                push_named_value(
                    &mut root,
                    &previous_section_name,
                    Value::Object(std::mem::take(&mut current_section)),
                );
            }
            saw_content = true;
            continue;
        }

        let Some((key, value)) = split_ini_assignment(line) else {
            return None;
        };
        let target = if current_section_name.is_some() {
            &mut current_section
        } else {
            &mut root
        };
        push_named_value(target, key, parse_scalar_value(value));
        saw_content = true;
    }

    if let Some(section_name) = current_section_name {
        push_named_value(
            &mut root,
            &section_name,
            Value::Object(std::mem::take(&mut current_section)),
        );
    }

    if !saw_content || root.is_empty() {
        return None;
    }

    Some(Value::Object(root))
}

fn parse_ini_section_name(line: &str) -> Option<String> {
    let trimmed = line.trim();
    let name = trimmed.strip_prefix('[')?.strip_suffix(']')?.trim();
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
    }
}

fn split_ini_assignment(line: &str) -> Option<(&str, &str)> {
    let equals_index = line.find('=');
    let colon_index = line.find(':');

    let split_index = match (equals_index, colon_index) {
        (Some(eq), Some(colon)) => Some(eq.min(colon)),
        (Some(eq), None) => Some(eq),
        (None, Some(colon)) => Some(colon),
        (None, None) => None,
    }?;

    let (key, value) = line.split_at(split_index);
    let value = value.get(1..)?;
    let key = key.trim();
    let value = value.trim();

    if key.is_empty() {
        None
    } else {
        Some((key, value))
    }
}
