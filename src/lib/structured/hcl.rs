use super::template::{parse_scalar_value, parse_trimmed, push_named_value, StructuredParser};
use crate::StructuredFormat;
use serde_json::Value;

pub(crate) struct HclParser;

impl StructuredParser for HclParser {
    const FORMAT: StructuredFormat = StructuredFormat::Hcl;

    fn parse(text: &str) -> Option<Value> {
        parse_trimmed(text, parse_hcl_payload)
    }
}

pub(crate) fn parse(text: &str) -> Option<Value> {
    HclParser::parse(text)
}

fn parse_hcl_payload(stripped: &str) -> Option<Value> {
    let mut lines = stripped.lines().peekable();
    let root = parse_hcl_scope(&mut lines, false)?;

    if root.is_empty() {
        None
    } else {
        Some(Value::Object(root))
    }
}

fn parse_hcl_scope<'a, I>(
    lines: &mut std::iter::Peekable<I>,
    in_block: bool,
) -> Option<serde_json::Map<String, Value>>
where
    I: Iterator<Item = &'a str>,
{
    let mut scope = serde_json::Map::new();
    let mut saw_content = false;

    while let Some(raw_line) = lines.next() {
        let line = raw_line.trim();
        if line.is_empty() || is_hcl_comment(line) {
            continue;
        }

        if line == "}" {
            return if in_block { Some(scope) } else { None };
        }

        if line.ends_with('{') {
            let Some((block_name, labels)) = parse_hcl_block_header(line) else {
                return None;
            };

            let mut block = parse_hcl_scope(lines, true)?;
            if let Some(label) = labels.first() {
                block.insert("name".to_string(), Value::String(label.clone()));
            }
            if labels.len() > 1 {
                block.insert(
                    "labels".to_string(),
                    Value::Array(labels.into_iter().map(Value::String).collect()),
                );
            }

            push_named_value(&mut scope, &block_name, Value::Object(block));
            saw_content = true;
            continue;
        }

        let Some((key, value)) = split_hcl_assignment(line) else {
            return None;
        };
        push_named_value(&mut scope, key, parse_scalar_value(value));
        saw_content = true;
    }

    if in_block {
        None
    } else if saw_content || !scope.is_empty() {
        Some(scope)
    } else {
        None
    }
}

fn parse_hcl_block_header(line: &str) -> Option<(String, Vec<String>)> {
    let trimmed = line.trim();
    let header = trimmed.strip_suffix('{')?.trim();
    let mut chars = header.chars().peekable();

    let mut block_name = String::new();
    while let Some(ch) = chars.peek().copied() {
        if ch.is_whitespace() {
            break;
        }
        block_name.push(ch);
        chars.next();
    }

    if block_name.is_empty() {
        return None;
    }

    while matches!(chars.peek(), Some(ch) if ch.is_whitespace()) {
        chars.next();
    }

    let mut labels = Vec::new();
    while let Some(ch) = chars.peek().copied() {
        if ch.is_whitespace() {
            chars.next();
            continue;
        }
        if ch != '"' {
            return None;
        }

        chars.next();
        let mut label = String::new();
        while let Some(inner) = chars.next() {
            if inner == '"' {
                break;
            }
            label.push(inner);
        }
        labels.push(label);

        while matches!(chars.peek(), Some(ch) if ch.is_whitespace()) {
            chars.next();
        }
    }

    Some((block_name, labels))
}

fn split_hcl_assignment(line: &str) -> Option<(&str, &str)> {
    let split_index = line.find('=')?;
    let (key, value) = line.split_at(split_index);
    let value = value.get(1..)?;
    let key = key.trim();
    let value = value.trim();

    if key.is_empty() || value.is_empty() {
        None
    } else {
        Some((key, value))
    }
}

fn is_hcl_comment(line: &str) -> bool {
    line.starts_with('#') || line.starts_with("//") || line.starts_with("/*")
}
