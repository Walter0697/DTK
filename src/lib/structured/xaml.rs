use super::template::{parse_trimmed, StructuredParser};
use serde_json::Value;

pub(crate) struct XamlParser;

impl StructuredParser for XamlParser {
    fn parse(text: &str) -> Option<Value> {
        parse_trimmed(text, parse_xaml_payload)
    }
}

pub(crate) fn parse(text: &str) -> Option<Value> {
    XamlParser::parse(text)
}

fn parse_xaml_payload(stripped: &str) -> Option<Value> {
    let element = xmltree::Element::parse(stripped.as_bytes()).ok()?;
    let value = xml_element_to_json(&element)?;
    Some(Value::Object(serde_json::Map::from_iter([(
        normalize_xml_name(&element.name),
        value,
    )])))
}

fn xml_element_to_json(element: &xmltree::Element) -> Option<Value> {
    let mut map = serde_json::Map::new();

    for (key, value) in &element.attributes {
        if key.starts_with("xmlns") {
            continue;
        }

        map.insert(normalize_xml_name(key), Value::String(value.clone()));
    }

    if let Some(text) = element.get_text() {
        let trimmed = text.trim();
        if !trimmed.is_empty() {
            map.insert("text".to_string(), Value::String(trimmed.to_string()));
        }
    }

    let mut grouped_children: std::collections::BTreeMap<String, Vec<Value>> =
        std::collections::BTreeMap::new();
    for child in &element.children {
        let xmltree::XMLNode::Element(child_element) = child else {
            continue;
        };

        let child_value = xml_element_to_json(child_element)?;
        grouped_children
            .entry(normalize_xml_name(&child_element.name))
            .or_default()
            .push(child_value);
    }

    for (name, values) in grouped_children {
        if values.len() == 1 {
            map.insert(name, values.into_iter().next().unwrap_or(Value::Null));
        } else {
            map.insert(name, Value::Array(values));
        }
    }

    Some(Value::Object(map))
}

fn normalize_xml_name(name: &str) -> String {
    name.trim()
        .replace(':', "_")
        .replace('.', "_")
        .replace('/', "_")
}
