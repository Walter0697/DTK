mod csv;
mod hcl;
mod ini;
mod json;
mod template;
mod toml;
mod xaml;
mod yaml;

use crate::StructuredFormat;
use serde_json::Value;

pub(crate) fn parse_json_payload(text: &str) -> Option<Value> {
    json::parse(text)
}

pub(crate) fn parse_structured_payload(text: &str) -> Option<Value> {
    parse_structured_payload_with_hint(text, None)
}

pub(crate) fn detect_structured_format(text: &str) -> Option<StructuredFormat> {
    if json::parse(text).is_some() {
        Some(StructuredFormat::Json)
    } else if yaml::parse(text).is_some() {
        Some(StructuredFormat::Yaml)
    } else if toml::parse(text).is_some() {
        Some(StructuredFormat::Toml)
    } else if xaml::parse(text).is_some() {
        Some(StructuredFormat::Xaml)
    } else if csv::parse(text).is_some() {
        Some(StructuredFormat::Csv)
    } else if ini::parse(text).is_some() {
        Some(StructuredFormat::Ini)
    } else if hcl::parse(text).is_some() {
        Some(StructuredFormat::Hcl)
    } else {
        None
    }
}

pub(crate) fn parse_structured_payload_with_hint(
    text: &str,
    format: Option<StructuredFormat>,
) -> Option<Value> {
    match format {
        Some(StructuredFormat::Json) => json::parse(text),
        Some(StructuredFormat::Yaml) => yaml::parse(text),
        Some(StructuredFormat::Toml) => toml::parse(text),
        Some(StructuredFormat::Csv) => csv::parse(text),
        Some(StructuredFormat::Ini) => ini::parse(text),
        Some(StructuredFormat::Hcl) => hcl::parse(text),
        Some(StructuredFormat::Xaml) => xaml::parse(text),
        None => json::parse(text)
            .or_else(|| yaml::parse(text))
            .or_else(|| toml::parse(text))
            .or_else(|| xaml::parse(text))
            .or_else(|| csv::parse(text))
            .or_else(|| ini::parse(text))
            .or_else(|| hcl::parse(text)),
    }
}
