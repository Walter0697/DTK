use super::template::{parse_trimmed, StructuredParser};
use crate::StructuredFormat;
use serde_json::Value;

pub(crate) struct CsvParser;

impl StructuredParser for CsvParser {
    const FORMAT: StructuredFormat = StructuredFormat::Csv;

    fn parse(text: &str) -> Option<Value> {
        parse_trimmed(text, parse_csv_payload)
    }
}

pub(crate) fn parse(text: &str) -> Option<Value> {
    CsvParser::parse(text)
}

fn parse_csv_payload(stripped: &str) -> Option<Value> {
    let mut reader = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_reader(stripped.as_bytes());
    let headers = reader.headers().ok()?.clone();
    if headers.is_empty() {
        return None;
    }

    let mut rows = Vec::new();
    for record in reader.records() {
        let record = record.ok()?;
        let mut row = serde_json::Map::new();

        for (index, header) in headers.iter().enumerate() {
            let value = record.get(index).unwrap_or_default();
            row.insert(header.to_string(), Value::String(value.to_string()));
        }

        rows.push(Value::Object(row));
    }

    if rows.is_empty() || headers.len() < 2 {
        return None;
    }

    Some(Value::Object(serde_json::Map::from_iter([(
        "rows".to_string(),
        Value::Array(rows),
    )])))
}
