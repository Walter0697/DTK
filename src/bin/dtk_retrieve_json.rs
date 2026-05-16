use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use dtk::{
    apply_pii_transform, load_filter_config_for_ref, recommendation_notices_for_retrieve,
    record_field_access, retrieve_original_payload, runtime_store_dir, FieldAccessRecordInput,
};

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let mut array_index: Option<usize> = None;
    let mut all = false;
    let mut ref_id: Option<String> = None;
    let mut fields_arg: Option<String> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--index" => {
                let Some(value) = args.next() else {
                    eprintln!("missing value for --index");
                    return ExitCode::from(2);
                };
                let Ok(parsed) = value.parse::<usize>() else {
                    eprintln!("invalid array index: {value}");
                    return ExitCode::from(2);
                };
                array_index = Some(parsed);
            }
            "--all" => {
                all = true;
            }
            "--fields" => {
                let Some(value) = args.next() else {
                    eprintln!("missing value for --fields");
                    return ExitCode::from(2);
                };
                fields_arg = Some(value);
            }
            "--help" | "-h" => {
                print_usage();
                return ExitCode::from(0);
            }
            other if other.starts_with("--index=") => {
                let value = other.trim_start_matches("--index=");
                let Ok(parsed) = value.parse::<usize>() else {
                    eprintln!("invalid array index: {value}");
                    return ExitCode::from(2);
                };
                array_index = Some(parsed);
            }
            other if other.starts_with("--fields=") => {
                fields_arg = Some(other.trim_start_matches("--fields=").to_string());
            }
            other if other.starts_with('-') => {
                eprintln!("unknown flag: {other}");
                print_usage();
                return ExitCode::from(2);
            }
            other => {
                if ref_id.is_none() {
                    ref_id = Some(other.to_string());
                } else if fields_arg.is_none() {
                    fields_arg = Some(other.to_string());
                } else {
                    eprintln!("unexpected argument: {other}");
                    print_usage();
                    return ExitCode::from(2);
                }
            }
        }
    }

    let Some(ref_id) = ref_id else {
        print_usage();
        return ExitCode::from(2);
    };
    let fields_arg = fields_arg.unwrap_or_default();
    let fields: Vec<String> = fields_arg
        .split(',')
        .map(str::trim)
        .filter(|field| !field.is_empty())
        .map(|field| field.to_string())
        .collect();

    if all && array_index.is_some() {
        eprintln!("use either --all or --index, not both");
        return ExitCode::from(2);
    }

    let store_dir = runtime_store_dir();
    let payload = match retrieve_original_payload(&ref_id, &store_dir, &fields, array_index, all) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("failed to retrieve payload for {ref_id}: {err}");
            return ExitCode::from(1);
        }
    };
    let payload = match load_filter_config_for_ref(&ref_id, &store_dir) {
        Ok(Some(config)) => apply_pii_transform(&payload, &config),
        Ok(None) => payload,
        Err(err) => {
            eprintln!("failed to load PII config for {ref_id}: {err}");
            payload
        }
    };

    match serde_json::to_string_pretty(&payload) {
        Ok(text) => {
            if !fields.is_empty() {
                let created_at_unix_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|duration| duration.as_millis())
                    .unwrap_or(0);
                let access = FieldAccessRecordInput {
                    ref_id: ref_id.clone(),
                    created_at_unix_ms,
                    fields: fields.clone(),
                    array_index,
                    all,
                    access_kind: "retrieve".to_string(),
                };
                if let Err(err) = record_field_access(&store_dir, &access) {
                    eprintln!("failed to record field access: {err}");
                }
                match recommendation_notices_for_retrieve(&store_dir, &ref_id, &fields) {
                    Ok(notices) => {
                        for notice in notices {
                            eprintln!("{notice}");
                        }
                    }
                    Err(err) => {
                        eprintln!("failed to load DTK recommendations: {err}");
                    }
                }
            }
            println!("{text}");
            ExitCode::from(0)
        }
        Err(err) => {
            eprintln!("failed to render retrieved JSON: {err}");
            ExitCode::from(1)
        }
    }
}

fn print_usage() {
    eprintln!(
        "usage: dtk_retrieve_json [--index N | --all] [--fields PATHS] <ref_id> [field1,field2,...]"
    );
    eprintln!(
        "  field paths use comma-separated allow-style paths like users[].address,users[0].firstName"
    );
}
