use std::io::{self, IsTerminal, Read};
use std::process::ExitCode;

use dtk::{
    filter_json_payload_with_ref, load_filter_config, parse_json_payload, resolve_config_path,
    runtime_store_dir, store_filtered_payload, store_original_payload, DEFAULT_SAMPLE_CONFIG_NAME,
};

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let config_path = args
        .next()
        .unwrap_or_else(|| DEFAULT_SAMPLE_CONFIG_NAME.to_string());

    let input = if let Some(text) = args.next() {
        text
    } else {
        if io::stdin().is_terminal() {
            eprintln!("provide JSON as an argument or pipe data on stdin");
            return ExitCode::from(2);
        }

        let mut buffer = String::new();
        if let Err(err) = io::stdin().read_to_string(&mut buffer) {
            eprintln!("failed to read stdin: {err}");
            return ExitCode::from(1);
        }
        buffer
    };

    let Some(value) = parse_json_payload(&input) else {
        eprintln!("input is not a structured JSON object or array");
        return ExitCode::from(1);
    };

    let resolved_config_path = resolve_config_path(&config_path);
    let config = match load_filter_config(&resolved_config_path) {
        Ok(config) => config,
        Err(err) => {
            eprintln!(
                "failed to load config {}: {err}",
                resolved_config_path.display()
            );
            return ExitCode::from(1);
        }
    };

    let store_dir = runtime_store_dir();
    let ref_id = match store_original_payload(&input, &store_dir) {
        Ok(ref_id) => ref_id,
        Err(err) => {
            eprintln!("failed to store original payload: {err}");
            return ExitCode::from(1);
        }
    };

    let Some(filtered) = filter_json_payload_with_ref(&value, &config, &ref_id) else {
        eprintln!("filtered payload is empty");
        return ExitCode::from(1);
    };

    if let Err(err) = store_filtered_payload(&filtered, &store_dir, &ref_id) {
        eprintln!("failed to store filtered payload: {err}");
        return ExitCode::from(1);
    }

    match serde_json::to_string_pretty(&filtered) {
        Ok(text) => {
            println!("{text}");
            ExitCode::from(0)
        }
        Err(err) => {
            eprintln!("failed to render JSON: {err}");
            ExitCode::from(1)
        }
    }
}
