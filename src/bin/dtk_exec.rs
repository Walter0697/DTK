use std::io::ErrorKind;
use std::process::{Command, ExitCode, Output, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use dtk::{
    default_store_dir, filter_json_payload_with_ref, load_filter_config, parse_json_payload,
    record_exec_metrics, resolve_config_path, runtime_store_dir, store_filtered_payload,
    store_original_payload_with_retention, summarize_command_signature, token_count_for_content,
    ExecMetricsInput, DEFAULT_SAMPLE_CONFIG_NAME,
};

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let mut config_path = DEFAULT_SAMPLE_CONFIG_NAME.to_string();
    let mut retention_days: Option<u64> = None;
    let mut command_args: Vec<String> = Vec::new();
    let mut seen_separator = false;

    while let Some(arg) = args.next() {
        if seen_separator {
            command_args.push(arg);
            continue;
        }

        match arg.as_str() {
            "--config" => {
                let Some(path) = args.next() else {
                    eprintln!("missing value for --config");
                    return ExitCode::from(2);
                };
                config_path = path;
            }
            "--retention-days" => {
                let Some(days) = args.next() else {
                    eprintln!("missing value for --retention-days");
                    return ExitCode::from(2);
                };
                let Ok(parsed) = days.parse::<u64>() else {
                    eprintln!("invalid retention days: {days}");
                    return ExitCode::from(2);
                };
                retention_days = Some(parsed);
            }
            "--" => {
                seen_separator = true;
            }
            other => {
                command_args.push(other.to_string());
                seen_separator = true;
            }
        }
    }

    if command_args.is_empty() {
        eprintln!("usage: dtk_exec [--config PATH] [--retention-days N] -- <command> [args...]");
        return ExitCode::from(2);
    }

    let output = match run_payload_command(&command_args) {
        Ok(output) => output,
        Err(err) => {
            eprintln!("failed to run command {}: {err}", command_args[0]);
            return ExitCode::from(1);
        }
    };

    if !output.stderr.is_empty() {
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
    }

    let stdout_text = String::from_utf8_lossy(&output.stdout).to_string();
    if let Some(value) = parse_json_payload(&stdout_text) {
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
        let preferred_store_dir = default_store_dir();
        if store_dir != preferred_store_dir {
            eprintln!(
                "DTK store dir {} is not writable; using {}",
                preferred_store_dir.display(),
                store_dir.display()
            );
        }
        let ref_id = match retention_days {
            Some(days) => {
                store_original_payload_with_retention(&stdout_text, &store_dir, Some(days))
            }
            None => store_original_payload_with_retention(&stdout_text, &store_dir, None),
        };

        let ref_id = match ref_id {
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

        let filtered_text = match serde_json::to_string_pretty(&filtered) {
            Ok(text) => text,
            Err(err) => {
                eprintln!("failed to render filtered JSON: {err}");
                return ExitCode::from(1);
            }
        };

        if let Err(err) = store_filtered_payload(&filtered, &store_dir, &ref_id) {
            eprintln!("failed to store filtered payload: {err}");
            return ExitCode::from(1);
        }

        if let Some(signature) = summarize_command_signature(&command_args) {
            let created_at_unix_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_millis())
                .unwrap_or(0);
            let metrics = ExecMetricsInput {
                ref_id: ref_id.clone(),
                created_at_unix_ms,
                signature,
                original_tokens: token_count_for_content(&stdout_text),
                filtered_tokens: token_count_for_content(&filtered_text),
            };

            if let Err(err) = record_exec_metrics(&store_dir, &metrics) {
                eprintln!("failed to record telemetry: {err}");
            }
        }

        println!("{filtered_text}");
    } else {
        print!("{stdout_text}");
    }

    if let Some(code) = output.status.code() {
        ExitCode::from(code as u8)
    } else {
        ExitCode::from(1)
    }
}

fn run_payload_command(command_args: &[String]) -> Result<Output, std::io::Error> {
    match run_payload_command_through_rtk_proxy(command_args) {
        Ok(output) => Ok(output),
        Err(err) if err.kind() == ErrorKind::NotFound => run_payload_command_direct(command_args),
        Err(err) => Err(err),
    }
}

fn run_payload_command_through_rtk_proxy(
    command_args: &[String],
) -> Result<Output, std::io::Error> {
    let mut command = Command::new("rtk");
    command.arg("proxy");
    command.args(command_args);
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    command.output()
}

fn run_payload_command_direct(command_args: &[String]) -> Result<Output, std::io::Error> {
    let mut command = Command::new(&command_args[0]);
    if command_args.len() > 1 {
        command.args(&command_args[1..]);
    }
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    command.output()
}
