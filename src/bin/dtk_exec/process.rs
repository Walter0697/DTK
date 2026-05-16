use dtk::{
    default_store_dir, filter_json_payload_with_ref, load_filter_config, parse_structured_format,
    parse_structured_payload_with_hint, recommendation_notices_for_exec, record_exec_metric_issue,
    record_exec_metrics, resolve_config_path, resolve_filter_config_id, runtime_store_dir,
    store_filtered_payload, store_original_payload_with_retention, summarize_command_signature,
    token_count_for_content, ExecMetricIssueInput, ExecMetricsInput,
};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use super::{args::ExecOptions, payload, shell};

pub(super) fn run_exec_flow(options: ExecOptions) -> ExitCode {
    let output = match payload::run_payload_command(&options.command_args) {
        Ok(output) => output,
        Err(err) => {
            eprintln!("failed to run command {}: {err}", options.command_args[0]);
            return ExitCode::from(1);
        }
    };

    if !output.stderr.is_empty() {
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
    }

    let stdout_text = String::from_utf8_lossy(&output.stdout).to_string();
    let resolved_config_path = resolve_config_path(&options.config_path);
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
    let format_hint = match config.format.as_deref() {
        Some(value) => match parse_structured_format(value) {
            Some(parsed) => Some(parsed),
            None => {
                eprintln!("unsupported config format override: {value}");
                return ExitCode::from(2);
            }
        },
        None => None,
    };

    if let Some(value) = parse_structured_payload_with_hint(&stdout_text, format_hint) {
        let config_id = resolve_filter_config_id(&config, &resolved_config_path);
        let config_path_text = resolved_config_path.to_string_lossy().to_string();

        let store_dir = runtime_store_dir();
        let preferred_store_dir = default_store_dir();
        if store_dir != preferred_store_dir {
            eprintln!(
                "DTK store dir {} is not writable; using {}",
                preferred_store_dir.display(),
                store_dir.display()
            );
        }
        let ref_id = match options.retention_days {
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

        let filtered_text = if format_hint.is_some() {
            serde_json::to_string(&filtered)
        } else {
            serde_json::to_string_pretty(&filtered)
        };
        let filtered_text = match filtered_text {
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

        let signature_args = shell::normalize_command_args_for_metrics(&options.command_args);
        if let Some(signature) = summarize_command_signature(&signature_args) {
            let original_tokens = token_count_for_content(&stdout_text);
            let filtered_tokens_raw = token_count_for_content(&filtered_text);
            let use_original_output = !matches!(format_hint, Some(dtk::StructuredFormat::Csv))
                && payload::should_return_original_output(original_tokens, filtered_tokens_raw);
            let emitted_filtered_tokens = if use_original_output {
                original_tokens
            } else {
                filtered_tokens_raw
            };
            let created_at_unix_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|duration| duration.as_millis())
                .unwrap_or(0);
            let metrics = ExecMetricsInput {
                ref_id: ref_id.clone(),
                created_at_unix_ms,
                signature,
                config_id: config_id.clone(),
                config_path: config_path_text.clone(),
                original_tokens,
                filtered_tokens: emitted_filtered_tokens,
            };

            if let Err(err) = record_exec_metrics(&store_dir, &metrics) {
                eprintln!("failed to record usage: {err}");
            }

            if use_original_output {
                let issue = ExecMetricIssueInput {
                    ref_id,
                    created_at_unix_ms,
                    signature: metrics.signature.clone(),
                    config_id: config_id.clone(),
                    config_path: config_path_text.clone(),
                    original_tokens,
                    filtered_tokens: filtered_tokens_raw,
                    issue_kind: "filtered_larger_than_original".to_string(),
                };

                if let Err(err) = record_exec_metric_issue(&store_dir, &issue) {
                    eprintln!("failed to record usage issue: {err}");
                }
                match recommendation_notices_for_exec(
                    &store_dir,
                    &config_id,
                    &metrics.signature.details,
                ) {
                    Ok(notices) => {
                        for notice in notices {
                            eprintln!("{notice}");
                        }
                    }
                    Err(err) => {
                        eprintln!("failed to load DTK recommendations: {err}");
                    }
                }

                print!("{stdout_text}");
                return match output.status.code() {
                    Some(code) => ExitCode::from(code as u8),
                    None => ExitCode::from(1),
                };
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
