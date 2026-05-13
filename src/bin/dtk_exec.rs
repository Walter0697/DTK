use std::io::ErrorKind;
use std::process::{Command, ExitCode, Output, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use dtk::{
    default_store_dir, filter_json_payload_with_ref, load_filter_config, parse_json_payload,
    record_exec_metric_issue, record_exec_metrics, resolve_config_path, runtime_store_dir,
    store_filtered_payload, store_original_payload_with_retention, summarize_command_signature,
    token_count_for_content, ExecMetricIssueInput, ExecMetricsInput, DEFAULT_SAMPLE_CONFIG_NAME,
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

        let signature_args = normalize_command_args_for_metrics(&command_args);
        if let Some(signature) = summarize_command_signature(&signature_args) {
            let original_tokens = token_count_for_content(&stdout_text);
            let filtered_tokens_raw = token_count_for_content(&filtered_text);
            let use_original_output =
                should_return_original_output(original_tokens, filtered_tokens_raw);
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
                    original_tokens,
                    filtered_tokens: filtered_tokens_raw,
                    issue_kind: "filtered_larger_than_original".to_string(),
                };

                if let Err(err) = record_exec_metric_issue(&store_dir, &issue) {
                    eprintln!("failed to record usage issue: {err}");
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

fn normalize_command_args_for_metrics(command_args: &[String]) -> Vec<String> {
    shell_wrapper_command_args(command_args).unwrap_or_else(|| command_args.to_vec())
}

fn shell_wrapper_command_args(command_args: &[String]) -> Option<Vec<String>> {
    let script = shell_wrapper_script(command_args)?;
    shell_script_command_args(&script)
}

fn shell_wrapper_script(command_args: &[String]) -> Option<String> {
    let first = command_args.first()?;
    let first_name = binary_name(first);
    let mut index = if first_name == "env" { 1 } else { 0 };

    if first_name == "env" {
        while let Some(arg) = command_args.get(index) {
            if arg == "--" {
                index += 1;
                continue;
            }
            if arg.starts_with('-') || arg.contains('=') {
                index += 1;
                continue;
            }
            break;
        }
    }

    let shell = command_args.get(index)?;
    let shell_name = binary_name(shell);
    if !matches!(shell_name, "bash" | "sh" | "zsh" | "dash" | "fish") {
        return None;
    }

    let mut flag_index = index + 1;
    while let Some(arg) = command_args.get(flag_index) {
        if arg == "--" {
            flag_index += 1;
            continue;
        }
        if arg.starts_with('-') {
            if arg.contains('c') {
                return command_args.get(flag_index + 1).cloned();
            }
            flag_index += 1;
            continue;
        }
        break;
    }

    None
}

fn shell_script_command_args(script: &str) -> Option<Vec<String>> {
    for statement in split_shell_statements(script) {
        let tokens = tokenize_shell_words(&statement);
        if tokens.is_empty() {
            continue;
        }

        if is_shell_builtin(&tokens[0]) {
            continue;
        }

        if let Some(payload) = command_args_after_dtk_exec(&tokens) {
            return Some(payload);
        }

        return Some(tokens);
    }

    None
}

fn command_args_after_dtk_exec(tokens: &[String]) -> Option<Vec<String>> {
    if tokens.len() < 2 {
        return None;
    }

    if binary_name(&tokens[0]) != "dtk" || tokens.get(1).map(String::as_str) != Some("exec") {
        return None;
    }

    let payload_start = tokens.iter().position(|token| token == "--")? + 1;
    let payload = tokens[payload_start..].to_vec();
    if payload.is_empty() {
        None
    } else {
        Some(payload)
    }
}

fn split_shell_statements(script: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = script.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;
    let mut escaped = false;

    while let Some(ch) = chars.next() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }

        match ch {
            '\\' if !in_single => {
                escaped = true;
            }
            '\'' if !in_double => {
                in_single = !in_single;
            }
            '"' if !in_single => {
                in_double = !in_double;
            }
            ';' | '\n' if !in_single && !in_double => {
                if !current.trim().is_empty() {
                    statements.push(current.trim().to_string());
                }
                current.clear();
            }
            '&' if !in_single && !in_double && matches!(chars.peek(), Some('&')) => {
                chars.next();
                if !current.trim().is_empty() {
                    statements.push(current.trim().to_string());
                }
                current.clear();
            }
            '|' if !in_single && !in_double && matches!(chars.peek(), Some('|')) => {
                chars.next();
                if !current.trim().is_empty() {
                    statements.push(current.trim().to_string());
                }
                current.clear();
            }
            '|' if !in_single && !in_double => {
                if !current.trim().is_empty() {
                    statements.push(current.trim().to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if !current.trim().is_empty() {
        statements.push(current.trim().to_string());
    }

    statements
}

fn tokenize_shell_words(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut chars = input.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;
    let mut escaped = false;

    while let Some(ch) = chars.next() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }

        match ch {
            '\\' if !in_single => {
                escaped = true;
            }
            '\'' if !in_double => {
                in_single = !in_single;
            }
            '"' if !in_single => {
                in_double = !in_double;
            }
            c if c.is_whitespace() && !in_single && !in_double => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
            }
            _ => current.push(ch),
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}

fn is_shell_builtin(command: &str) -> bool {
    matches!(
        command,
        "set"
            | "export"
            | "cd"
            | "source"
            | "."
            | "exec"
            | "command"
            | "builtin"
            | "eval"
            | "alias"
            | "unalias"
            | "umask"
            | "trap"
            | "readonly"
            | "shift"
            | "test"
            | "["
            | ":"
            | "true"
            | "false"
            | "pwd"
            | "printf"
            | "echo"
    )
}

fn binary_name(value: &str) -> &str {
    value.rsplit('/').next().unwrap_or(value)
}

fn should_return_original_output(original_tokens: usize, filtered_tokens: usize) -> bool {
    filtered_tokens > original_tokens
}

#[cfg(test)]
mod tests {
    use super::{normalize_command_args_for_metrics, should_return_original_output};

    #[test]
    fn keeps_plain_command_intact() {
        let args = vec![
            "curl".to_string(),
            "-sS".to_string(),
            "https://dummyjson.com/users".to_string(),
        ];

        assert_eq!(normalize_command_args_for_metrics(&args), args);
    }

    #[test]
    fn unwraps_shell_wrapped_dtk_exec_payload() {
        let args = vec![
            "bash".to_string(),
            "-lc".to_string(),
            "set -euo pipefail; ./target/debug/dtk exec --config dummyjson_users.json -- curl -sS https://dummyjson.com/users".to_string(),
        ];

        assert_eq!(
            normalize_command_args_for_metrics(&args),
            vec![
                "curl".to_string(),
                "-sS".to_string(),
                "https://dummyjson.com/users".to_string()
            ]
        );
    }

    #[test]
    fn falls_back_when_filtered_is_larger() {
        assert!(should_return_original_output(120, 130));
        assert!(!should_return_original_output(120, 120));
        assert!(!should_return_original_output(130, 120));
    }
}
