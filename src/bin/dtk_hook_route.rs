use std::io::{self, Read};
use std::process::ExitCode;

use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HookProvider {
    Claude,
    Cursor,
    Copilot,
    Gemini,
}

#[derive(Debug, Deserialize)]
struct HookRules {
    #[serde(default)]
    rules: Vec<HookRule>,
}

#[derive(Debug, Deserialize)]
struct HookRule {
    name: Option<String>,
    config: Option<String>,
    #[serde(default)]
    command_prefix: Option<String>,
    #[serde(default)]
    command_contains: Vec<String>,
    #[serde(default)]
    retention_days: Option<u64>,
}

fn main() -> ExitCode {
    let provider = parse_provider();
    let mut buffer = String::new();
    if let Err(err) = io::stdin().read_to_string(&mut buffer) {
        eprintln!("failed to read hook input: {err}");
        return allow_noop(provider);
    }

    let input: Value = match serde_json::from_str(&buffer) {
        Ok(input) => input,
        Err(_) => {
            return allow_noop(provider);
        }
    };

    let command = extract_command(provider, &input).unwrap_or_default();
    if command.is_empty() {
        return allow_noop(provider);
    }

    let command = normalize_command_for_matching(&command);

    let rules_path = std::env::var("DTK_HOOK_RULES").unwrap_or_else(|_| {
        dtk::default_config_dir()
            .join("hooks.json")
            .to_string_lossy()
            .to_string()
    });
    let rules_text = match std::fs::read_to_string(&rules_path) {
        Ok(text) => text,
        Err(_) => {
            return allow_noop(provider);
        }
    };

    let rules: HookRules = match serde_json::from_str(&rules_text) {
        Ok(rules) => rules,
        Err(_) => return allow_noop(provider),
    };

    for rule in rules.rules {
        if !rule_matches(&rule, &command) {
            continue;
        }

        let Some(config) = rule.config else {
            continue;
        };

        let mut wrapped = format!("dtk exec --config {}", shell_quote(&config));
        if let Some(days) = rule.retention_days {
            wrapped.push_str(&format!(" --retention-days {days}"));
        }
        wrapped.push_str(" -- ");
        wrapped.push_str(&command);

        let response = serde_json::json!({
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "allow",
                "permissionDecisionReason": format!(
                "DTK auto-wrap{}",
                rule.name
                    .as_deref()
                    .map(|name| format!(": {name}"))
                    .unwrap_or_default()
                ),
                "updatedInput": {
                    "command": wrapped
                }
            }
        });

        println!("{}", format_hook_response(provider, response));
        return ExitCode::from(0);
    }

    if is_curl_command(&command) && command_exists("rtk") {
        let response = serde_json::json!({
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "allow",
                "permissionDecisionReason": "RTK proxy fallback",
                "updatedInput": {
                    "command": format!("rtk proxy {}", command)
                }
            }
        });

        println!("{}", format_hook_response(provider, response));
        return ExitCode::from(0);
    }

    allow_noop(provider)
}

fn rule_matches(rule: &HookRule, command: &str) -> bool {
    if let Some(prefix) = &rule.command_prefix {
        if !command.starts_with(prefix) {
            return false;
        }
    }

    if !rule
        .command_contains
        .iter()
        .all(|needle| command.contains(needle))
    {
        return false;
    }

    true
}

fn normalize_command_for_matching(command: &str) -> String {
    let trimmed = command.trim_start();
    for prefix in ["rtk proxy ", "rtk "] {
        if let Some(rest) = trimmed.strip_prefix(prefix) {
            return rest.trim_start().to_string();
        }
    }
    trimmed.to_string()
}

fn is_curl_command(command: &str) -> bool {
    command
        .split_whitespace()
        .next()
        .map(|token| token == "curl")
        .unwrap_or(false)
}

fn command_exists(binary: &str) -> bool {
    let path = match std::env::var_os("PATH") {
        Some(path) => path,
        None => return false,
    };

    for entry in std::env::split_paths(&path) {
        let candidate = entry.join(binary);
        if candidate.is_file() {
            return true;
        }
    }

    false
}

fn extract_command(provider: HookProvider, input: &Value) -> Option<String> {
    match provider {
        HookProvider::Copilot => extract_copilot_command(input),
        _ => input
            .pointer("/tool_input/command")
            .and_then(|value| value.as_str())
            .filter(|value| !value.is_empty())
            .map(|value| value.to_string()),
    }
}

fn extract_copilot_command(input: &Value) -> Option<String> {
    if let Some(tool_name) = input.get("tool_name").and_then(|value| value.as_str()) {
        if matches!(tool_name, "runTerminalCommand" | "Bash" | "bash") {
            return input
                .pointer("/tool_input/command")
                .and_then(|value| value.as_str())
                .filter(|value| !value.is_empty())
                .map(|value| value.to_string());
        }
        return None;
    }

    if let Some(tool_name) = input.get("toolName").and_then(|value| value.as_str()) {
        if tool_name == "bash" {
            if let Some(tool_args_str) = input.get("toolArgs").and_then(|value| value.as_str()) {
                if let Ok(tool_args) = serde_json::from_str::<Value>(tool_args_str) {
                    return tool_args
                        .get("command")
                        .and_then(|value| value.as_str())
                        .filter(|value| !value.is_empty())
                        .map(|value| value.to_string());
                }
            }
        }
    }

    None
}

fn shell_quote(value: &str) -> String {
    if value.is_empty() {
        return "''".to_string();
    }

    if !value.contains('\'') && !value.contains(' ') && !value.contains('"') {
        return value.to_string();
    }

    format!("'{}'", value.replace('\'', r"'\''"))
}

#[cfg(test)]
mod tests {
    use super::normalize_command_for_matching;
    use super::parse_provider_value;
    use super::HookProvider;
    use serde_json::json;

    #[test]
    fn unwraps_rtk_proxy_prefix() {
        assert_eq!(
            normalize_command_for_matching("rtk proxy curl -sS https://dummyjson.com/users"),
            "curl -sS https://dummyjson.com/users"
        );
    }

    #[test]
    fn unwraps_plain_rtk_prefix() {
        assert_eq!(
            normalize_command_for_matching("rtk curl -sS https://dummyjson.com/users"),
            "curl -sS https://dummyjson.com/users"
        );
    }

    #[test]
    fn parses_gemini_provider() {
        assert_eq!(parse_provider_value("gemini"), HookProvider::Gemini);
    }

    #[test]
    fn extracts_copilot_vscode_command() {
        let input = json!({
            "tool_name": "Bash",
            "tool_input": { "command": "git status" }
        });
        assert_eq!(
            super::extract_command(HookProvider::Copilot, &input),
            Some("git status".to_string())
        );
    }

    #[test]
    fn recognizes_curl_commands() {
        assert!(super::is_curl_command("curl -sS https://example.com"));
        assert!(!super::is_curl_command("git status"));
    }
}

fn parse_provider() -> HookProvider {
    let mut args = std::env::args().skip(1);
    let mut provider = HookProvider::Claude;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--provider" => {
                let value = args.next().unwrap_or_else(|| "claude".to_string());
                provider = parse_provider_value(&value);
            }
            _ if arg.starts_with("--provider=") => {
                let value = arg.trim_start_matches("--provider=");
                provider = parse_provider_value(value);
            }
            _ => {}
        }
    }

    provider
}

fn parse_provider_value(value: &str) -> HookProvider {
    match value.trim().to_ascii_lowercase().as_str() {
        "cursor" => HookProvider::Cursor,
        "copilot" => HookProvider::Copilot,
        "gemini" => HookProvider::Gemini,
        _ => HookProvider::Claude,
    }
}

fn format_hook_response(provider: HookProvider, response: serde_json::Value) -> String {
    match provider {
        HookProvider::Claude => serde_json::to_string(&response).expect("valid hook response"),
        HookProvider::Cursor => {
            let command = response
                .pointer("/hookSpecificOutput/updatedInput/command")
                .and_then(|value| value.as_str())
                .unwrap_or_default();
            serde_json::to_string(&serde_json::json!({
                "permission": "allow",
                "updated_input": {
                    "command": command
                }
            }))
            .expect("valid hook response")
        }
        HookProvider::Copilot => serde_json::to_string(&response).expect("valid hook response"),
        HookProvider::Gemini => {
            let command = response
                .pointer("/hookSpecificOutput/updatedInput/command")
                .and_then(|value| value.as_str())
                .unwrap_or_default();
            serde_json::to_string(&serde_json::json!({
                "decision": "allow",
                "hookSpecificOutput": {
                    "tool_input": {
                        "command": command
                    }
                }
            }))
            .expect("valid hook response")
        }
    }
}

fn allow_noop(provider: HookProvider) -> ExitCode {
    let response = match provider {
        HookProvider::Claude => serde_json::json!({
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "allow"
            }
        }),
        HookProvider::Cursor => serde_json::json!({}),
        HookProvider::Copilot => serde_json::json!({
            "hookSpecificOutput": {
                "hookEventName": "PreToolUse",
                "permissionDecision": "allow"
            }
        }),
        HookProvider::Gemini => serde_json::json!({
            "decision": "allow"
        }),
    };
    println!(
        "{}",
        serde_json::to_string(&response).expect("valid hook response")
    );
    ExitCode::from(0)
}
