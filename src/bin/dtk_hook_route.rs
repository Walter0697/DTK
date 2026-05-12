use std::io::{self, Read};
use std::process::ExitCode;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct HookInput {
    _tool_name: Option<String>,
    tool_input: Option<HookToolInput>,
}

#[derive(Debug, Deserialize)]
struct HookToolInput {
    command: Option<String>,
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
    let mut buffer = String::new();
    if let Err(err) = io::stdin().read_to_string(&mut buffer) {
        eprintln!("failed to read hook input: {err}");
        return ExitCode::from(1);
    }

    let input: HookInput = match serde_json::from_str(&buffer) {
        Ok(input) => input,
        Err(_) => {
            return allow_noop();
        }
    };

    let command = input
        .tool_input
        .and_then(|tool_input| tool_input.command)
        .unwrap_or_default();
    if command.is_empty() {
        return allow_noop();
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
            return allow_noop();
        }
    };

    let rules: HookRules = match serde_json::from_str(&rules_text) {
        Ok(rules) => rules,
        Err(_) => return allow_noop(),
    };

    for rule in rules.rules {
        if !rule_matches(&rule, &command) {
            continue;
        }

        let Some(config) = rule.config else {
            continue;
        };

        let mut wrapped = format!("rtk dtk exec --config {}", shell_quote(&config));
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

        println!(
            "{}",
            serde_json::to_string(&response).expect("valid hook response")
        );
        return ExitCode::from(0);
    }

    allow_noop()
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
}

fn allow_noop() -> ExitCode {
    let response = serde_json::json!({
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "allow"
        }
    });
    println!(
        "{}",
        serde_json::to_string(&response).expect("valid hook response")
    );
    ExitCode::from(0)
}
