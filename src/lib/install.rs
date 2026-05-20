mod samples;

use crate::provider::{
    antigravity, claude, cline, codex, copilot, cursor, gemini, hermes, kilocode, opencode,
    windsurf,
};
use crate::{
    add_or_update_hook_rule, default_config_dir, remove_hook_rules_for_config, AgentInstallReport,
    AgentTarget, HookRule,
};
use serde_json::Value;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

const DEFAULT_HOOK_RULE_NAME: &str = "dummyjson_users";
const DEFAULT_HOOK_RULE_CONFIG: &str = "dummyjson_users.json";
const DEFAULT_HOOK_RULE_COMMAND: &str = "curl -sS https://dummyjson.com/users";
const CLAUDE_HOOK_COMMAND: &str = "dtk_hook_route --provider claude";
const CURSOR_HOOK_COMMAND: &str = "dtk_hook_route --provider cursor";
const COPILOT_HOOK_COMMAND: &str = "dtk_hook_route --provider copilot";
const GEMINI_HOOK_COMMAND: &str = "dtk_hook_route --provider gemini";

pub fn install_agent_guidance(target: AgentTarget) -> io::Result<AgentInstallReport> {
    install_agent_guidance_with_sample_set(target, false)
}

pub fn install_agent_guidance_with_dummy_samples(
    target: AgentTarget,
) -> io::Result<AgentInstallReport> {
    install_agent_guidance_with_sample_set(target, true)
}

pub fn uninstall_agent_guidance(target: AgentTarget) -> io::Result<AgentInstallReport> {
    let mut changed = false;

    match target {
        AgentTarget::All => {
            changed |= codex::uninstall_codex_guidance()?;
            changed |= codex::uninstall_codex_agents_attachment()?;
            changed |= claude::uninstall_claude_guidance()?;
            changed |= cursor::uninstall_cursor_guidance()?;
            changed |= copilot::uninstall_copilot_guidance()?;
            changed |= gemini::uninstall_gemini_guidance()?;
            changed |= windsurf::uninstall_windsurf_guidance()?;
            changed |= cline::uninstall_cline_guidance()?;
            changed |= kilocode::uninstall_kilocode_guidance()?;
            changed |= antigravity::uninstall_antigravity_guidance()?;
            changed |= opencode::uninstall_opencode_guidance()?;
            changed |= hermes::uninstall_hermes_guidance()?;
        }
        AgentTarget::Codex => {
            changed |= codex::uninstall_codex_guidance()?;
            changed |= codex::uninstall_codex_agents_attachment()?;
        }
        AgentTarget::Claude => {
            changed |= claude::uninstall_claude_guidance()?;
        }
        AgentTarget::Cursor => {
            changed |= cursor::uninstall_cursor_guidance()?;
        }
        AgentTarget::Copilot => {
            changed |= copilot::uninstall_copilot_guidance()?;
        }
        AgentTarget::Gemini => {
            changed |= gemini::uninstall_gemini_guidance()?;
        }
        AgentTarget::Windsurf => {
            changed |= windsurf::uninstall_windsurf_guidance()?;
        }
        AgentTarget::Cline => {
            changed |= cline::uninstall_cline_guidance()?;
        }
        AgentTarget::KiloCode => {
            changed |= kilocode::uninstall_kilocode_guidance()?;
        }
        AgentTarget::Antigravity => {
            changed |= antigravity::uninstall_antigravity_guidance()?;
        }
        AgentTarget::OpenCode => {
            changed |= opencode::uninstall_opencode_guidance()?;
        }
        AgentTarget::Hermes => {
            changed |= hermes::uninstall_hermes_guidance()?;
        }
    }

    if hook_rule_target(target) {
        changed |= remove_default_hook_rule_if_unused()?;
    }

    Ok(AgentInstallReport { changed })
}

pub fn install_config_skill(target: AgentTarget) -> io::Result<bool> {
    match target {
        AgentTarget::All => {
            let mut changed = false;
            changed |= codex::install_codex_skill()?;
            changed |= claude::install_claude_skill()?;
            changed |= cursor::install_cursor_skill()?;
            changed |= gemini::install_gemini_skill()?;
            Ok(changed)
        }
        AgentTarget::Codex => codex::install_codex_skill(),
        AgentTarget::Claude => claude::install_claude_skill(),
        AgentTarget::Cursor => cursor::install_cursor_skill(),
        AgentTarget::Copilot => Ok(false),
        AgentTarget::Gemini => gemini::install_gemini_skill(),
        AgentTarget::Windsurf
        | AgentTarget::Cline
        | AgentTarget::KiloCode
        | AgentTarget::Antigravity
        | AgentTarget::OpenCode
        | AgentTarget::Hermes => Ok(false),
    }
}

pub fn codex_dir() -> PathBuf {
    std::env::var("DTK_CODEX_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| platform_codex_dir())
}

pub fn claude_dir() -> PathBuf {
    std::env::var("DTK_CLAUDE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| home_dir().join(".claude"))
}

pub fn cursor_dir() -> PathBuf {
    std::env::var("DTK_CURSOR_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| home_dir().join(".cursor"))
}

pub fn gemini_dir() -> PathBuf {
    std::env::var("DTK_GEMINI_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| home_dir().join(".gemini"))
}

pub(crate) fn normalize_codex_agents_content(
    existing: &str,
    include_line: Option<&str>,
    remove_line: Option<&str>,
) -> Option<String> {
    let mut lines: Vec<String> = existing
        .lines()
        .map(str::trim)
        .filter(|line| {
            !line.is_empty()
                && *line != "@@DTK-START@@"
                && *line != "@@DTK-END@@"
                && *line != "<!-- DTK-START -->"
                && *line != "<!-- DTK-END -->"
        })
        .map(|line| line.to_string())
        .collect();

    if let Some(remove_line) = remove_line {
        lines.retain(|line| line != remove_line);
    }

    if let Some(include_line) = include_line {
        lines.retain(|line| line != include_line);
        lines.push(include_line.to_string());
    }

    if lines.is_empty() {
        return None;
    }

    Some(format!("{}\n", lines.join("\n")))
}

fn install_agent_guidance_with_sample_set(
    target: AgentTarget,
    install_dummy_samples: bool,
) -> io::Result<AgentInstallReport> {
    let mut changed = false;

    match target {
        AgentTarget::All => {
            changed |= codex::install_codex_guidance()?;
            changed |= codex::install_codex_agents_attachment()?;
            changed |= claude::install_claude_guidance()?;
            changed |= cursor::install_cursor_guidance()?;
            changed |= copilot::install_copilot_guidance()?;
            changed |= gemini::install_gemini_guidance()?;
            changed |= windsurf::install_windsurf_guidance()?;
            changed |= cline::install_cline_guidance()?;
            changed |= kilocode::install_kilocode_guidance()?;
            changed |= antigravity::install_antigravity_guidance()?;
            changed |= opencode::install_opencode_guidance()?;
            changed |= hermes::install_hermes_guidance()?;
        }
        AgentTarget::Codex => {
            changed |= codex::install_codex_guidance()?;
            changed |= codex::install_codex_agents_attachment()?;
        }
        AgentTarget::Claude => {
            changed |= claude::install_claude_guidance()?;
        }
        AgentTarget::Cursor => {
            changed |= cursor::install_cursor_guidance()?;
        }
        AgentTarget::Copilot => {
            changed |= copilot::install_copilot_guidance()?;
        }
        AgentTarget::Gemini => {
            changed |= gemini::install_gemini_guidance()?;
        }
        AgentTarget::Windsurf => {
            changed |= windsurf::install_windsurf_guidance()?;
        }
        AgentTarget::Cline => {
            changed |= cline::install_cline_guidance()?;
        }
        AgentTarget::KiloCode => {
            changed |= kilocode::install_kilocode_guidance()?;
        }
        AgentTarget::Antigravity => {
            changed |= antigravity::install_antigravity_guidance()?;
        }
        AgentTarget::OpenCode => {
            changed |= opencode::install_opencode_guidance()?;
        }
        AgentTarget::Hermes => {
            changed |= hermes::install_hermes_guidance()?;
        }
    }

    changed |= samples::install_default_sample_configs()?;
    if hook_rule_target(target) {
        changed |= install_default_hook_rule()?;
    }
    if install_dummy_samples {
        changed |= samples::install_dummy_sample_configs()?;
    }

    Ok(AgentInstallReport { changed })
}

pub(crate) fn install_text_file(path: PathBuf, content: &str) -> io::Result<bool> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let changed = match fs::read_to_string(&path) {
        Ok(existing) if existing == content => false,
        _ => {
            fs::write(&path, content)?;
            true
        }
    };

    Ok(changed)
}

fn hook_rule_target(target: AgentTarget) -> bool {
    matches!(
        target,
        AgentTarget::All
            | AgentTarget::Claude
            | AgentTarget::Cursor
            | AgentTarget::Copilot
            | AgentTarget::Gemini
    )
}

fn install_default_hook_rule() -> io::Result<bool> {
    install_default_hook_rule_in_dir(&default_config_dir())
}

fn install_default_hook_rule_in_dir(config_dir: &Path) -> io::Result<bool> {
    let hooks_path = config_dir.join("hooks.json");
    let rule = HookRule {
        name: Some(DEFAULT_HOOK_RULE_NAME.to_string()),
        config: Some(DEFAULT_HOOK_RULE_CONFIG.to_string()),
        command_prefix: Some(DEFAULT_HOOK_RULE_COMMAND.to_string()),
        command_contains: Vec::new(),
        retention_days: None,
    };

    add_or_update_hook_rule(&hooks_path, rule)
}

fn remove_default_hook_rule_if_unused() -> io::Result<bool> {
    remove_default_hook_rule_if_unused_in_paths(
        &default_config_dir(),
        &claude_dir().join("settings.json"),
        &cursor_dir().join("hooks.json"),
        &PathBuf::from(".github")
            .join("hooks")
            .join("dtk-rewrite.json"),
        &gemini_dir().join("settings.json"),
    )
}

fn remove_default_hook_rule_if_unused_in_paths(
    config_dir: &Path,
    claude_settings: &Path,
    cursor_hooks: &Path,
    copilot_hooks: &Path,
    gemini_settings: &Path,
) -> io::Result<bool> {
    if hook_provider_artifacts_present(
        claude_settings,
        cursor_hooks,
        copilot_hooks,
        gemini_settings,
    ) {
        return Ok(false);
    }

    let hooks_path = config_dir.join("hooks.json");
    let mut changed = false;
    for key in [DEFAULT_HOOK_RULE_CONFIG, DEFAULT_HOOK_RULE_NAME] {
        changed |= remove_hook_rules_for_config(&hooks_path, key)?;
    }
    Ok(changed)
}

fn hook_provider_artifacts_present(
    claude_settings: &Path,
    cursor_hooks: &Path,
    copilot_hooks: &Path,
    gemini_settings: &Path,
) -> bool {
    file_contains(claude_settings, CLAUDE_HOOK_COMMAND)
        || file_contains(cursor_hooks, CURSOR_HOOK_COMMAND)
        || file_contains(copilot_hooks, COPILOT_HOOK_COMMAND)
        || file_contains(gemini_settings, GEMINI_HOOK_COMMAND)
}

fn file_contains(path: &Path, needle: &str) -> bool {
    fs::read_to_string(path)
        .map(|content| content.contains(needle))
        .unwrap_or(false)
}

pub(crate) fn remove_if_exists(path: PathBuf) -> io::Result<bool> {
    match fs::remove_file(&path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

pub(crate) fn hooks_are_empty(hooks: &serde_json::Map<String, Value>) -> bool {
    hooks.values().all(|value| match value {
        Value::Array(items) => items.is_empty(),
        _ => false,
    })
}

pub(crate) fn load_json_file(path: &Path) -> io::Result<Value> {
    let content = fs::read_to_string(path)?;
    serde_json::from_str::<Value>(&content)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, format!("invalid json: {err}")))
}

pub(crate) fn write_json_file(path: &Path, value: &Value) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let content = serde_json::to_string_pretty(value).map_err(|err| {
        io::Error::new(io::ErrorKind::InvalidData, format!("invalid json: {err}"))
    })?;
    fs::write(path, content)
}

fn platform_codex_dir() -> PathBuf {
    if cfg!(windows) {
        windows_codex_dir()
    } else {
        unix_codex_dir()
    }
}

fn unix_codex_dir() -> PathBuf {
    std::env::var("HOME")
        .map(|home| PathBuf::from(home).join(".codex"))
        .or_else(|_| std::env::var("XDG_CONFIG_HOME").map(PathBuf::from))
        .unwrap_or_else(|_| PathBuf::from(".codex"))
}

fn windows_codex_dir() -> PathBuf {
    std::env::var("APPDATA")
        .map(PathBuf::from)
        .map(|path| path.join("Codex"))
        .or_else(|_| {
            std::env::var("LOCALAPPDATA")
                .map(PathBuf::from)
                .map(|path| path.join("Codex"))
        })
        .unwrap_or_else(|_| PathBuf::from(".codex"))
}

fn home_dir() -> PathBuf {
    std::env::var("HOME")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("USERPROFILE").map(PathBuf::from))
        .unwrap_or_else(|_| PathBuf::from("."))
}

pub(crate) fn normalize_codex_agents(
    path: PathBuf,
    include_line: Option<String>,
    remove_line: Option<String>,
) -> io::Result<bool> {
    let existing = fs::read_to_string(&path).unwrap_or_default();
    let Some(next) =
        normalize_codex_agents_content(&existing, include_line.as_deref(), remove_line.as_deref())
    else {
        match fs::remove_file(&path) {
            Ok(()) => return Ok(true),
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
            Err(err) => return Err(err),
        };
    };
    if next == existing {
        return Ok(false);
    }

    fs::write(&path, next)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::{
        install_default_hook_rule_in_dir, remove_default_hook_rule_if_unused_in_paths,
        DEFAULT_HOOK_RULE_COMMAND, DEFAULT_HOOK_RULE_CONFIG,
    };
    use std::fs;
    use std::path::{Path, PathBuf};

    fn temp_dir(name: &str) -> PathBuf {
        std::env::temp_dir()
            .join("dtk-tests")
            .join("install")
            .join(name)
    }

    fn cleanup(path: impl AsRef<Path>) {
        let _ = fs::remove_dir_all(path);
    }

    #[test]
    fn installs_default_hook_rule_for_the_shared_sample_config() {
        let config_dir = temp_dir("install-default-hook-rule");
        cleanup(&config_dir);

        let changed = install_default_hook_rule_in_dir(&config_dir).expect("install hook rule");
        assert!(changed);

        let content = fs::read_to_string(config_dir.join("hooks.json")).expect("hooks file");
        assert!(content.contains(DEFAULT_HOOK_RULE_CONFIG));
        assert!(content.contains(DEFAULT_HOOK_RULE_COMMAND));

        cleanup(&config_dir);
    }

    #[test]
    fn keeps_shared_hook_rule_when_other_provider_artifacts_exist() {
        let config_dir = temp_dir("retain-hook-rule");
        let claude_settings = temp_dir("retain-hook-rule-claude").join("settings.json");
        let cursor_hooks = temp_dir("retain-hook-rule-cursor").join("hooks.json");
        let copilot_hooks = temp_dir("retain-hook-rule-copilot")
            .join(".github")
            .join("hooks")
            .join("dtk-rewrite.json");
        let gemini_settings = temp_dir("retain-hook-rule-gemini").join("settings.json");

        cleanup(&config_dir);
        cleanup(claude_settings.parent().expect("claude parent"));
        cleanup(cursor_hooks.parent().expect("cursor parent"));
        cleanup(copilot_hooks.parent().expect("copilot parent"));
        cleanup(gemini_settings.parent().expect("gemini parent"));

        install_default_hook_rule_in_dir(&config_dir).expect("install hook rule");
        fs::create_dir_all(claude_settings.parent().expect("claude parent"))
            .expect("create claude dir");
        fs::write(
            &claude_settings,
            r#"{"hooks":{"PreToolUse":[{"hooks":[{"command":"dtk_hook_route --provider claude"}]}]}}"#,
        )
        .expect("write claude settings");

        let changed = remove_default_hook_rule_if_unused_in_paths(
            &config_dir,
            &claude_settings,
            &cursor_hooks,
            &copilot_hooks,
            &gemini_settings,
        )
        .expect("cleanup hook rule");

        assert!(!changed);
        let content = fs::read_to_string(config_dir.join("hooks.json")).expect("hooks file");
        assert!(content.contains(DEFAULT_HOOK_RULE_CONFIG));

        cleanup(&config_dir);
        cleanup(claude_settings.parent().expect("claude parent"));
        cleanup(cursor_hooks.parent().expect("cursor parent"));
        cleanup(copilot_hooks.parent().expect("copilot parent"));
        cleanup(gemini_settings.parent().expect("gemini parent"));
    }

    #[test]
    fn removes_shared_hook_rule_when_no_hook_provider_artifacts_remain() {
        let config_dir = temp_dir("remove-hook-rule");
        let claude_settings = temp_dir("remove-hook-rule-claude").join("settings.json");
        let cursor_hooks = temp_dir("remove-hook-rule-cursor").join("hooks.json");
        let copilot_hooks = temp_dir("remove-hook-rule-copilot")
            .join(".github")
            .join("hooks")
            .join("dtk-rewrite.json");
        let gemini_settings = temp_dir("remove-hook-rule-gemini").join("settings.json");

        cleanup(&config_dir);
        cleanup(claude_settings.parent().expect("claude parent"));
        cleanup(cursor_hooks.parent().expect("cursor parent"));
        cleanup(copilot_hooks.parent().expect("copilot parent"));
        cleanup(gemini_settings.parent().expect("gemini parent"));

        install_default_hook_rule_in_dir(&config_dir).expect("install hook rule");
        let changed = remove_default_hook_rule_if_unused_in_paths(
            &config_dir,
            &claude_settings,
            &cursor_hooks,
            &copilot_hooks,
            &gemini_settings,
        )
        .expect("cleanup hook rule");

        assert!(changed);
        let content = fs::read_to_string(config_dir.join("hooks.json")).expect("hooks file");
        assert!(!content.contains(DEFAULT_HOOK_RULE_CONFIG));

        cleanup(&config_dir);
        cleanup(claude_settings.parent().expect("claude parent"));
        cleanup(cursor_hooks.parent().expect("cursor parent"));
        cleanup(copilot_hooks.parent().expect("copilot parent"));
        cleanup(gemini_settings.parent().expect("gemini parent"));
    }
}
