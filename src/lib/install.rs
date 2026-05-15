use crate::{
    default_config_dir, AgentInstallReport, AgentTarget, DTK_CONFIG_ASSISTANT_SKILL, DTK_GUIDE,
    DUMMYJSON_USERS_CONFIG, KUBERNETES_DEPLOYMENT_YAML_CONFIG, KUBERNETES_DEPLOYMENT_YAML_PAYLOAD,
};
use serde_json::Value;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

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
            changed |= uninstall_codex_guidance()?;
            changed |= uninstall_codex_agents_attachment()?;
            changed |= uninstall_claude_guidance()?;
            changed |= uninstall_cursor_guidance()?;
        }
        AgentTarget::Codex => {
            changed |= uninstall_codex_guidance()?;
            changed |= uninstall_codex_agents_attachment()?;
        }
        AgentTarget::Claude => {
            changed |= uninstall_claude_guidance()?;
        }
        AgentTarget::Cursor => {
            changed |= uninstall_cursor_guidance()?;
        }
    }

    Ok(AgentInstallReport { changed })
}

pub fn install_config_skill(target: AgentTarget) -> io::Result<bool> {
    match target {
        AgentTarget::All => {
            let mut changed = false;
            changed |= install_codex_skill()?;
            changed |= install_claude_skill()?;
            changed |= install_cursor_skill()?;
            Ok(changed)
        }
        AgentTarget::Codex => install_codex_skill(),
        AgentTarget::Claude => install_claude_skill(),
        AgentTarget::Cursor => install_cursor_skill(),
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
            changed |= install_codex_guidance()?;
            changed |= install_codex_agents_attachment()?;
            changed |= install_claude_guidance()?;
            changed |= install_cursor_guidance()?;
        }
        AgentTarget::Codex => {
            changed |= install_codex_guidance()?;
            changed |= install_codex_agents_attachment()?;
        }
        AgentTarget::Claude => {
            changed |= install_claude_guidance()?;
        }
        AgentTarget::Cursor => {
            changed |= install_cursor_guidance()?;
        }
    }

    changed |= install_default_sample_configs()?;
    if install_dummy_samples {
        changed |= install_dummy_sample_configs()?;
    }

    Ok(AgentInstallReport { changed })
}

fn install_codex_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= install_text_file(codex_dir().join("DTK.md"), DTK_GUIDE)?;
    Ok(changed)
}

fn uninstall_codex_guidance() -> io::Result<bool> {
    remove_if_exists(codex_dir().join("DTK.md"))
}

fn install_codex_agents_attachment() -> io::Result<bool> {
    let path = codex_dir().join("AGENTS.md");
    let guide_path = codex_dir().join("DTK.md");
    let include_line = format!("@{}", guide_path.display());
    normalize_codex_agents(path, Some(include_line), None)
}

fn uninstall_codex_agents_attachment() -> io::Result<bool> {
    let path = codex_dir().join("AGENTS.md");
    let guide_path = codex_dir().join("DTK.md");
    let remove_line = format!("@{}", guide_path.display());
    normalize_codex_agents(path, None, Some(remove_line))
}

fn install_codex_skill() -> io::Result<bool> {
    install_text_file(
        codex_dir().join("skills").join("dtk").join("SKILL.md"),
        DTK_CONFIG_ASSISTANT_SKILL,
    )
}

fn install_claude_skill() -> io::Result<bool> {
    install_text_file(
        claude_dir().join("skills").join("dtk").join("SKILL.md"),
        DTK_CONFIG_ASSISTANT_SKILL,
    )
}

fn install_cursor_skill() -> io::Result<bool> {
    install_text_file(
        cursor_dir().join("skills").join("dtk").join("SKILL.md"),
        DTK_CONFIG_ASSISTANT_SKILL,
    )
}

fn install_claude_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= install_text_file(claude_dir().join("DTK.md"), DTK_GUIDE)?;
    changed |= ensure_claude_instructions()?;
    Ok(changed)
}

fn uninstall_claude_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= remove_if_exists(claude_dir().join("DTK.md"))?;
    changed |= remove_claude_instructions()?;
    changed |= remove_if_exists(claude_dir().join("hooks").join("dtk-rewrite.sh"))?;
    changed |= remove_claude_hooks()?;
    Ok(changed)
}

fn install_cursor_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= install_text_file(cursor_dir().join("DTK.md"), DTK_GUIDE)?;
    Ok(changed)
}

fn install_default_sample_configs() -> io::Result<bool> {
    install_text_file(
        default_config_dir()
            .join("configs")
            .join("dummyjson_users.json"),
        DUMMYJSON_USERS_CONFIG,
    )
}

fn install_dummy_sample_configs() -> io::Result<bool> {
    let mut changed = false;
    changed |= install_text_file(
        default_config_dir()
            .join("configs")
            .join("kubernetes_deployment.yaml.json"),
        KUBERNETES_DEPLOYMENT_YAML_CONFIG,
    )?;
    changed |= install_text_file(
        default_config_dir()
            .join("samples")
            .join("kubernetes_deployment.yaml"),
        KUBERNETES_DEPLOYMENT_YAML_PAYLOAD,
    )?;
    Ok(changed)
}

fn uninstall_cursor_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= remove_if_exists(cursor_dir().join("DTK.md"))?;
    changed |= remove_cursor_hooks()?;
    changed |= remove_if_exists(cursor_dir().join("hooks").join("dtk-rewrite.sh"))?;
    Ok(changed)
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

fn install_text_file(path: PathBuf, content: &str) -> io::Result<bool> {
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

fn remove_if_exists(path: PathBuf) -> io::Result<bool> {
    match fs::remove_file(&path) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

fn ensure_claude_instructions() -> io::Result<bool> {
    let claude_md = claude_dir().join("CLAUDE.md");
    let line = "@DTK.md";
    let existing = fs::read_to_string(&claude_md).unwrap_or_default();
    if existing
        .lines()
        .any(|existing_line| existing_line.trim() == line)
    {
        return Ok(false);
    }

    let mut next = existing.trim_end().to_string();
    if !next.is_empty() {
        next.push('\n');
    }
    next.push_str(line);
    next.push('\n');
    install_text_file(claude_md, &next)
}

fn remove_claude_hooks() -> io::Result<bool> {
    let settings_path = claude_dir().join("settings.json");
    let mut root = match load_json_file(&settings_path) {
        Ok(value) => value,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    };

    let Some(hooks) = root
        .as_object_mut()
        .and_then(|map| map.get_mut("hooks"))
        .and_then(Value::as_object_mut)
    else {
        return Ok(false);
    };

    let Some(pre_tool_use) = hooks.get_mut("PreToolUse") else {
        return Ok(false);
    };
    let Some(entries) = pre_tool_use.as_array_mut() else {
        return Ok(false);
    };

    let before = entries.len();
    entries.retain(|entry| {
        let Some(hooks) = entry.get("hooks").and_then(Value::as_array) else {
            return true;
        };
        !hooks.iter().any(|hook| {
            hook.get("command")
                .and_then(Value::as_str)
                .map(|command| command.contains("dtk-rewrite.sh"))
                .unwrap_or(false)
        })
    });

    if entries.len() == before {
        return Ok(false);
    }

    if hooks_are_empty(hooks) {
        if settings_path.exists() {
            fs::remove_file(&settings_path)?;
        }
        return Ok(true);
    }

    write_json_file(&settings_path, &root)?;
    Ok(true)
}

fn remove_claude_instructions() -> io::Result<bool> {
    let claude_md = claude_dir().join("CLAUDE.md");
    let existing = match fs::read_to_string(&claude_md) {
        Ok(text) => text,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    };

    let filtered: Vec<&str> = existing
        .lines()
        .filter(|line| line.trim() != "@DTK.md")
        .collect();

    let next = filtered.join("\n");
    if next.trim().is_empty() {
        if claude_md.exists() {
            fs::remove_file(&claude_md)?;
            return Ok(true);
        }
        return Ok(false);
    }

    let next = format!("{next}\n");
    install_text_file(claude_md, &next)
}

fn remove_cursor_hooks() -> io::Result<bool> {
    let hooks_path = cursor_dir().join("hooks.json");
    let mut root = match load_json_file(&hooks_path) {
        Ok(value) => value,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    };

    let Some(hooks) = root
        .as_object_mut()
        .and_then(|map| map.get_mut("hooks"))
        .and_then(Value::as_object_mut)
    else {
        return Ok(false);
    };

    let Some(pre_tool_use) = hooks.get_mut("preToolUse") else {
        return Ok(false);
    };
    let Some(entries) = pre_tool_use.as_array_mut() else {
        return Ok(false);
    };

    let before = entries.len();
    entries.retain(|entry| {
        entry
            .get("command")
            .and_then(Value::as_str)
            .map(|command| command != "./hooks/dtk-rewrite.sh")
            .unwrap_or(true)
    });

    if entries.len() == before {
        return Ok(false);
    }

    if hooks_are_empty(hooks) {
        if hooks_path.exists() {
            fs::remove_file(&hooks_path)?;
        }
        return Ok(true);
    }

    write_json_file(&hooks_path, &root)?;
    Ok(true)
}

fn hooks_are_empty(hooks: &serde_json::Map<String, Value>) -> bool {
    hooks.values().all(|value| match value {
        Value::Array(items) => items.is_empty(),
        _ => false,
    })
}

fn load_json_file(path: &Path) -> io::Result<Value> {
    let content = fs::read_to_string(path)?;
    serde_json::from_str::<Value>(&content)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, format!("invalid json: {err}")))
}

fn write_json_file(path: &Path, value: &Value) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let content = serde_json::to_string_pretty(value).map_err(|err| {
        io::Error::new(io::ErrorKind::InvalidData, format!("invalid json: {err}"))
    })?;
    fs::write(path, content)
}

fn home_dir() -> PathBuf {
    std::env::var("HOME")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("USERPROFILE").map(PathBuf::from))
        .unwrap_or_else(|_| PathBuf::from("."))
}

fn normalize_codex_agents(
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
