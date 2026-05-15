mod claude;
mod codex;
mod cursor;
mod samples;

use crate::{AgentInstallReport, AgentTarget};
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
            changed |= codex::uninstall_codex_guidance()?;
            changed |= codex::uninstall_codex_agents_attachment()?;
            changed |= claude::uninstall_claude_guidance()?;
            changed |= cursor::uninstall_cursor_guidance()?;
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
            Ok(changed)
        }
        AgentTarget::Codex => codex::install_codex_skill(),
        AgentTarget::Claude => claude::install_claude_skill(),
        AgentTarget::Cursor => cursor::install_cursor_skill(),
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
            changed |= codex::install_codex_guidance()?;
            changed |= codex::install_codex_agents_attachment()?;
            changed |= claude::install_claude_guidance()?;
            changed |= cursor::install_cursor_guidance()?;
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
    }

    changed |= samples::install_default_sample_configs()?;
    if install_dummy_samples {
        changed |= samples::install_dummy_sample_configs()?;
    }

    Ok(AgentInstallReport { changed })
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
