use serde_json::Value;
use std::fs;
use std::io;

use super::template::{
    claude_dir, hooks_are_empty, install_text_file, load_json_file, remove_if_exists,
    write_json_file, ProviderTemplate,
};

const CLAUDE_HOOK_COMMAND: &str = "dtk_hook_route --provider claude";

struct ClaudeProvider;

impl ProviderTemplate for ClaudeProvider {
    fn base_dir() -> std::path::PathBuf {
        claude_dir()
    }
}

pub(crate) fn install_claude_skill() -> io::Result<bool> {
    ClaudeProvider::install_skill_file()
}

pub(crate) fn install_claude_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= ClaudeProvider::install_guidance_file()?;
    changed |= ensure_claude_instructions()?;
    changed |= ensure_claude_hook()?;
    Ok(changed)
}

pub(crate) fn uninstall_claude_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= ClaudeProvider::uninstall_guidance_file()?;
    changed |= remove_claude_instructions()?;
    changed |= remove_claude_hook()?;
    changed |= remove_if_exists(claude_dir().join("hooks").join("dtk-rewrite.sh"))?;
    changed |= remove_claude_hooks()?;
    Ok(changed)
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

fn ensure_claude_hook() -> io::Result<bool> {
    let settings_path = claude_dir().join("settings.json");
    let mut root = match fs::read_to_string(&settings_path) {
        Ok(content) if content.trim().is_empty() => serde_json::json!({}),
        Ok(content) => serde_json::from_str(&content).map_err(|err| {
            io::Error::new(io::ErrorKind::InvalidData, format!("invalid json: {err}"))
        })?,
        Err(err) if err.kind() == io::ErrorKind::NotFound => serde_json::json!({}),
        Err(err) => return Err(err),
    };

    if claude_hook_present(&root) {
        return Ok(false);
    }

    insert_claude_hook(&mut root)?;
    write_json_file(&settings_path, &root)?;
    Ok(true)
}

fn remove_claude_hook() -> io::Result<bool> {
    let settings_path = claude_dir().join("settings.json");
    let mut root = match fs::read_to_string(&settings_path) {
        Ok(content) if content.trim().is_empty() => return Ok(false),
        Ok(content) => serde_json::from_str(&content).map_err(|err| {
            io::Error::new(io::ErrorKind::InvalidData, format!("invalid json: {err}"))
        })?,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    };

    if !remove_claude_hook_from_json(&mut root) {
        return Ok(false);
    }

    let Some(hooks) = root
        .as_object_mut()
        .and_then(|map| map.get_mut("hooks"))
        .and_then(Value::as_object_mut)
    else {
        return Ok(false);
    };

    if hooks_are_empty(hooks) {
        if settings_path.exists() {
            fs::remove_file(&settings_path)?;
        }
        return Ok(true);
    }

    write_json_file(&settings_path, &root)?;
    Ok(true)
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

fn claude_hook_present(root: &Value) -> bool {
    let Some(pre_tool_use) = root
        .get("hooks")
        .and_then(|hooks| hooks.get("PreToolUse"))
        .and_then(Value::as_array)
    else {
        return false;
    };

    pre_tool_use.iter().any(|entry| {
        entry
            .get("hooks")
            .and_then(Value::as_array)
            .map(|hooks| {
                hooks.iter().any(|hook| {
                    hook.get("command")
                        .and_then(Value::as_str)
                        .is_some_and(|command| {
                            command == CLAUDE_HOOK_COMMAND || command.contains("dtk-rewrite.sh")
                        })
                })
            })
            .unwrap_or(false)
    })
}

fn insert_claude_hook(root: &mut Value) -> io::Result<()> {
    let root_obj = root.as_object_mut().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "settings.json root is not an object",
        )
    })?;

    let hooks = root_obj
        .entry("hooks")
        .or_insert_with(|| serde_json::json!({}))
        .as_object_mut()
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "hooks value is not an object")
        })?;

    let pre_tool_use = hooks
        .entry("PreToolUse")
        .or_insert_with(|| serde_json::json!([]))
        .as_array_mut()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "PreToolUse value is not an array",
            )
        })?;

    pre_tool_use.push(serde_json::json!({
        "matcher": "Bash",
        "hooks": [{
            "type": "command",
            "command": CLAUDE_HOOK_COMMAND
        }]
    }));

    Ok(())
}

fn remove_claude_hook_from_json(root: &mut Value) -> bool {
    let Some(pre_tool_use) = root
        .get_mut("hooks")
        .and_then(|hooks| hooks.get_mut("PreToolUse"))
        .and_then(Value::as_array_mut)
    else {
        return false;
    };

    let original_len = pre_tool_use.len();
    pre_tool_use.retain(|entry| {
        let Some(hooks) = entry.get("hooks").and_then(Value::as_array) else {
            return true;
        };

        !hooks.iter().any(|hook| {
            hook.get("command")
                .and_then(Value::as_str)
                .is_some_and(|command| {
                    command == CLAUDE_HOOK_COMMAND || command.contains("dtk-rewrite.sh")
                })
        })
    });

    pre_tool_use.len() < original_len
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
