use serde_json::Value;
use std::fs;
use std::io;

use super::template::{hooks_are_empty, remove_if_exists, write_json_file, ProviderTemplate};
use crate::install::gemini_dir;

const GEMINI_HOOK_COMMAND: &str = "dtk_hook_route --provider gemini";

struct GeminiProvider;

impl ProviderTemplate for GeminiProvider {
    fn base_dir() -> std::path::PathBuf {
        gemini_dir()
    }
}

pub(crate) fn install_gemini_skill() -> io::Result<bool> {
    GeminiProvider::install_skill_file()
}

pub(crate) fn install_gemini_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= GeminiProvider::install_guidance_file()?;
    changed |= ensure_gemini_hook()?;
    Ok(changed)
}

pub(crate) fn uninstall_gemini_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= GeminiProvider::uninstall_guidance_file()?;
    changed |= remove_gemini_hook()?;
    changed |= remove_if_exists(gemini_dir().join("hooks").join("dtk-rewrite.sh"))?;
    Ok(changed)
}

fn ensure_gemini_hook() -> io::Result<bool> {
    let settings_path = gemini_dir().join("settings.json");
    let mut root = match fs::read_to_string(&settings_path) {
        Ok(content) if content.trim().is_empty() => serde_json::json!({}),
        Ok(content) => serde_json::from_str(&content).map_err(|err| {
            io::Error::new(io::ErrorKind::InvalidData, format!("invalid json: {err}"))
        })?,
        Err(err) if err.kind() == io::ErrorKind::NotFound => serde_json::json!({}),
        Err(err) => return Err(err),
    };

    if gemini_hook_present(&root) {
        return Ok(false);
    }

    insert_gemini_hook(&mut root)?;
    write_json_file(&settings_path, &root)?;
    Ok(true)
}

fn remove_gemini_hook() -> io::Result<bool> {
    let settings_path = gemini_dir().join("settings.json");
    let mut root = match fs::read_to_string(&settings_path) {
        Ok(content) if content.trim().is_empty() => return Ok(false),
        Ok(content) => serde_json::from_str(&content).map_err(|err| {
            io::Error::new(io::ErrorKind::InvalidData, format!("invalid json: {err}"))
        })?,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    };

    if !remove_gemini_hook_from_json(&mut root) {
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

fn gemini_hook_present(root: &Value) -> bool {
    let Some(before_tool) = root
        .get("hooks")
        .and_then(|hooks| hooks.get("BeforeTool"))
        .and_then(Value::as_array)
    else {
        return false;
    };

    before_tool.iter().any(|entry| {
        entry
            .get("hooks")
            .and_then(Value::as_array)
            .map(|hooks| {
                hooks.iter().any(|hook| {
                    hook.get("command")
                        .and_then(Value::as_str)
                        .is_some_and(|command| {
                            command == GEMINI_HOOK_COMMAND || command.contains("dtk-rewrite.sh")
                        })
                })
            })
            .unwrap_or(false)
    })
}

fn insert_gemini_hook(root: &mut Value) -> io::Result<()> {
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

    let before_tool = hooks
        .entry("BeforeTool")
        .or_insert_with(|| serde_json::json!([]))
        .as_array_mut()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "BeforeTool value is not an array",
            )
        })?;

    before_tool.push(serde_json::json!({
        "matcher": "run_shell_command",
        "hooks": [{
            "type": "command",
            "command": GEMINI_HOOK_COMMAND
        }]
    }));

    Ok(())
}

fn remove_gemini_hook_from_json(root: &mut Value) -> bool {
    let Some(before_tool) = root
        .get_mut("hooks")
        .and_then(|hooks| hooks.get_mut("BeforeTool"))
        .and_then(Value::as_array_mut)
    else {
        return false;
    };

    let original_len = before_tool.len();
    before_tool.retain(|entry| {
        let Some(hooks) = entry.get("hooks").and_then(Value::as_array) else {
            return true;
        };

        !hooks.iter().any(|hook| {
            hook.get("command")
                .and_then(Value::as_str)
                .is_some_and(|command| {
                    command == GEMINI_HOOK_COMMAND || command.contains("dtk-rewrite.sh")
                })
        })
    });

    before_tool.len() < original_len
}
