use serde_json::Value;
use std::fs;
use std::io;

use super::template::{
    cursor_dir, hooks_are_empty, remove_if_exists, write_json_file, ProviderTemplate,
};

const CURSOR_HOOK_COMMAND: &str = "dtk_hook_route --provider cursor";

struct CursorProvider;

impl ProviderTemplate for CursorProvider {
    fn base_dir() -> std::path::PathBuf {
        cursor_dir()
    }
}

pub(crate) fn install_cursor_skill() -> io::Result<bool> {
    CursorProvider::install_skill_file()
}

pub(crate) fn install_cursor_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= CursorProvider::install_guidance_file()?;
    changed |= ensure_cursor_hook()?;
    Ok(changed)
}

pub(crate) fn uninstall_cursor_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= CursorProvider::uninstall_guidance_file()?;
    changed |= remove_cursor_hook()?;
    changed |= remove_if_exists(cursor_dir().join("hooks").join("dtk-rewrite.sh"))?;
    changed |= remove_legacy_cursor_hooks()?;
    Ok(changed)
}

fn ensure_cursor_hook() -> io::Result<bool> {
    let hooks_path = cursor_dir().join("hooks.json");
    let mut root = match fs::read_to_string(&hooks_path) {
        Ok(content) if content.trim().is_empty() => serde_json::json!({ "version": 1 }),
        Ok(content) => serde_json::from_str(&content).map_err(|err| {
            io::Error::new(io::ErrorKind::InvalidData, format!("invalid json: {err}"))
        })?,
        Err(err) if err.kind() == io::ErrorKind::NotFound => serde_json::json!({ "version": 1 }),
        Err(err) => return Err(err),
    };

    if cursor_hook_present(&root) {
        return Ok(false);
    }

    insert_cursor_hook(&mut root)?;
    write_json_file(&hooks_path, &root)?;
    Ok(true)
}

fn remove_cursor_hook() -> io::Result<bool> {
    let hooks_path = cursor_dir().join("hooks.json");
    let mut root = match fs::read_to_string(&hooks_path) {
        Ok(content) if content.trim().is_empty() => return Ok(false),
        Ok(content) => serde_json::from_str(&content).map_err(|err| {
            io::Error::new(io::ErrorKind::InvalidData, format!("invalid json: {err}"))
        })?,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    };

    if !remove_cursor_hook_from_json(&mut root) {
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
        if hooks_path.exists() {
            fs::remove_file(&hooks_path)?;
        }
        return Ok(true);
    }

    write_json_file(&hooks_path, &root)?;
    Ok(true)
}

fn remove_legacy_cursor_hooks() -> io::Result<bool> {
    let hooks_path = cursor_dir().join("hooks.json");
    let mut root: Value = match fs::read_to_string(&hooks_path) {
        Ok(content) if content.trim().is_empty() => return Ok(false),
        Ok(content) => serde_json::from_str(&content).map_err(|err| {
            io::Error::new(io::ErrorKind::InvalidData, format!("invalid json: {err}"))
        })?,
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

fn cursor_hook_present(root: &Value) -> bool {
    let Some(pre_tool_use) = root
        .get("hooks")
        .and_then(|hooks| hooks.get("preToolUse"))
        .and_then(Value::as_array)
    else {
        return false;
    };

    pre_tool_use.iter().any(|entry| {
        entry
            .get("command")
            .and_then(Value::as_str)
            .is_some_and(|command| {
                command == CURSOR_HOOK_COMMAND || command.contains("dtk-rewrite.sh")
            })
    })
}

fn insert_cursor_hook(root: &mut Value) -> io::Result<()> {
    let root_obj = root.as_object_mut().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "hooks.json root is not an object",
        )
    })?;

    root_obj.entry("version").or_insert(serde_json::json!(1));

    let hooks = root_obj
        .entry("hooks")
        .or_insert_with(|| serde_json::json!({}))
        .as_object_mut()
        .ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "hooks value is not an object")
        })?;

    let pre_tool_use = hooks
        .entry("preToolUse")
        .or_insert_with(|| serde_json::json!([]))
        .as_array_mut()
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "preToolUse value is not an array",
            )
        })?;

    pre_tool_use.push(serde_json::json!({
        "command": CURSOR_HOOK_COMMAND,
        "matcher": "Shell"
    }));

    Ok(())
}

fn remove_cursor_hook_from_json(root: &mut Value) -> bool {
    let Some(pre_tool_use) = root
        .get_mut("hooks")
        .and_then(|hooks| hooks.get_mut("preToolUse"))
        .and_then(Value::as_array_mut)
    else {
        return false;
    };

    let original_len = pre_tool_use.len();
    pre_tool_use.retain(|entry| {
        !entry
            .get("command")
            .and_then(Value::as_str)
            .is_some_and(|command| {
                command == CURSOR_HOOK_COMMAND || command.contains("dtk-rewrite.sh")
            })
    });

    pre_tool_use.len() < original_len
}
