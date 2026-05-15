use crate::DTK_GUIDE;
use serde_json::Value;
use std::fs;
use std::io;

use super::{
    cursor_dir, hooks_are_empty, install_text_file, load_json_file, remove_if_exists,
    write_json_file,
};

pub(super) fn install_cursor_skill() -> io::Result<bool> {
    install_text_file(
        cursor_dir().join("skills").join("dtk").join("SKILL.md"),
        crate::DTK_CONFIG_ASSISTANT_SKILL,
    )
}

pub(super) fn install_cursor_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= install_text_file(cursor_dir().join("DTK.md"), DTK_GUIDE)?;
    Ok(changed)
}

pub(super) fn uninstall_cursor_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= remove_if_exists(cursor_dir().join("DTK.md"))?;
    changed |= remove_cursor_hooks()?;
    changed |= remove_if_exists(cursor_dir().join("hooks").join("dtk-rewrite.sh"))?;
    Ok(changed)
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
