use crate::DTK_GUIDE;
use serde_json::Value;
use std::fs;
use std::io;

use super::{
    claude_dir, hooks_are_empty, install_text_file, load_json_file, remove_if_exists,
    write_json_file,
};

pub(crate) fn install_claude_skill() -> io::Result<bool> {
    install_text_file(
        claude_dir().join("skills").join("dtk").join("SKILL.md"),
        crate::DTK_CONFIG_ASSISTANT_SKILL,
    )
}

pub(crate) fn install_claude_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= install_text_file(claude_dir().join("DTK.md"), DTK_GUIDE)?;
    changed |= ensure_claude_instructions()?;
    Ok(changed)
}

pub(crate) fn uninstall_claude_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= remove_if_exists(claude_dir().join("DTK.md"))?;
    changed |= remove_claude_instructions()?;
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
