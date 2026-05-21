use serde_yaml::Value;
use std::fs;
use std::io;
use std::path::PathBuf;

use super::template::{install_text_file, remove_if_exists};

const HERMES_PLUGIN_INIT: &str = r#""""DTK plugin adapter for Hermes.

The plugin only bridges Hermes terminal payloads to `dtk exec --` and fails open.
"""

import shutil
import subprocess
import sys

def register(ctx):
    if shutil.which("dtk") is None:
        print("dtk: hermes plugin warning: dtk binary not found in PATH; Hermes hook not registered", file=sys.stderr)
        return
    ctx.register_hook("pre_tool_call", _pre_tool_call)

def _pre_tool_call(tool_name=None, args=None, **_kwargs):
    try:
        if tool_name != "terminal" or not isinstance(args, dict):
            return

        command = args.get("command")
        if not isinstance(command, str) or not command.strip():
            return
        if command.startswith("dtk exec --"):
            return

        args["command"] = f"dtk exec -- {command}"
    except Exception as e:
        print(f"dtk: hermes plugin warning: {e}", file=sys.stderr)
"#;

const HERMES_PLUGIN_YAML: &str = r#"name: dtk-rewrite
version: "0.1.0"
description: Rewrite Hermes terminal commands through DTK before execution.
author: DTK Contributors
hooks:
  - pre_tool_call
provides_hooks:
  - pre_tool_call
"#;

fn hermes_home() -> PathBuf {
    std::env::var("DTK_HERMES_DIR")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("HERMES_HOME").map(PathBuf::from))
        .unwrap_or_else(|_| {
            std::env::var("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(".hermes")
        })
}

fn plugin_dir() -> PathBuf {
    hermes_home().join("plugins").join("dtk-rewrite")
}

fn init_path() -> PathBuf {
    plugin_dir().join("__init__.py")
}

fn yaml_path() -> PathBuf {
    plugin_dir().join("plugin.yaml")
}

fn config_path() -> PathBuf {
    hermes_home().join("config.yaml")
}

pub(crate) fn install_hermes_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= install_text_file(init_path(), HERMES_PLUGIN_INIT)?;
    changed |= install_text_file(yaml_path(), HERMES_PLUGIN_YAML)?;
    changed |= patch_config()?;
    Ok(changed)
}

pub(crate) fn uninstall_hermes_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= remove_if_exists(init_path())?;
    changed |= remove_if_exists(yaml_path())?;
    changed |= remove_plugin_dir_if_empty()?;
    changed |= unpatch_config()?;
    Ok(changed)
}

fn patch_config() -> io::Result<bool> {
    let path = config_path();
    let existing = fs::read_to_string(&path).unwrap_or_default();
    let mut root: Value = if existing.trim().is_empty() {
        Value::Mapping(Default::default())
    } else {
        serde_yaml::from_str(&existing).unwrap_or_else(|_| Value::Mapping(Default::default()))
    };

    let root_map = root.as_mapping_mut().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "Hermes config is not a mapping")
    })?;

    let plugins = root_map
        .entry(Value::String("plugins".to_string()))
        .or_insert_with(|| Value::Mapping(Default::default()));
    let plugins_map = plugins
        .as_mapping_mut()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "plugins is not a mapping"))?;

    let enabled = plugins_map
        .entry(Value::String("enabled".to_string()))
        .or_insert_with(|| Value::Sequence(Vec::new()));
    let enabled_seq = enabled.as_sequence_mut().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "plugins.enabled is not a sequence",
        )
    })?;

    let plugin_name = Value::String("dtk-rewrite".to_string());
    if enabled_seq.iter().any(|value| value == &plugin_name) {
        return Ok(false);
    }

    enabled_seq.push(plugin_name);
    let content = serde_yaml::to_string(&root).unwrap_or_default();
    fs::create_dir_all(path.parent().unwrap_or_else(|| std::path::Path::new(".")))?;
    fs::write(path, content)?;
    Ok(true)
}

fn unpatch_config() -> io::Result<bool> {
    let path = config_path();
    let existing = match fs::read_to_string(&path) {
        Ok(content) => content,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    };

    let mut root: Value = match serde_yaml::from_str(&existing) {
        Ok(value) => value,
        Err(_) => return Ok(false),
    };

    let Some(root_map) = root.as_mapping_mut() else {
        return Ok(false);
    };
    let Some(plugins) = root_map
        .get_mut(Value::String("plugins".to_string()))
        .and_then(Value::as_mapping_mut)
    else {
        return Ok(false);
    };
    let Some(enabled) = plugins
        .get_mut(Value::String("enabled".to_string()))
        .and_then(Value::as_sequence_mut)
    else {
        return Ok(false);
    };

    let before = enabled.len();
    enabled.retain(|value| value != &Value::String("dtk-rewrite".to_string()));
    if enabled.len() == before {
        return Ok(false);
    }

    fs::write(&path, serde_yaml::to_string(&root).unwrap_or_default())?;
    Ok(true)
}

fn remove_plugin_dir_if_empty() -> io::Result<bool> {
    let dir = plugin_dir();
    if !dir.exists() {
        return Ok(false);
    }
    match fs::remove_dir(&dir) {
        Ok(()) => Ok(true),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) if err.kind() == io::ErrorKind::DirectoryNotEmpty => Ok(false),
        Err(err) => Err(err),
    }
}
