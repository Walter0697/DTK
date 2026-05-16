use crate::{FilterConfig, HookRule, HookRules};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

pub fn load_filter_config(path: impl AsRef<Path>) -> std::io::Result<FilterConfig> {
    let content = fs::read_to_string(path)?;
    let config = serde_json::from_str::<FilterConfig>(&content).map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid config: {err}"),
        )
    })?;
    Ok(config)
}

pub fn write_filter_config(path: impl AsRef<Path>, config: &FilterConfig) -> io::Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let content = serde_json::to_string_pretty(config).map_err(|err| {
        io::Error::new(io::ErrorKind::InvalidData, format!("invalid config: {err}"))
    })?;
    fs::write(path, format!("{content}\n"))
}

pub fn load_hook_rules(path: impl AsRef<Path>) -> std::io::Result<HookRules> {
    let content = fs::read_to_string(path)?;
    let rules = serde_json::from_str::<HookRules>(&content).map_err(|err| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("invalid hook rules: {err}"),
        )
    })?;
    Ok(rules)
}

pub fn write_hook_rules(path: impl AsRef<Path>, rules: &HookRules) -> std::io::Result<()> {
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let content = serde_json::to_string_pretty(rules).map_err(|err| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid hook rules: {err}"),
        )
    })?;
    fs::write(path, content)
}

pub fn add_or_update_hook_rule(path: impl AsRef<Path>, rule: HookRule) -> std::io::Result<bool> {
    let path = path.as_ref();
    let mut rules = match load_hook_rules(path) {
        Ok(rules) => rules,
        Err(err) if err.kind() == io::ErrorKind::NotFound => HookRules::default(),
        Err(err) => return Err(err),
    };

    let mut changed = false;
    let mut replaced = false;
    for existing in &mut rules.rules {
        if existing.name == rule.name || existing.config == rule.config {
            if existing != &rule {
                *existing = rule.clone();
                changed = true;
            }
            replaced = true;
            break;
        }
    }

    if !replaced {
        rules.rules.push(rule);
        changed = true;
    }

    if changed {
        write_hook_rules(path, &rules)?;
    }

    Ok(changed)
}

pub fn remove_hook_rules_for_config(
    path: impl AsRef<Path>,
    config_identifier: &str,
) -> io::Result<bool> {
    let path = path.as_ref();
    let mut rules = match load_hook_rules(path) {
        Ok(rules) => rules,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(err) => return Err(err),
    };

    let before = rules.rules.len();
    rules.rules.retain(|rule| {
        rule.config.as_deref() != Some(config_identifier)
            && rule.name.as_deref() != Some(config_identifier)
    });
    if rules.rules.len() == before {
        return Ok(false);
    }

    write_hook_rules(path, &rules)?;
    Ok(true)
}

pub fn resolve_config_path(path: impl AsRef<Path>) -> PathBuf {
    let path = path.as_ref();
    if path.is_absolute() {
        return path.to_path_buf();
    }

    let global_path = crate::paths::default_config_dir()
        .join("configs")
        .join(path);
    if global_path.exists() {
        return global_path;
    }

    if path.exists() {
        return path.to_path_buf();
    }

    global_path
}

pub fn resolve_filter_config_id(config: &FilterConfig, config_path: impl AsRef<Path>) -> String {
    if let Some(id) = config
        .id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return id.to_string();
    }

    if let Some(name) = config
        .name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return name.to_string();
    }

    let path = config_path.as_ref();
    if let Some(stem) = path.file_stem().and_then(|value| value.to_str()) {
        let stem = stem.trim();
        if !stem.is_empty() {
            return stem.to_string();
        }
    }

    let rendered = path.to_string_lossy().trim().to_string();
    if !rendered.is_empty() {
        return rendered;
    }

    "dtk_config".to_string()
}
