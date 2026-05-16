use dtk::{
    default_config_dir, load_filter_config, load_hook_rules, resolve_config_path, FilterConfig,
};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

pub(super) fn resolve_config_identifier(identifier: &str) -> io::Result<PathBuf> {
    resolve_config_identifier_in_dir(identifier, &default_config_dir())
}

pub(super) fn resolve_config_identifier_in_dir(
    identifier: &str,
    config_dir: &Path,
) -> io::Result<PathBuf> {
    let trimmed = identifier.trim();
    if trimmed.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "config identifier cannot be empty",
        ));
    }

    let trimmed_path = Path::new(trimmed);
    if trimmed_path.is_absolute() && trimmed_path.exists() {
        return Ok(trimmed_path.to_path_buf());
    }

    let global_path = config_dir.join("configs").join(trimmed);
    if global_path.exists() {
        return Ok(global_path);
    }

    if trimmed_path.exists() {
        return Ok(trimmed_path.to_path_buf());
    }

    let hooks_path = config_dir.join("hooks.json");
    let hooks = match load_hook_rules(&hooks_path) {
        Ok(hooks) => Some(hooks),
        Err(err) if err.kind() == io::ErrorKind::NotFound => None,
        Err(err) => return Err(err),
    };

    if let Some(hooks) = hooks {
        for rule in hooks.rules {
            if rule.name.as_deref() == Some(trimmed) {
                if let Some(config) = rule.config {
                    let resolved = resolve_config_path(config);
                    if resolved.exists() {
                        return Ok(resolved);
                    }
                    return Ok(resolved);
                }
            }
        }
    }

    let configs_dir = config_dir.join("configs");
    if configs_dir.exists() {
        for entry in fs::read_dir(&configs_dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() || path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let config = match load_filter_config(&path) {
                Ok(config) => config,
                Err(_) => continue,
            };
            let matches_id = config.id.as_deref().map(str::trim) == Some(trimmed);
            let matches_name = config.name.as_deref().map(str::trim) == Some(trimmed);
            if matches_id || matches_name {
                return Ok(path);
            }
        }
    }

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!("unknown config or hook rule: {trimmed}"),
    ))
}

pub(super) fn resolve_config_identity(config: &FilterConfig, config_path: &Path) -> Option<String> {
    config
        .id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_string())
        .or_else(|| {
            config
                .name
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| value.to_string())
        })
        .or_else(|| {
            config_path
                .file_stem()
                .and_then(|value| value.to_str())
                .map(|value| value.to_string())
        })
}

pub(super) fn config_key_for_hooks(path: &PathBuf) -> String {
    if let Ok(relative) = path.strip_prefix(default_config_dir().join("configs")) {
        return relative.to_string_lossy().to_string();
    }
    if let Some(name) = path.file_name().and_then(|value| value.to_str()) {
        return name.to_string();
    }
    path.to_string_lossy().to_string()
}
