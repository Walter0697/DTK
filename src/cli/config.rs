use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use dtk::{
    add_or_update_hook_rule, default_config_dir, load_filter_config, load_hook_rules,
    remove_hook_rules_for_config, resolve_config_path, write_filter_config, FilterConfig, HookRule,
};

pub fn run_hook_command(args: Vec<String>) -> ExitCode {
    let mut iter = args.into_iter();
    let Some(subcommand) = iter.next() else {
        eprintln!("usage: dtk hook add --name NAME --config PATH --command-prefix PREFIX [--command-contains NEEDLE]...");
        return ExitCode::from(2);
    };

    if subcommand != "add" {
        eprintln!("unknown hook subcommand: {subcommand}");
        return ExitCode::from(2);
    }

    let mut name: Option<String> = None;
    let mut config: Option<String> = None;
    let mut command_prefix: Option<String> = None;
    let mut command_contains: Vec<String> = Vec::new();
    let mut retention_days: Option<u64> = None;

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--name" => name = iter.next(),
            "--config" => config = iter.next(),
            "--command-prefix" => command_prefix = iter.next(),
            "--command-contains" => {
                if let Some(value) = iter.next() {
                    command_contains.push(value);
                }
            }
            "--retention-days" => {
                let Some(value) = iter.next() else {
                    eprintln!("missing value for --retention-days");
                    return ExitCode::from(2);
                };
                let Ok(days) = value.parse::<u64>() else {
                    eprintln!("invalid retention days: {value}");
                    return ExitCode::from(2);
                };
                retention_days = Some(days);
            }
            other => {
                eprintln!("unexpected argument: {other}");
                return ExitCode::from(2);
            }
        }
    }

    let Some(name) = name else {
        eprintln!("missing --name");
        return ExitCode::from(2);
    };
    let Some(config) = config else {
        eprintln!("missing --config");
        return ExitCode::from(2);
    };
    let Some(command_prefix) = command_prefix else {
        eprintln!("missing --command-prefix");
        return ExitCode::from(2);
    };

    let rule = HookRule {
        name: Some(name),
        config: Some(config),
        command_prefix: Some(command_prefix),
        command_contains,
        retention_days,
    };

    let hooks_path = default_config_dir().join("hooks.json");
    match add_or_update_hook_rule(&hooks_path, rule) {
        Ok(true) => {
            println!("updated {}", hooks_path.display());
            ExitCode::from(0)
        }
        Ok(false) => {
            println!("already up to date: {}", hooks_path.display());
            ExitCode::from(0)
        }
        Err(err) => {
            eprintln!("failed to update hooks: {err}");
            ExitCode::from(1)
        }
    }
}

pub fn run_config_command(args: Vec<String>) -> ExitCode {
    let mut args = args.into_iter();
    let Some(subcommand) = args.next() else {
        print_config_usage();
        return ExitCode::from(2);
    };

    match subcommand.as_str() {
        "allow" => run_config_allow_command(args.collect()),
        "list" | "ls" => run_config_list_command(args.collect()),
        "delete" | "remove" | "wipe" => run_config_delete_command(args.collect()),
        "help" | "--help" | "-h" => {
            print_config_usage();
            ExitCode::from(0)
        }
        other => {
            eprintln!("unknown config subcommand: {other}");
            print_config_usage();
            ExitCode::from(2)
        }
    }
}

fn run_config_allow_command(args: Vec<String>) -> ExitCode {
    let mut args = args.into_iter();
    let Some(action) = args.next() else {
        print_config_allow_usage();
        return ExitCode::from(2);
    };
    let Some(config_identifier) = args.next() else {
        eprintln!("missing config identifier");
        print_config_allow_usage();
        return ExitCode::from(2);
    };
    let Some(field_path) = args.next() else {
        eprintln!("missing field path");
        print_config_allow_usage();
        return ExitCode::from(2);
    };
    if args.next().is_some() {
        eprintln!("unexpected extra arguments");
        print_config_allow_usage();
        return ExitCode::from(2);
    }

    let resolved = match resolve_config_identifier(&config_identifier) {
        Ok(path) => path,
        Err(err) => {
            eprintln!("failed to resolve config {config_identifier}: {err}");
            return ExitCode::from(1);
        }
    };
    let mut config = match load_filter_config(&resolved) {
        Ok(config) => config,
        Err(err) => {
            eprintln!("failed to load config {}: {err}", resolved.display());
            return ExitCode::from(1);
        }
    };

    let changed = match action.as_str() {
        "add" => add_allow_path(&mut config, &field_path),
        "remove" | "rm" => remove_allow_path(&mut config, &field_path),
        other => {
            eprintln!("unknown allow action: {other}");
            print_config_allow_usage();
            return ExitCode::from(2);
        }
    };

    if !changed {
        let message = if action == "add" {
            "allowlist already contains field"
        } else {
            "allowlist did not contain field"
        };
        println!("{message}: {} -> {}", resolved.display(), field_path.trim());
        return ExitCode::from(0);
    }

    if let Err(err) = write_filter_config(&resolved, &config) {
        eprintln!("failed to write config {}: {err}", resolved.display());
        return ExitCode::from(1);
    }

    println!(
        "updated config: {} {} {}",
        resolved.display(),
        if action == "add" { "added" } else { "removed" },
        field_path.trim()
    );
    ExitCode::from(0)
}

fn run_config_list_command(args: Vec<String>) -> ExitCode {
    if !args.is_empty() {
        eprintln!("unexpected extra arguments");
        print_config_list_usage();
        return ExitCode::from(2);
    }

    let config_dir = default_config_dir().join("configs");
    let entries = match list_config_entries(&config_dir) {
        Ok(entries) => entries,
        Err(err) => {
            eprintln!("failed to list configs in {}: {err}", config_dir.display());
            return ExitCode::from(1);
        }
    };

    if entries.is_empty() {
        println!("no configs found");
        return ExitCode::from(0);
    }

    println!("{:<24} {:<24} {}", "identifier", "config_id", "path");
    for entry in entries {
        println!(
            "{:<24} {:<24} {}",
            entry.identifier,
            entry.config_id.unwrap_or_else(|| "-".to_string()),
            entry.path.display()
        );
    }

    ExitCode::from(0)
}

fn run_config_delete_command(args: Vec<String>) -> ExitCode {
    let mut args = args.into_iter();
    let Some(config_identifier) = args.next() else {
        eprintln!("missing config identifier");
        print_config_delete_usage();
        return ExitCode::from(2);
    };
    if args.next().is_some() {
        eprintln!("unexpected extra arguments");
        print_config_delete_usage();
        return ExitCode::from(2);
    }

    let resolved = match resolve_config_identifier(&config_identifier) {
        Ok(path) => path,
        Err(err) => {
            eprintln!("failed to resolve config {config_identifier}: {err}");
            return ExitCode::from(1);
        }
    };

    let config_key = config_key_for_hooks(&resolved);
    let file_removed = match fs::remove_file(&resolved) {
        Ok(()) => true,
        Err(err) if err.kind() == io::ErrorKind::NotFound => false,
        Err(err) => {
            eprintln!("failed to delete config {}: {err}", resolved.display());
            return ExitCode::from(1);
        }
    };
    let hooks_path = default_config_dir().join("hooks.json");
    let resolved_text = resolved.to_string_lossy().to_string();
    let mut hooks_changed = false;
    for key in [
        config_identifier.as_str(),
        config_key.as_str(),
        resolved_text.as_str(),
    ] {
        match remove_hook_rules_for_config(&hooks_path, key) {
            Ok(changed) => hooks_changed |= changed,
            Err(err) => {
                eprintln!("failed to update hooks: {err}");
                return ExitCode::from(1);
            }
        }
    }

    if !file_removed && !hooks_changed {
        println!("nothing to delete for {}", resolved.display());
        return ExitCode::from(0);
    }

    println!(
        "deleted config: {}{}",
        resolved.display(),
        if hooks_changed {
            " (removed matching hook rules)"
        } else {
            ""
        }
    );
    ExitCode::from(0)
}

fn print_config_usage() {
    eprintln!("usage: dtk config <allow|delete|list> ...");
    eprintln!("  dtk config allow add <config> <field>");
    eprintln!("  dtk config allow remove <config> <field>");
    eprintln!("  dtk config list");
    eprintln!("  dtk config delete <config>");
}

fn print_config_allow_usage() {
    eprintln!("usage: dtk config allow <add|remove> <config> <field>");
}

fn print_config_delete_usage() {
    eprintln!("usage: dtk config delete <config>");
}

fn print_config_list_usage() {
    eprintln!("usage: dtk config list");
}

#[derive(Debug, Clone)]
struct ConfigEntry {
    identifier: String,
    config_id: Option<String>,
    path: PathBuf,
}

fn list_config_entries(config_dir: &Path) -> io::Result<Vec<ConfigEntry>> {
    if !config_dir.exists() {
        return Ok(Vec::new());
    }

    let mut entries = Vec::new();
    for entry in fs::read_dir(config_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() || path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }

        let config = match load_filter_config(&path) {
            Ok(config) => config,
            Err(err) => {
                eprintln!("skipping invalid config {}: {err}", path.display());
                continue;
            }
        };
        let identifier = path
            .strip_prefix(config_dir)
            .ok()
            .and_then(|relative| {
                relative
                    .to_str()
                    .map(|value| value.trim_end_matches(".json").to_string())
            })
            .or_else(|| {
                path.file_stem()
                    .and_then(|value| value.to_str())
                    .map(|value| value.to_string())
            })
            .unwrap_or_else(|| path.display().to_string());
        entries.push(ConfigEntry {
            identifier,
            config_id: resolve_config_identity(&config, &path),
            path,
        });
    }

    entries.sort_by(|left, right| left.identifier.cmp(&right.identifier));
    Ok(entries)
}

fn resolve_config_identity(config: &FilterConfig, config_path: &Path) -> Option<String> {
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

fn resolve_config_identifier(identifier: &str) -> io::Result<PathBuf> {
    resolve_config_identifier_in_dir(identifier, &default_config_dir())
}

fn resolve_config_identifier_in_dir(identifier: &str, config_dir: &Path) -> io::Result<PathBuf> {
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

fn config_key_for_hooks(path: &PathBuf) -> String {
    if let Ok(relative) = path.strip_prefix(default_config_dir().join("configs")) {
        return relative.to_string_lossy().to_string();
    }
    if let Some(name) = path.file_name().and_then(|value| value.to_str()) {
        return name.to_string();
    }
    path.to_string_lossy().to_string()
}

fn add_allow_path(config: &mut FilterConfig, field_path: &str) -> bool {
    let trimmed = field_path.trim();
    if trimmed.is_empty() || config.allow.iter().any(|existing| existing == trimmed) {
        return false;
    }
    config.allow.push(trimmed.to_string());
    true
}

fn remove_allow_path(config: &mut FilterConfig, field_path: &str) -> bool {
    let trimmed = field_path.trim();
    let before = config.allow.len();
    config.allow.retain(|existing| existing != trimmed);
    config.allow.len() != before
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::*;

    fn temp_config_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join("dtk-tests").join(name)
    }

    #[test]
    fn add_allow_path_ignores_duplicates() {
        let mut config = FilterConfig {
            id: None,
            name: None,
            source: None,
            request: None,
            notes: None,
            format: None,
            content_path: None,
            allow: vec!["users[].id".to_string()],
        };

        assert!(add_allow_path(&mut config, "users[].email"));
        assert!(!add_allow_path(&mut config, "users[].email"));
        assert_eq!(
            config.allow,
            vec!["users[].id".to_string(), "users[].email".to_string()]
        );
    }

    #[test]
    fn remove_allow_path_removes_exact_match() {
        let mut config = FilterConfig {
            id: None,
            name: None,
            source: None,
            request: None,
            notes: None,
            format: None,
            content_path: None,
            allow: vec!["users[].id".to_string(), "users[].email".to_string()],
        };

        assert!(remove_allow_path(&mut config, "users[].email"));
        assert!(!remove_allow_path(&mut config, "users[].email"));
        assert_eq!(config.allow, vec!["users[].id".to_string()]);
    }

    #[test]
    fn lists_configs_with_identifier_and_config_id() {
        let config_dir = temp_config_dir("config-list");
        let configs_dir = config_dir.join("configs");
        let _ = fs::remove_dir_all(&config_dir);
        fs::create_dir_all(&configs_dir).expect("create configs dir");
        fs::write(
            configs_dir.join("users.json"),
            r#"{"id":"users_cfg","name":"users_cfg","allow":["[].id"]}"#,
        )
        .expect("write config");
        fs::write(
            configs_dir.join("report.json"),
            r#"{"name":"report_cfg","allow":["[].title"]}"#,
        )
        .expect("write config");

        let entries = list_config_entries(&configs_dir).expect("list configs");
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].identifier, "report");
        assert_eq!(entries[0].config_id.as_deref(), Some("report_cfg"));
        assert_eq!(entries[1].identifier, "users");
        assert_eq!(entries[1].config_id.as_deref(), Some("users_cfg"));
        let _ = fs::remove_dir_all(&config_dir);
    }

    #[test]
    fn resolves_config_identifier_by_config_id_inside_temp_config_dir() {
        let config_dir = temp_config_dir("config-resolve");
        let configs_dir = config_dir.join("configs");
        let _ = fs::remove_dir_all(&config_dir);
        fs::create_dir_all(&configs_dir).expect("create configs dir");
        let config_path = configs_dir.join("users.json");
        fs::write(
            &config_path,
            r#"{"id":"users_cfg","name":"users_cfg","allow":["[].id"]}"#,
        )
        .expect("write config");

        let resolved =
            resolve_config_identifier_in_dir("users_cfg", &config_dir).expect("resolve config id");
        assert_eq!(resolved, config_path);
        let _ = fs::remove_dir_all(&config_dir);
    }
}
