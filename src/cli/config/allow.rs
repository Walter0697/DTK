use dtk::{load_filter_config, write_filter_config, FilterConfig};
use std::process::ExitCode;

use super::{print_config_allow_usage, resolve::resolve_config_identifier};

pub(super) fn run_config_allow_command(args: Vec<String>) -> ExitCode {
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

pub(super) fn add_allow_path(config: &mut FilterConfig, field_path: &str) -> bool {
    let trimmed = field_path.trim();
    if trimmed.is_empty() || config.allow.iter().any(|existing| existing == trimmed) {
        return false;
    }
    config.allow.push(trimmed.to_string());
    true
}

pub(super) fn remove_allow_path(config: &mut FilterConfig, field_path: &str) -> bool {
    let trimmed = field_path.trim();
    let before = config.allow.len();
    config.allow.retain(|existing| existing != trimmed);
    config.allow.len() != before
}

#[cfg(test)]
mod tests {
    use dtk::FilterConfig;

    use super::{add_allow_path, remove_allow_path};

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
}
