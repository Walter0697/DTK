use dtk::{
    load_filter_config, write_filter_config, FilterConfig, PiiAction, PiiRule, PiiUuidMethod,
};
use std::process::ExitCode;

use super::resolve::resolve_config_identifier;

pub(super) fn run_config_pii_command(args: Vec<String>) -> ExitCode {
    let mut args = args.into_iter();
    let Some(action) = args.next() else {
        print_config_pii_usage();
        return ExitCode::from(2);
    };

    match action.as_str() {
        "add" => run_config_pii_add(args.collect()),
        "remove" | "rm" => run_config_pii_remove(args.collect()),
        other => {
            eprintln!("unknown pii action: {other}");
            print_config_pii_usage();
            ExitCode::from(2)
        }
    }
}

fn run_config_pii_add(args: Vec<String>) -> ExitCode {
    let mut args = args.into_iter();
    let Some(config_identifier) = args.next() else {
        eprintln!("missing config identifier");
        print_config_pii_usage();
        return ExitCode::from(2);
    };
    let Some(field_path) = args.next() else {
        eprintln!("missing pii path");
        print_config_pii_usage();
        return ExitCode::from(2);
    };
    let Some(action_text) = args.next() else {
        eprintln!("missing pii action");
        print_config_pii_usage();
        return ExitCode::from(2);
    };

    let mut replacement: Option<String> = None;
    let mut method: Option<PiiUuidMethod> = None;
    let mut template: Option<String> = None;
    let mut source_fields: Vec<String> = Vec::new();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--replacement" => {
                let Some(value) = args.next() else {
                    eprintln!("missing value for --replacement");
                    return ExitCode::from(2);
                };
                replacement = Some(value);
            }
            "--method" => {
                let Some(value) = args.next() else {
                    eprintln!("missing value for --method");
                    return ExitCode::from(2);
                };
                let Some(parsed) = parse_uuid_method(&value) else {
                    eprintln!("unknown pii uuid method: {value}");
                    return ExitCode::from(2);
                };
                method = Some(parsed);
            }
            "--template" => {
                let Some(value) = args.next() else {
                    eprintln!("missing value for --template");
                    return ExitCode::from(2);
                };
                template = Some(value);
            }
            "--source-fields" => {
                let Some(value) = args.next() else {
                    eprintln!("missing value for --source-fields");
                    return ExitCode::from(2);
                };
                source_fields.extend(split_field_list(&value));
            }
            "--source-field" => {
                let Some(value) = args.next() else {
                    eprintln!("missing value for --source-field");
                    return ExitCode::from(2);
                };
                source_fields.push(value);
            }
            other if other.starts_with("--source-fields=") => {
                source_fields.extend(split_field_list(
                    other.trim_start_matches("--source-fields="),
                ));
            }
            other if other.starts_with("--replacement=") => {
                replacement = Some(other.trim_start_matches("--replacement=").to_string());
            }
            other if other.starts_with("--method=") => {
                let value = other.trim_start_matches("--method=");
                let Some(parsed) = parse_uuid_method(value) else {
                    eprintln!("unknown pii uuid method: {value}");
                    return ExitCode::from(2);
                };
                method = Some(parsed);
            }
            other if other.starts_with("--template=") => {
                template = Some(other.trim_start_matches("--template=").to_string());
            }
            other if other.starts_with("--source-field=") => {
                source_fields.push(other.trim_start_matches("--source-field=").to_string());
            }
            other => {
                eprintln!("unexpected argument: {other}");
                print_config_pii_usage();
                return ExitCode::from(2);
            }
        }
    }

    let action = match parse_pii_action(&action_text) {
        Some(action) => action,
        None => {
            eprintln!("unknown pii action: {action_text}");
            print_config_pii_usage();
            return ExitCode::from(2);
        }
    };

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

    let rule = PiiRule {
        path: field_path.trim().to_string(),
        action,
        replacement,
        method,
        template,
        source_fields,
    };
    let changed = add_or_update_pii_rule(&mut config, rule);
    if !changed {
        println!(
            "pii rule already matches: {} -> {}",
            resolved.display(),
            field_path.trim()
        );
        return ExitCode::from(0);
    }

    if let Err(err) = write_filter_config(&resolved, &config) {
        eprintln!("failed to write config {}: {err}", resolved.display());
        return ExitCode::from(1);
    }

    println!(
        "updated pii config: {} added {}",
        resolved.display(),
        field_path.trim()
    );
    ExitCode::from(0)
}

fn run_config_pii_remove(args: Vec<String>) -> ExitCode {
    let mut args = args.into_iter();
    let Some(config_identifier) = args.next() else {
        eprintln!("missing config identifier");
        print_config_pii_usage();
        return ExitCode::from(2);
    };
    let Some(field_path) = args.next() else {
        eprintln!("missing pii path");
        print_config_pii_usage();
        return ExitCode::from(2);
    };
    if args.next().is_some() {
        eprintln!("unexpected extra arguments");
        print_config_pii_usage();
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

    if !remove_pii_rule(&mut config, field_path.trim()) {
        println!(
            "pii rule did not contain path: {} -> {}",
            resolved.display(),
            field_path.trim()
        );
        return ExitCode::from(0);
    }

    if let Err(err) = write_filter_config(&resolved, &config) {
        eprintln!("failed to write config {}: {err}", resolved.display());
        return ExitCode::from(1);
    }

    println!(
        "updated pii config: {} removed {}",
        resolved.display(),
        field_path.trim()
    );
    ExitCode::from(0)
}

fn add_or_update_pii_rule(config: &mut FilterConfig, rule: PiiRule) -> bool {
    if let Some(existing) = config
        .pii
        .iter_mut()
        .find(|existing| existing.path == rule.path)
    {
        if existing == &rule {
            return false;
        }
        *existing = rule;
        return true;
    }

    config.pii.push(rule);
    true
}

fn remove_pii_rule(config: &mut FilterConfig, field_path: &str) -> bool {
    let before = config.pii.len();
    config.pii.retain(|rule| rule.path != field_path);
    config.pii.len() != before
}

fn parse_pii_action(value: &str) -> Option<PiiAction> {
    match value.trim().to_ascii_lowercase().as_str() {
        "mask" => Some(PiiAction::Mask),
        "uuid" => Some(PiiAction::Uuid),
        "replace" => Some(PiiAction::Replace),
        _ => None,
    }
}

fn parse_uuid_method(value: &str) -> Option<PiiUuidMethod> {
    match value.trim().to_ascii_lowercase().as_str() {
        "default" => Some(PiiUuidMethod::Default),
        "random" => Some(PiiUuidMethod::Random),
        "template" => Some(PiiUuidMethod::Template),
        _ => None,
    }
}

fn split_field_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|field| !field.is_empty())
        .map(|field| field.to_string())
        .collect()
}

fn print_config_pii_usage() {
    eprintln!("usage: dtk config pii <add|remove> <config> <path> [options]");
    eprintln!("  dtk config pii add <config> <path> mask [--replacement TEXT]");
    eprintln!("  dtk config pii add <config> <path> uuid [--method default|random|template] [--template TEXT]");
    eprintln!(
        "  dtk config pii add <config> <path> replace [--source-fields a,b] [--template TEXT]"
    );
    eprintln!("  dtk config pii remove <config> <path>");
}

#[cfg(test)]
mod tests {
    use dtk::{FilterConfig, PiiAction, PiiRule, PiiUuidMethod};

    use super::{
        add_or_update_pii_rule, parse_pii_action, parse_uuid_method, remove_pii_rule,
        split_field_list,
    };

    #[test]
    fn add_or_update_pii_rule_replaces_same_path() {
        let mut config = FilterConfig {
            id: None,
            name: None,
            source: None,
            request: None,
            notes: None,
            format: None,
            content_path: None,
            allow: vec![],
            pii: vec![PiiRule {
                path: "email".to_string(),
                action: PiiAction::Mask,
                replacement: None,
                method: None,
                template: None,
                source_fields: vec![],
            }],
        };

        let changed = add_or_update_pii_rule(
            &mut config,
            PiiRule {
                path: "email".to_string(),
                action: PiiAction::Replace,
                replacement: None,
                method: None,
                template: Some("{firstName}.{lastName}@example.com".to_string()),
                source_fields: vec!["firstName".to_string(), "lastName".to_string()],
            },
        );

        assert!(changed);
        assert_eq!(config.pii.len(), 1);
        assert_eq!(config.pii[0].action, PiiAction::Replace);
    }

    #[test]
    fn remove_pii_rule_removes_exact_path() {
        let mut config = FilterConfig {
            id: None,
            name: None,
            source: None,
            request: None,
            notes: None,
            format: None,
            content_path: None,
            allow: vec![],
            pii: vec![PiiRule {
                path: "email".to_string(),
                action: PiiAction::Mask,
                replacement: None,
                method: None,
                template: None,
                source_fields: vec![],
            }],
        };

        assert!(remove_pii_rule(&mut config, "email"));
        assert!(!remove_pii_rule(&mut config, "email"));
        assert!(config.pii.is_empty());
    }

    #[test]
    fn parses_pii_command_enums() {
        assert_eq!(parse_pii_action("mask"), Some(PiiAction::Mask));
        assert_eq!(parse_pii_action("uuid"), Some(PiiAction::Uuid));
        assert_eq!(parse_pii_action("replace"), Some(PiiAction::Replace));
        assert_eq!(parse_uuid_method("default"), Some(PiiUuidMethod::Default));
        assert_eq!(parse_uuid_method("random"), Some(PiiUuidMethod::Random));
        assert_eq!(parse_uuid_method("template"), Some(PiiUuidMethod::Template));
    }

    #[test]
    fn splits_comma_separated_field_lists() {
        assert_eq!(
            split_field_list("firstName,lastName, company.name "),
            vec!["firstName", "lastName", "company.name"]
        );
    }
}
