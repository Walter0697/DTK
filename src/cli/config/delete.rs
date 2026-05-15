use dtk::{default_config_dir, remove_hook_rules_for_config};
use std::fs;
use std::io;
use std::process::ExitCode;

use super::{
    print_config_delete_usage, resolve::config_key_for_hooks, resolve::resolve_config_identifier,
};

pub(super) fn run_config_delete_command(args: Vec<String>) -> ExitCode {
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
