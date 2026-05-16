use dtk::{add_or_update_hook_rule, default_config_dir, HookRule};
use std::process::ExitCode;

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
