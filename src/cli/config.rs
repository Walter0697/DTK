mod allow;
mod delete;
mod hook;
mod list;
mod resolve;

pub use hook::run_hook_command;

use std::process::ExitCode;

pub fn run_config_command(args: Vec<String>) -> ExitCode {
    let mut args = args.into_iter();
    let Some(subcommand) = args.next() else {
        print_config_usage();
        return ExitCode::from(2);
    };

    match subcommand.as_str() {
        "allow" => allow::run_config_allow_command(args.collect()),
        "list" | "ls" => list::run_config_list_command(args.collect()),
        "delete" | "remove" | "wipe" => delete::run_config_delete_command(args.collect()),
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
