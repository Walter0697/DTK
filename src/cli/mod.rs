mod cache;
mod config;
mod gain;
mod install;
mod session;

use std::path::PathBuf;
use std::process::{Command, ExitCode};

use dtk::{end_session, runtime_store_dir, start_session, AgentTarget};

pub fn run() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(command) = args.next() else {
        print_usage();
        return ExitCode::from(2);
    };

    if command == "version" || command == "--version" || command == "-V" {
        println!("v{}", env!("CARGO_PKG_VERSION"));
        return ExitCode::from(0);
    }

    if command == "exec" {
        return run_exec_passthrough();
    }

    if command == "retrieve" {
        return run_retrieve_passthrough();
    }

    if command == "cache" {
        return cache::run_cache_command(args.collect());
    }

    if command == "session" {
        return session::run_session_command(args.collect());
    }

    if command == "gain" {
        return gain::run_gain_command(args.collect());
    }

    if command == "hook" {
        return config::run_hook_command(args.collect());
    }

    if command == "config" {
        return config::run_config_command(args.collect());
    }

    let mut explicit_target: Option<AgentTarget> = None;
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--agent" => {
                let Some(value) = args.next() else {
                    eprintln!("missing value for --agent");
                    return ExitCode::from(2);
                };
                match AgentTarget::parse(&value) {
                    Some(parsed) => explicit_target = Some(parsed),
                    None => {
                        eprintln!("unknown agent: {value}");
                        print_usage();
                        return ExitCode::from(2);
                    }
                }
            }
            _ if arg.starts_with("--agent=") => {
                let value = arg.trim_start_matches("--agent=");
                match AgentTarget::parse(value) {
                    Some(parsed) => explicit_target = Some(parsed),
                    None => {
                        eprintln!("unknown agent: {value}");
                        print_usage();
                        return ExitCode::from(2);
                    }
                }
            }
            other => {
                eprintln!("unexpected argument: {other}");
                print_usage();
                return ExitCode::from(2);
            }
        }
    }

    if command == "doctor" {
        let target = explicit_target.unwrap_or_else(detect_agent_target);
        return install::run_doctor(target);
    }

    let (mut target, auto_detected) = match explicit_target {
        Some(target) => (target, false),
        None => (detect_agent_target(), true),
    };

    if let Some(selected) =
        install::maybe_select_target(&command, explicit_target.is_some(), target)
    {
        target = selected;
    } else if auto_detected {
        println!(
            "Auto-detected agent target: {} (override with --agent)",
            target.as_str()
        );
    }

    if let Some(exit) = install::run_command(&command, target) {
        return exit;
    }

    match command.as_str() {
        "help" | "-h" | "--help" => {
            print_usage();
            ExitCode::from(0)
        }
        other => {
            eprintln!("unknown command: {other}");
            print_usage();
            ExitCode::from(2)
        }
    }
}

fn print_usage() {
    eprintln!(
        "Usage: dtk <install|install-dummy|uninstall|doctor|hook|config|exec|retrieve|cache|session|gain|version|help> [--agent all|codex|claude|cursor]"
    );
    eprintln!("Commands:");
    eprintln!("  dtk install");
    eprintln!("  dtk install-dummy");
    eprintln!("  dtk exec [dtk_exec args...]");
    eprintln!("  dtk retrieve [dtk_retrieve_json args...] (supports --no-pii-filter)");
    eprintln!("  dtk config list");
    eprintln!("  dtk config allow <add|remove> <config> <field>");
    eprintln!("  dtk config pii <add|remove> <config> <path> [options]");
    eprintln!("  dtk config delete <config>");
    eprintln!("  dtk cache <list|show> [ref_id]");
    eprintln!("  dtk session <start|end> [--ticket-id ID|--ticketId ID]");
    eprintln!("  dtk gain [--limit N]");
    eprintln!("  dtk version");
    eprintln!("  dtk doctor");
    eprintln!("  dtk hook add --name NAME --config PATH --command-prefix PREFIX [--command-contains NEEDLE]...");
}

fn detect_agent_target() -> AgentTarget {
    if let Ok(value) = std::env::var("DTK_AGENT") {
        if let Some(parsed) = AgentTarget::parse(&value) {
            return parsed;
        }
    }
    if std::env::var("CLAUDECODE").is_ok() || std::env::var("ANTHROPIC_API_KEY").is_ok() {
        return AgentTarget::Claude;
    }
    if std::env::var("CURSOR_TRACE_ID").is_ok() || std::env::var("CURSOR_SESSION_ID").is_ok() {
        return AgentTarget::Cursor;
    }
    AgentTarget::Codex
}

fn run_exec_passthrough() -> ExitCode {
    let passthrough_args: Vec<String> = std::env::args().skip(2).collect();

    let exec_path = resolve_dtk_exec_path();
    let mut command = Command::new(exec_path);
    command.args(passthrough_args);

    let status = match command.status() {
        Ok(status) => status,
        Err(err) => {
            eprintln!("failed to run dtk_exec: {err}");
            return ExitCode::from(1);
        }
    };

    match status.code() {
        Some(code) => ExitCode::from(code as u8),
        None => ExitCode::from(1),
    }
}

fn run_retrieve_passthrough() -> ExitCode {
    let passthrough_args: Vec<String> = std::env::args().skip(2).collect();

    let exec_path = resolve_dtk_retrieve_path();
    let mut command = Command::new(exec_path);
    command.args(passthrough_args);

    let status = match command.status() {
        Ok(status) => status,
        Err(err) => {
            eprintln!("failed to run dtk_retrieve_json: {err}");
            return ExitCode::from(1);
        }
    };

    match status.code() {
        Some(code) => ExitCode::from(code as u8),
        None => ExitCode::from(1),
    }
}

fn print_cache_table(headers: &[&str], rows: &[Vec<String>]) {
    let mut widths: Vec<usize> = headers.iter().map(|header| header.len()).collect();
    for row in rows {
        for (idx, cell) in row.iter().enumerate() {
            if idx >= widths.len() {
                widths.push(cell.len());
            } else {
                widths[idx] = widths[idx].max(cell.len());
            }
        }
    }

    print_table_border(&widths);
    print_table_row(
        headers.iter().map(|header| header.to_string()).collect(),
        &widths,
    );
    print_table_border(&widths);
    for row in rows {
        print_table_row(row.clone(), &widths);
    }
    print_table_border(&widths);
}

fn print_table_border(widths: &[usize]) {
    print!("+");
    for width in widths {
        print!("{:-<1$}+", "-", width + 2);
    }
    println!();
}

fn print_table_row(cells: Vec<String>, widths: &[usize]) {
    print!("|");
    for (idx, cell) in cells.iter().enumerate() {
        let width = widths[idx];
        print!(" {:<width$} |", cell, width = width);
    }
    println!();
}

fn age_from_unix_ms(created_at_unix_ms: u128) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(created_at_unix_ms);

    if now <= created_at_unix_ms {
        return "0s".to_string();
    }

    let diff_ms = now - created_at_unix_ms;
    if diff_ms < 1_000 {
        format!("{diff_ms}ms")
    } else if diff_ms < 60_000 {
        format!("{}s", diff_ms / 1_000)
    } else if diff_ms < 3_600_000 {
        format!("{}m", diff_ms / 60_000)
    } else {
        format!("{}h", diff_ms / 3_600_000)
    }
}

fn token_delta_for_tokens(original_tokens: &str, filtered_tokens: &str) -> String {
    let Ok(original) = original_tokens.parse::<isize>() else {
        return "-".to_string();
    };
    let Ok(filtered) = filtered_tokens.parse::<isize>() else {
        return "-".to_string();
    };

    if original <= 0 {
        return "-".to_string();
    }

    let reduction = (original - filtered) as f64 / original as f64 * 100.0;
    if reduction.is_finite() {
        format!("{reduction:.1}%")
    } else {
        "-".to_string()
    }
}

fn resolve_dtk_exec_path() -> PathBuf {
    let current_exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("dtk"));
    let mut sibling = current_exe.clone();
    let sibling_name = if cfg!(windows) {
        "dtk_exec.exe"
    } else {
        "dtk_exec"
    };
    sibling.set_file_name(sibling_name);
    if sibling.exists() {
        return sibling;
    }
    PathBuf::from("dtk_exec")
}

fn resolve_dtk_retrieve_path() -> PathBuf {
    let current_exe = std::env::current_exe().unwrap_or_else(|_| PathBuf::from("dtk"));
    let mut sibling = current_exe.clone();
    let sibling_name = if cfg!(windows) {
        "dtk_retrieve_json.exe"
    } else {
        "dtk_retrieve_json"
    };
    sibling.set_file_name(sibling_name);
    if sibling.exists() {
        return sibling;
    }
    PathBuf::from("dtk_retrieve_json")
}

#[cfg(test)]
mod tests {
    #[test]
    fn usage_mentions_install_and_uninstall() {
        let usage =
            "Usage: dtk <install|install-dummy|uninstall|doctor|hook|config|exec|retrieve|cache|session|gain|version|help> [--agent all|codex|claude|cursor]";
        assert!(usage.contains("install"));
        assert!(usage.contains("install-dummy"));
        assert!(usage.contains("uninstall"));
        assert!(usage.contains("exec"));
        assert!(usage.contains("retrieve"));
        assert!(usage.contains("config"));
        assert!(usage.contains("cache"));
        assert!(usage.contains("session"));
        assert!(usage.contains("gain"));
        assert!(usage.contains("version"));
    }
}
