mod cache;
mod config;
mod gain;
mod install;
mod marketplace;
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

    if command == "marketplace" {
        return marketplace::run_marketplace_command(args.collect());
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
        if explicit_target.is_none() {
            if let Some((_, reason)) = detect_agent_target_report() {
                println!("Auto-detected agent target: {} ({reason})", target.as_str());
            } else {
                println!("Auto-detected agent target: {}", target.as_str());
            }
        }
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
        "Usage: dtk <install|install-dummy|uninstall|doctor|hook|config|marketplace|exec|retrieve|cache|session|gain|version|help> [--agent all|codex|claude|cursor|copilot|gemini|windsurf|cline|kilocode|antigravity|opencode|hermes]"
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
    eprintln!("  dtk marketplace list [category]");
    eprintln!("  dtk marketplace search <query>");
    eprintln!("  dtk marketplace install <category|config> [--force]");
    eprintln!("  dtk marketplace update [--force]");
    eprintln!("  dtk cache <list|show> [ref_id]");
    eprintln!("  dtk session <start|end> [--ticket-id ID|--ticketId ID]");
    eprintln!("  dtk gain [--limit N]");
    eprintln!("  dtk version");
    eprintln!("  dtk doctor");
    eprintln!("  dtk hook add --name NAME --config PATH --command-prefix PREFIX [--command-contains NEEDLE]...");
}

fn detect_agent_target() -> AgentTarget {
    detect_agent_target_report()
        .map(|(target, _)| target)
        .unwrap_or(AgentTarget::Codex)
}

fn detect_agent_target_report() -> Option<(AgentTarget, &'static str)> {
    if let Ok(value) = std::env::var("DTK_AGENT") {
        if let Some(parsed) = AgentTarget::parse(&value) {
            return Some((parsed, "DTK_AGENT"));
        }
    }
    if let Some(target) = detect_agent_target_from_env() {
        return Some(target);
    }
    if let Some(target) = detect_agent_target_from_artifacts() {
        return Some(target);
    }
    None
}

fn detect_agent_target_from_env() -> Option<(AgentTarget, &'static str)> {
    if env_present(&["HERMES_HOME"]) {
        return Some((AgentTarget::Hermes, "env: HERMES_HOME"));
    }
    if env_present(&["CLAUDECODE", "ANTHROPIC_API_KEY"]) {
        return Some((AgentTarget::Claude, "env: CLAUDECODE or ANTHROPIC_API_KEY"));
    }
    if env_present(&["CURSOR_TRACE_ID", "CURSOR_SESSION_ID"]) {
        return Some((
            AgentTarget::Cursor,
            "env: CURSOR_TRACE_ID or CURSOR_SESSION_ID",
        ));
    }
    if env_present(&["COPILOT_AGENT", "GITHUB_COPILOT"]) {
        return Some((AgentTarget::Copilot, "env: COPILOT_AGENT or GITHUB_COPILOT"));
    }
    if env_present(&["GEMINI_API_KEY", "GOOGLE_API_KEY"]) {
        return Some((AgentTarget::Gemini, "env: GEMINI_API_KEY or GOOGLE_API_KEY"));
    }
    None
}

fn detect_agent_target_from_artifacts() -> Option<(AgentTarget, &'static str)> {
    if path_exists(&[".windsurfrules"]) {
        return Some((AgentTarget::Windsurf, "artifact: .windsurfrules"));
    }
    if path_exists(&[".clinerules"]) {
        return Some((AgentTarget::Cline, "artifact: .clinerules"));
    }
    if path_exists(&[".kilocode", "rules", "rtk-rules.md"]) {
        return Some((
            AgentTarget::KiloCode,
            "artifact: .kilocode/rules/rtk-rules.md",
        ));
    }
    if path_exists(&[".agents", "rules", "antigravity-rtk-rules.md"]) {
        return Some((
            AgentTarget::Antigravity,
            "artifact: .agents/rules/antigravity-rtk-rules.md",
        ));
    }
    if path_exists(&[".config", "opencode", "plugins", "dtk.ts"]) {
        return Some((
            AgentTarget::OpenCode,
            "artifact: .config/opencode/plugins/dtk.ts",
        ));
    }
    if hermes_artifact_exists() {
        return Some((AgentTarget::Hermes, "artifact: Hermes plugin/config"));
    }
    None
}

fn env_present(vars: &[&str]) -> bool {
    vars.iter().any(|var| std::env::var_os(var).is_some())
}

fn path_exists(segments: &[&str]) -> bool {
    let mut path = PathBuf::new();
    for segment in segments {
        path.push(segment);
    }
    path.exists()
}

fn hermes_artifact_exists() -> bool {
    hermes_home_dir()
        .join("plugins")
        .join("dtk-rewrite")
        .join("plugin.yaml")
        .exists()
        || hermes_home_dir()
            .join("plugins")
            .join("dtk-rewrite")
            .join("__init__.py")
            .exists()
        || hermes_home_dir().join("config.yaml").exists()
}

fn hermes_home_dir() -> PathBuf {
    if let Some(path) = std::env::var_os("HERMES_HOME").filter(|value| !value.is_empty()) {
        return PathBuf::from(path);
    }
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".hermes")
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
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn usage_mentions_install_and_uninstall() {
        let usage =
            "Usage: dtk <install|install-dummy|uninstall|doctor|hook|config|exec|retrieve|cache|session|gain|version|help> [--agent all|codex|claude|cursor|copilot|gemini|windsurf|cline|kilocode|antigravity|opencode|hermes]";
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

    #[test]
    fn detect_agent_target_prefers_explicit_agent_env() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let vars = [
            "DTK_AGENT",
            "HERMES_HOME",
            "CLAUDECODE",
            "ANTHROPIC_API_KEY",
            "CURSOR_TRACE_ID",
            "CURSOR_SESSION_ID",
            "COPILOT_AGENT",
            "GITHUB_COPILOT",
            "GEMINI_API_KEY",
            "GOOGLE_API_KEY",
        ];
        let saved: Vec<(String, Option<std::ffi::OsString>)> = vars
            .iter()
            .map(|var| ((*var).to_string(), std::env::var_os(var)))
            .collect();
        for (var, _) in &saved {
            std::env::remove_var(var);
        }
        std::env::set_var("DTK_AGENT", "gemini");
        std::env::set_var("CURSOR_TRACE_ID", "cursor");
        assert_eq!(super::detect_agent_target(), dtk::AgentTarget::Gemini);
        assert_eq!(
            super::detect_agent_target_report().map(|(_, reason)| reason),
            Some("DTK_AGENT")
        );
        for (var, value) in saved {
            match value {
                Some(value) => std::env::set_var(var, value),
                None => std::env::remove_var(var),
            }
        }
    }

    #[test]
    fn detect_agent_target_recognizes_new_provider_artifacts() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let vars = [
            "DTK_AGENT",
            "HERMES_HOME",
            "CLAUDECODE",
            "ANTHROPIC_API_KEY",
            "CURSOR_TRACE_ID",
            "CURSOR_SESSION_ID",
            "COPILOT_AGENT",
            "GITHUB_COPILOT",
            "GEMINI_API_KEY",
            "GOOGLE_API_KEY",
        ];
        let saved: Vec<(String, Option<std::ffi::OsString>)> = vars
            .iter()
            .map(|var| ((*var).to_string(), std::env::var_os(var)))
            .collect();
        for (var, _) in &saved {
            std::env::remove_var(var);
        }

        let temp = make_temp_dir();
        let original_dir = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&temp).expect("set cwd");

        std::fs::write(temp.join(".windsurfrules"), "windsurf").expect("windsurf");
        assert_eq!(super::detect_agent_target(), dtk::AgentTarget::Windsurf);
        std::fs::remove_file(temp.join(".windsurfrules")).expect("cleanup");

        std::fs::write(temp.join(".clinerules"), "cline").expect("cline");
        assert_eq!(super::detect_agent_target(), dtk::AgentTarget::Cline);
        std::fs::remove_file(temp.join(".clinerules")).expect("cleanup");

        std::fs::create_dir_all(temp.join(".kilocode/rules")).expect("kilocode dir");
        std::fs::write(temp.join(".kilocode/rules/rtk-rules.md"), "kilocode").expect("kilocode");
        assert_eq!(super::detect_agent_target(), dtk::AgentTarget::KiloCode);
        std::fs::remove_dir_all(temp.join(".kilocode")).expect("cleanup");

        std::fs::create_dir_all(temp.join(".agents/rules")).expect("agents dir");
        std::fs::write(
            temp.join(".agents/rules/antigravity-rtk-rules.md"),
            "antigravity",
        )
        .expect("antigravity");
        assert_eq!(super::detect_agent_target(), dtk::AgentTarget::Antigravity);
        std::fs::remove_dir_all(temp.join(".agents")).expect("cleanup");

        std::fs::create_dir_all(temp.join(".config/opencode/plugins")).expect("opencode dir");
        std::fs::write(temp.join(".config/opencode/plugins/dtk.ts"), "opencode").expect("opencode");
        assert_eq!(super::detect_agent_target(), dtk::AgentTarget::OpenCode);
        std::fs::remove_dir_all(temp.join(".config")).expect("cleanup");

        std::env::set_current_dir(&original_dir).expect("restore cwd");
        std::env::set_var("HERMES_HOME", &temp);
        assert_eq!(super::detect_agent_target(), dtk::AgentTarget::Hermes);

        for (var, value) in saved {
            match value {
                Some(value) => std::env::set_var(var, value),
                None => std::env::remove_var(var),
            }
        }
        std::env::set_current_dir(&original_dir).expect("restore cwd");
        let _ = std::fs::remove_dir_all(&temp);
    }

    #[test]
    fn detect_agent_target_reports_artifact_reason() {
        let _guard = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let vars = [
            "DTK_AGENT",
            "HERMES_HOME",
            "CLAUDECODE",
            "ANTHROPIC_API_KEY",
            "CURSOR_TRACE_ID",
            "CURSOR_SESSION_ID",
            "COPILOT_AGENT",
            "GITHUB_COPILOT",
            "GEMINI_API_KEY",
            "GOOGLE_API_KEY",
        ];
        let saved: Vec<(String, Option<std::ffi::OsString>)> = vars
            .iter()
            .map(|var| ((*var).to_string(), std::env::var_os(var)))
            .collect();
        for (var, _) in &saved {
            std::env::remove_var(var);
        }

        let temp = make_temp_dir();
        let original_dir = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&temp).expect("set cwd");
        std::fs::write(temp.join(".windsurfrules"), "windsurf").expect("windsurf");

        let report = super::detect_agent_target_report().expect("report");
        assert_eq!(report.0, dtk::AgentTarget::Windsurf);
        assert_eq!(report.1, "artifact: .windsurfrules");

        std::env::set_current_dir(&original_dir).expect("restore cwd");
        let _ = std::fs::remove_dir_all(&temp);

        for (var, value) in saved {
            match value {
                Some(value) => std::env::set_var(var, value),
                None => std::env::remove_var(var),
            }
        }
    }

    fn make_temp_dir() -> std::path::PathBuf {
        let mut base = std::env::temp_dir();
        let unique = format!(
            "dtk-cli-detect-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|duration| duration.as_nanos())
                .unwrap_or_default()
        );
        base.push(unique);
        std::fs::create_dir_all(&base).expect("create temp dir");
        base
    }
}
