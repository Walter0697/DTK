use std::fs;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use dialoguer::{theme::ColorfulTheme, MultiSelect, Select};
use dtk::{
    add_or_update_hook_rule, claude_dir, codex_dir, cursor_dir, default_config_dir,
    filtered_payload_path, install_agent_guidance, install_config_skill, read_store_index,
    runtime_store_dir, uninstall_agent_guidance, AgentTarget, HookRule,
};

fn main() -> ExitCode {
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
        return run_cache_command(args.collect());
    }

    if command == "hook" {
        return run_hook_command(args.collect());
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
        return run_doctor(target);
    }

    let (mut target, auto_detected) = match explicit_target {
        Some(target) => (target, false),
        None => (detect_agent_target(), true),
    };

    if command == "install"
        && explicit_target.is_none()
        && io::stdin().is_terminal()
        && io::stdout().is_terminal()
    {
        if let Some(selected) = select_target_interactive(target, "install") {
            target = selected;
        }
    } else if command == "uninstall"
        && explicit_target.is_none()
        && io::stdin().is_terminal()
        && io::stdout().is_terminal()
    {
        if let Some(selected) = select_target_interactive(
            selection_to_target(&detect_installed_selection(), AgentTarget::Codex),
            "uninstall",
        ) {
            target = selected;
        }
    } else if auto_detected {
        println!(
            "Auto-detected agent target: {} (override with --agent)",
            target.as_str()
        );
    }

    match command.as_str() {
        "install" => {
            let (exit, changed) = run_agent_steps(
                "Installing DTK integration",
                target,
                install_agent_guidance,
                "install failed",
            );
            if exit == ExitCode::from(0) {
                maybe_install_skill_interactive(target);
            }
            if changed {
                eprintln!("Install complete for {}", target.as_str());
            } else {
                eprintln!("Already up to date for {}", target.as_str());
            }
            exit
        }
        "uninstall" => {
            let (exit, changed) = run_agent_steps(
                "Removing DTK integration",
                target,
                uninstall_agent_guidance,
                "uninstall failed",
            );
            if changed {
                eprintln!("Uninstall complete for {}", target.as_str());
            } else {
                eprintln!("Nothing to remove for {}", target.as_str());
            }
            exit
        }
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
        "Usage: dtk <install|uninstall|doctor|hook|exec|retrieve|cache|version|help> [--agent all|codex|claude|cursor]"
    );
    eprintln!("  dtk exec [dtk_exec args...]");
    eprintln!("  dtk retrieve [dtk_retrieve_json args...]");
    eprintln!("  dtk cache <list|show> [ref_id]");
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

fn run_agent_steps<F>(
    label: &str,
    target: AgentTarget,
    op: F,
    fail_prefix: &str,
) -> (ExitCode, bool)
where
    F: Fn(AgentTarget) -> Result<dtk::AgentInstallReport, std::io::Error>,
{
    let agents = expand_target(target);
    let total = agents.len();
    let mut any_changed = false;
    for (idx, agent) in agents.iter().enumerate() {
        let width = 24usize;
        let progress = ((idx * 100) / total).min(99);
        let filled = (progress * width) / 100;
        eprint!(
            "\r\x1b[2K[{}{}] {:>3}% {} ({}/{})",
            "=".repeat(filled),
            " ".repeat(width.saturating_sub(filled)),
            progress,
            label,
            idx + 1,
            total
        );
        let _ = io::stderr().flush();

        match op(*agent) {
            Ok(report) => {
                any_changed |= report.changed;
            }
            Err(err) => {
                eprint!("\r\x1b[2K");
                eprintln!("{fail_prefix}: {err}");
                return (ExitCode::from(1), any_changed);
            }
        }
    }
    eprint!("\r\x1b[2K[{}] 100% {}\n", "=".repeat(24), label);
    let _ = io::stderr().flush();
    (ExitCode::from(0), any_changed)
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

fn run_cache_command(args: Vec<String>) -> ExitCode {
    let mut iter = args.into_iter();
    let Some(subcommand) = iter.next() else {
        print_cache_usage();
        return ExitCode::from(2);
    };

    match subcommand.as_str() {
        "list" => run_cache_list(),
        "show" => {
            let Some(ref_id) = iter.next() else {
                eprintln!("missing ref_id");
                print_cache_usage();
                return ExitCode::from(2);
            };
            run_cache_show(&ref_id)
        }
        "help" | "-h" | "--help" => {
            print_cache_usage();
            ExitCode::from(0)
        }
        other => {
            eprintln!("unknown cache subcommand: {other}");
            print_cache_usage();
            ExitCode::from(2)
        }
    }
}

fn print_cache_usage() {
    eprintln!("Usage: dtk cache <list|show> [ref_id]");
    eprintln!("  dtk cache list");
    eprintln!("  dtk cache show <ref_id>");
}

fn run_cache_list() -> ExitCode {
    let store_dir = runtime_store_dir();
    let index = match read_store_index(&store_dir) {
        Ok(index) => index,
        Err(err) => {
            eprintln!("failed to read DTK cache index: {err}");
            return ExitCode::from(1);
        }
    };

    if index.is_empty() {
        println!("no cache entries");
        return ExitCode::from(0);
    }

    let mut entries: Vec<_> = index.into_iter().collect();
    entries.sort_by(|left, right| {
        left.1
            .created_at_unix_ms
            .cmp(&right.1.created_at_unix_ms)
            .then(left.0.cmp(&right.0))
    });

    let mut rows: Vec<Vec<String>> = Vec::new();
    for (ref_id, entry) in entries {
        let filtered_path = filtered_payload_path(&store_dir, &ref_id);
        let original_tokens = token_count_for_path(Path::new(&entry.path));
        let filtered_tokens = token_count_for_path(filtered_path.as_path());
        rows.push(vec![
            ref_id,
            age_from_unix_ms(entry.created_at_unix_ms),
            original_tokens.clone(),
            filtered_tokens.clone(),
            token_delta_for_tokens(&original_tokens, &filtered_tokens),
        ]);
    }

    print_cache_table(
        &["ref_id", "age", "orig_tokens", "filtered_tokens", "delta"],
        &rows,
    );

    ExitCode::from(0)
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

fn token_count_for_path(path: &Path) -> String {
    let Ok(content) = fs::read_to_string(path) else {
        return "-".to_string();
    };

    token_count_for_content(&content).to_string()
}

fn token_count_for_content(content: &str) -> usize {
    let normalized = serde_json::from_str::<serde_json::Value>(content)
        .ok()
        .and_then(|value| serde_json::to_string(&value).ok())
        .unwrap_or_else(|| content.to_string());

    let mut count = 0usize;
    let mut in_word = false;

    for ch in normalized.chars() {
        if ch.is_whitespace() {
            if in_word {
                in_word = false;
            }
            continue;
        }

        if ch.is_alphanumeric() || ch == '_' {
            if !in_word {
                count += 1;
                in_word = true;
            }
        } else {
            if in_word {
                in_word = false;
            }
            count += 1;
        }
    }

    count
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

fn run_cache_show(ref_id: &str) -> ExitCode {
    let store_dir = runtime_store_dir();
    let index = match read_store_index(&store_dir) {
        Ok(index) => index,
        Err(err) => {
            eprintln!("failed to read DTK cache index: {err}");
            return ExitCode::from(1);
        }
    };

    let Some(entry) = index.get(ref_id) else {
        eprintln!("unknown ref_id: {ref_id}");
        return ExitCode::from(1);
    };

    let filtered_path = filtered_payload_path(&store_dir, ref_id);
    println!("ref_id: {ref_id}");
    println!("created_at_unix_ms: {}", entry.created_at_unix_ms);
    println!(
        "retention_days: {}",
        entry
            .retention_days
            .map(|days| days.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "expires_at_unix_ms: {}",
        entry
            .expires_at_unix_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!("original_path: {}", entry.path);
    println!("filtered_path: {}", filtered_path.display());
    println!();
    println!("--- original ---");
    match fs::read_to_string(&entry.path) {
        Ok(content) => print_json_or_raw(&content),
        Err(err) => {
            eprintln!("failed to read original payload: {err}");
            return ExitCode::from(1);
        }
    }
    println!();
    println!("--- filtered ---");
    match fs::read_to_string(&filtered_path) {
        Ok(content) => print_json_or_raw(&content),
        Err(err) if err.kind() == io::ErrorKind::NotFound => println!("[missing]"),
        Err(err) => {
            eprintln!("failed to read filtered payload: {err}");
            return ExitCode::from(1);
        }
    }

    ExitCode::from(0)
}

fn print_json_or_raw(content: &str) {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(content) {
        match serde_json::to_string_pretty(&value) {
            Ok(text) => println!("{text}"),
            Err(_) => println!("{content}"),
        }
    } else {
        println!("{content}");
    }
}

fn run_hook_command(args: Vec<String>) -> ExitCode {
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
            "--name" => {
                name = iter.next();
            }
            "--config" => {
                config = iter.next();
            }
            "--command-prefix" => {
                command_prefix = iter.next();
            }
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

fn run_doctor(target: AgentTarget) -> ExitCode {
    let store_dir = runtime_store_dir();

    println!("DTK doctor");
    println!("  version: v{}", env!("CARGO_PKG_VERSION"));
    println!("  detected agent: {}", target.as_str());
    println!("  store dir: {}", store_dir.display());

    let checks = doctor_checks(target, &store_dir);
    let mut failed = false;

    for check in checks {
        let status = if check.ok {
            "ok"
        } else if check.required {
            "missing"
        } else {
            "warn"
        };
        let note = if check.required { "" } else { " (optional)" };
        println!("  [{status}] {}{}{}", check.label, note, check.detail);
        if check.required && !check.ok {
            failed = true;
        }
    }

    if failed {
        ExitCode::from(1)
    } else {
        ExitCode::from(0)
    }
}

#[derive(Debug, Clone)]
struct DoctorCheck {
    label: String,
    ok: bool,
    detail: String,
    required: bool,
}

fn doctor_checks(target: AgentTarget, store_dir: &PathBuf) -> Vec<DoctorCheck> {
    let mut checks = Vec::new();
    match target {
        AgentTarget::All => {
            checks.extend(agent_doctor_checks(AgentTarget::Codex));
            checks.extend(agent_doctor_checks(AgentTarget::Claude));
            checks.extend(agent_doctor_checks(AgentTarget::Cursor));
        }
        AgentTarget::Codex | AgentTarget::Claude | AgentTarget::Cursor => {
            checks.extend(agent_doctor_checks(target));
        }
    }

    let store_ok = check_store_writable(store_dir);
    checks.push(store_ok);

    checks
}

fn agent_doctor_checks(target: AgentTarget) -> Vec<DoctorCheck> {
    let mut checks = Vec::new();
    match target {
        AgentTarget::Codex => {
            let guide = codex_dir().join("DTK.md");
            let skill = codex_dir().join("skills").join("dtk").join("SKILL.md");
            checks.push(file_check(&guide, true));
            checks.push(text_contains_check(&guide, "DTK Config Assistant", true));
            checks.push(file_check(&skill, false));
        }
        AgentTarget::Claude => {
            let guide = claude_dir().join("DTK.md");
            let skill = claude_dir().join("skills").join("dtk").join("SKILL.md");
            let claude_md = claude_dir().join("CLAUDE.md");
            checks.push(file_check(&guide, true));
            checks.push(text_contains_check(&guide, "DTK Config Assistant", true));
            checks.push(file_check(&skill, false));
            checks.push(file_check(&claude_md, true));
            checks.push(text_contains_check(&claude_md, "@DTK.md", true));
        }
        AgentTarget::Cursor => {
            let guide = cursor_dir().join("DTK.md");
            let skill = cursor_dir().join("skills").join("dtk").join("SKILL.md");
            checks.push(file_check(&guide, true));
            checks.push(text_contains_check(&guide, "DTK Config Assistant", true));
            checks.push(file_check(&skill, false));
        }
        AgentTarget::All => unreachable!(),
    }

    checks
}

fn file_check(path: &PathBuf, required: bool) -> DoctorCheck {
    DoctorCheck {
        label: path.display().to_string(),
        ok: path.exists(),
        detail: String::new(),
        required,
    }
}

fn text_contains_check(path: &PathBuf, needle: &str, required: bool) -> DoctorCheck {
    match fs::read_to_string(path) {
        Ok(content) => DoctorCheck {
            label: format!("{} contains {:?}", path.display(), needle),
            ok: content.contains(needle),
            detail: String::new(),
            required,
        },
        Err(err) => DoctorCheck {
            label: format!("{} contains {:?}", path.display(), needle),
            ok: false,
            detail: format!(" ({err})"),
            required,
        },
    }
}

fn check_store_writable(store_dir: &PathBuf) -> DoctorCheck {
    let test_dir = store_dir.join(".doctor");
    let test_file = test_dir.join("write-test");
    let result = (|| -> std::io::Result<()> {
        std::fs::create_dir_all(&test_dir)?;
        std::fs::write(&test_file, b"ok")?;
        std::fs::remove_file(&test_file)?;
        let _ = std::fs::remove_dir(&test_dir);
        Ok(())
    })();

    match result {
        Ok(()) => DoctorCheck {
            label: "store writable".to_string(),
            ok: true,
            detail: format!(" ({})", store_dir.display()),
            required: true,
        },
        Err(err) => DoctorCheck {
            label: "store writable".to_string(),
            ok: false,
            detail: format!(" ({}: {err})", store_dir.display()),
            required: true,
        },
    }
}

fn maybe_install_skill_interactive(target: AgentTarget) {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return;
    }

    let should_install = prompt_skill_install(target);
    if !should_install {
        eprintln!("Skipped skill installation.");
        return;
    }

    let exit = run_skill_steps(target);
    if exit == ExitCode::from(0) {
        eprintln!("Skill install complete for {}", target.as_str());
    } else {
        eprintln!("DTK integration installed, but skill install did not complete.");
    }
}

fn prompt_skill_install(target: AgentTarget) -> bool {
    loop {
        eprintln!();
        let selection = Select::with_theme(&ColorfulTheme::default())
            .with_prompt(format!(
                "Install DTK configuration skill for {}",
                target.as_str()
            ))
            .items(["Yes", "No", "What is it?"])
            .default(0)
            .interact();

        let Ok(selection) = selection else {
            eprintln!("Skipping skill install (failed to read input).");
            return false;
        };

        match selection {
            0 => return true,
            1 => return false,
            _ => {
                explain_skill(target);
            }
        }
    }
}

fn explain_skill(target: AgentTarget) {
    eprintln!();
    eprintln!(
        "DTK config skill helps you configure DTK from a live curl URL, endpoint, or command."
    );
    eprintln!(
        "It runs the source, inspects the output, asks what fields matter, and drafts config."
    );
    eprintln!(
        "For {}, it is optional guidance for interactive payload filtering setup.",
        target.as_str()
    );
}

fn run_skill_steps(target: AgentTarget) -> ExitCode {
    let agents = expand_target(target);
    let total = agents.len();
    for (idx, agent) in agents.iter().enumerate() {
        let width = 24usize;
        let progress = ((idx * 100) / total).min(99);
        let filled = (progress * width) / 100;
        eprint!(
            "\r\x1b[2K[{}{}] {:>3}% Installing DTK skill ({}/{})",
            "=".repeat(filled),
            " ".repeat(width.saturating_sub(filled)),
            progress,
            idx + 1,
            total
        );
        let _ = io::stderr().flush();
        if let Err(err) = install_config_skill(*agent) {
            eprint!("\r\x1b[2K");
            eprintln!("skill install failed: {err}");
            return ExitCode::from(1);
        }
    }
    eprint!("\r\x1b[2K[{}] 100% Installing DTK skill\n", "=".repeat(24));
    let _ = io::stderr().flush();
    ExitCode::from(0)
}

fn expand_target(target: AgentTarget) -> Vec<AgentTarget> {
    match target {
        AgentTarget::All => vec![AgentTarget::Codex, AgentTarget::Claude, AgentTarget::Cursor],
        other => vec![other],
    }
}

fn select_target_interactive(detected: AgentTarget, action: &str) -> Option<AgentTarget> {
    let mut selected = [false, false, false];
    apply_detected_selection(detected, &mut selected);

    let labels = vec![
        format!("Auto-detect (apply: {})", detected.as_str()),
        "codex".to_string(),
        "claude".to_string(),
        "cursor".to_string(),
    ];
    let defaults = vec![false, selected[0], selected[1], selected[2]];
    let prompt = format!(
        "Select coding agents for DTK {} (space=toggle, enter=confirm)",
        action
    );

    let choices = MultiSelect::with_theme(&ColorfulTheme::default())
        .with_prompt(prompt)
        .items(&labels)
        .defaults(&defaults)
        .report(false)
        .interact_opt();

    match choices {
        Ok(Some(indices)) => {
            let mut resolved = [false, false, false];
            let mut apply_auto = false;
            for idx in indices {
                if idx == 0 {
                    apply_auto = true;
                } else if idx <= 3 {
                    resolved[idx - 1] = true;
                }
            }
            if apply_auto {
                apply_detected_selection(detected, &mut resolved);
            }
            Some(selection_to_target(&resolved, detected))
        }
        Ok(None) => Some(detected),
        Err(_) => Some(detected),
    }
}

fn apply_detected_selection(detected: AgentTarget, selected: &mut [bool; 3]) {
    *selected = [false, false, false];
    match detected {
        AgentTarget::All => {
            *selected = [true, true, true];
        }
        AgentTarget::Codex => selected[0] = true,
        AgentTarget::Claude => selected[1] = true,
        AgentTarget::Cursor => selected[2] = true,
    }
}

fn selection_to_target(selected: &[bool; 3], detected: AgentTarget) -> AgentTarget {
    let count = selected.iter().filter(|value| **value).count();
    if count == 0 {
        return detected;
    }
    if count > 1 {
        return AgentTarget::All;
    }
    if selected[0] {
        AgentTarget::Codex
    } else if selected[1] {
        AgentTarget::Claude
    } else {
        AgentTarget::Cursor
    }
}

fn detect_installed_selection() -> [bool; 3] {
    [
        codex_artifacts_present(),
        claude_artifacts_present(),
        cursor_artifacts_present(),
    ]
}

fn codex_artifacts_present() -> bool {
    codex_dir().join("DTK.md").exists()
        || codex_dir()
            .join("skills")
            .join("dtk")
            .join("SKILL.md")
            .exists()
}

fn claude_artifacts_present() -> bool {
    let base = claude_dir();
    if base.join("DTK.md").exists()
        || base.join("hooks").join("dtk-rewrite.sh").exists()
        || base.join("skills").join("dtk").join("SKILL.md").exists()
    {
        return true;
    }
    let claude_md = base.join("CLAUDE.md");
    if let Ok(content) = fs::read_to_string(claude_md) {
        if content.lines().any(|line| line.trim() == "@DTK.md") {
            return true;
        }
    }
    let settings = base.join("settings.json");
    if let Ok(content) = fs::read_to_string(settings) {
        return content.contains("dtk-rewrite.sh");
    }
    false
}

fn cursor_artifacts_present() -> bool {
    let base = cursor_dir();
    if base.join("DTK.md").exists()
        || base.join("hooks").join("dtk-rewrite.sh").exists()
        || base.join("skills").join("dtk").join("SKILL.md").exists()
    {
        return true;
    }
    let hooks = base.join("hooks.json");
    if let Ok(content) = fs::read_to_string(hooks) {
        return content.contains("dtk-rewrite.sh");
    }
    false
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
            "Usage: dtk <install|uninstall|doctor|hook|exec|retrieve|cache|version|help> [--agent all|codex|claude|cursor]";
        assert!(usage.contains("install"));
        assert!(usage.contains("uninstall"));
        assert!(usage.contains("exec"));
        assert!(usage.contains("retrieve"));
        assert!(usage.contains("cache"));
        assert!(usage.contains("version"));
    }
}
