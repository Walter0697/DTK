use std::collections::BTreeMap;
use std::fs;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use dialoguer::{theme::ColorfulTheme, MultiSelect, Select};
use dtk::{
    add_or_update_hook_rule, claude_dir, codex_dir, cursor_dir, default_config_dir, end_session,
    filtered_payload_path, init_telemetry_schema, install_agent_guidance, install_config_skill,
    read_store_index, runtime_store_dir, start_session, telemetry_db_path, token_count_for_path,
    uninstall_agent_guidance, AgentTarget, HookRule,
};
use rusqlite::Connection;
use serde::Serialize;

struct GainRow {
    command: Option<String>,
    domain: Option<String>,
    details: Option<String>,
    runs: i64,
    original_tokens: i64,
    filtered_tokens: i64,
    token_delta: i64,
    saved_pct: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GainGroupBy {
    Signature,
    Command,
    Domain,
    Details,
}

impl GainGroupBy {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "signature" => Some(Self::Signature),
            "command" => Some(Self::Command),
            "domain" => Some(Self::Domain),
            "details" => Some(Self::Details),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Signature => "signature",
            Self::Command => "command",
            Self::Domain => "domain",
            Self::Details => "details",
        }
    }
}

#[derive(Debug, Clone)]
struct TelemetryRecord {
    created_at_unix_ms: i64,
    command: String,
    domain: String,
    details: String,
    ticket_id: String,
    original_tokens: i64,
    filtered_tokens: i64,
    token_delta: i64,
}

#[derive(Debug, Clone, Serialize)]
struct GainSummaryJson {
    runs: i64,
    original_tokens: i64,
    filtered_tokens: i64,
    token_delta: i64,
    saved_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
struct GainGroupJson {
    command: Option<String>,
    domain: Option<String>,
    details: Option<String>,
    runs: i64,
    original_tokens: i64,
    filtered_tokens: i64,
    token_delta: i64,
    saved_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
struct GainPeriodJson {
    period: String,
    runs: i64,
    original_tokens: i64,
    filtered_tokens: i64,
    token_delta: i64,
    saved_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
struct GainReportJson {
    group_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    ticket_id: Option<String>,
    summary: GainSummaryJson,
    groups: Vec<GainGroupJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    daily: Option<Vec<GainPeriodJson>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    weekly: Option<Vec<GainPeriodJson>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    monthly: Option<Vec<GainPeriodJson>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GainPeriodKind {
    Daily,
    Weekly,
    Monthly,
}

struct GainOptions {
    json_output: bool,
    group_by: GainGroupBy,
    limit: Option<usize>,
    ticket_id: Option<String>,
    all: bool,
    daily: bool,
    weekly: bool,
    monthly: bool,
}

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

    if command == "session" {
        return run_session_command(args.collect());
    }

    if command == "gain" {
        return run_gain_command(args.collect());
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
        "Usage: dtk <install|uninstall|doctor|hook|exec|retrieve|cache|session|gain|version|help> [--agent all|codex|claude|cursor]"
    );
    eprintln!("Commands:");
    eprintln!("  dtk exec [dtk_exec args...]");
    eprintln!("  dtk retrieve [dtk_retrieve_json args...]");
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
        let original_tokens = token_count_for_path(Path::new(&entry.path))
            .map(|value| value.to_string())
            .unwrap_or_else(|_| "-".to_string());
        let filtered_tokens = token_count_for_path(filtered_path.as_path())
            .map(|value| value.to_string())
            .unwrap_or_else(|_| "-".to_string());
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

fn run_gain_command(args: Vec<String>) -> ExitCode {
    let mut iter = args.into_iter();
    let mut options = GainOptions {
        json_output: false,
        group_by: GainGroupBy::Signature,
        limit: Some(10),
        ticket_id: None,
        all: false,
        daily: false,
        weekly: false,
        monthly: false,
    };

    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--limit" => {
                let Some(value) = iter.next() else {
                    eprintln!("missing value for --limit");
                    return ExitCode::from(2);
                };
                match value.parse::<usize>() {
                    Ok(parsed) if parsed > 0 => options.limit = Some(parsed),
                    _ => {
                        eprintln!("invalid limit: {value}");
                        return ExitCode::from(2);
                    }
                }
            }
            "--json" => {
                options.json_output = true;
            }
            "--ticket-id" | "--ticketId" => {
                let Some(value) = iter.next() else {
                    eprintln!("missing value for --ticket-id");
                    return ExitCode::from(2);
                };
                if value.trim().is_empty() {
                    eprintln!("invalid ticketId: {value}");
                    return ExitCode::from(2);
                }
                options.ticket_id = Some(value);
            }
            "--group-by" => {
                let Some(value) = iter.next() else {
                    eprintln!("missing value for --group-by");
                    return ExitCode::from(2);
                };
                match GainGroupBy::parse(&value) {
                    Some(parsed) => options.group_by = parsed,
                    None => {
                        eprintln!("invalid group-by value: {value}");
                        print_gain_usage();
                        return ExitCode::from(2);
                    }
                }
            }
            "--all" => {
                options.all = true;
                options.daily = true;
                options.weekly = true;
                options.monthly = true;
                options.limit = None;
            }
            "--daily" => {
                options.daily = true;
            }
            "--weekly" => {
                options.weekly = true;
            }
            "--monthly" => {
                options.monthly = true;
            }
            "--help" | "-h" => {
                print_gain_usage();
                return ExitCode::from(0);
            }
            other => {
                eprintln!("unknown gain argument: {other}");
                print_gain_usage();
                return ExitCode::from(2);
            }
        }
    }

    if !options.all && !options.daily && !options.weekly && !options.monthly {
        options.limit = options.limit.or(Some(10));
    }

    let store_dir = runtime_store_dir();
    let db_path = telemetry_db_path(&store_dir);
    if !db_path.exists() {
        println!("no telemetry entries");
        return ExitCode::from(0);
    }

    let connection = match Connection::open(&db_path) {
        Ok(connection) => connection,
        Err(err) => {
            eprintln!("failed to open telemetry db: {err}");
            return ExitCode::from(1);
        }
    };
    if let Err(err) = init_telemetry_schema(&connection) {
        eprintln!("failed to initialize telemetry db: {err}");
        return ExitCode::from(1);
    }

    let records = match load_telemetry_records(&connection) {
        Ok(records) => records,
        Err(err) => {
            eprintln!("failed to read telemetry data: {err}");
            return ExitCode::from(1);
        }
    };

    let records = if let Some(ticket_id) = options.ticket_id.as_deref() {
        records
            .into_iter()
            .filter(|record| record.ticket_id == ticket_id)
            .collect::<Vec<_>>()
    } else {
        records
    };

    if records.is_empty() {
        println!("no telemetry entries");
        return ExitCode::from(0);
    }

    let summary = summarize_telemetry(&records);
    let color_enabled = supports_color();

    let group_rows = group_telemetry_rows(&records, options.group_by);
    let group_rows = if options.all {
        group_rows
    } else if let Some(limit) = options.limit {
        group_rows.into_iter().take(limit).collect()
    } else {
        group_rows
    };

    if options.json_output {
        let report = GainReportJson {
            group_by: options.group_by.as_str().to_string(),
            ticket_id: options.ticket_id.clone(),
            summary: summary.clone(),
            groups: group_rows.iter().map(group_row_to_json).collect(),
            daily: if options.all || options.daily {
                Some(period_telemetry_rows(&records, GainPeriodKind::Daily))
            } else {
                None
            },
            weekly: if options.all || options.weekly {
                Some(period_telemetry_rows(&records, GainPeriodKind::Weekly))
            } else {
                None
            },
            monthly: if options.all || options.monthly {
                Some(period_telemetry_rows(&records, GainPeriodKind::Monthly))
            } else {
                None
            },
        };

        match serde_json::to_string_pretty(&report) {
            Ok(text) => println!("{text}"),
            Err(err) => {
                eprintln!("failed to render telemetry json: {err}");
                return ExitCode::from(1);
            }
        }

        return ExitCode::from(0);
    }

    if (options.daily || options.weekly || options.monthly) && !options.all {
        if options.daily {
            print_period_section(
                "Daily Breakdown",
                "Date",
                &period_telemetry_rows(&records, GainPeriodKind::Daily),
            );
            println!();
        }
        if options.weekly {
            print_period_section(
                "Weekly Breakdown",
                "Week",
                &period_telemetry_rows(&records, GainPeriodKind::Weekly),
            );
            println!();
        }
        if options.monthly {
            print_period_section(
                "Monthly Breakdown",
                "Month",
                &period_telemetry_rows(&records, GainPeriodKind::Monthly),
            );
        }
        return ExitCode::from(0);
    }

    print_gain_report(
        &summary,
        &group_rows,
        options.group_by,
        options.ticket_id.as_deref(),
        color_enabled,
    );

    if options.all {
        println!();
        print_period_section(
            "Daily Breakdown",
            "Date",
            &period_telemetry_rows(&records, GainPeriodKind::Daily),
        );
        println!();
        print_period_section(
            "Weekly Breakdown",
            "Week",
            &period_telemetry_rows(&records, GainPeriodKind::Weekly),
        );
        println!();
        print_period_section(
            "Monthly Breakdown",
            "Month",
            &period_telemetry_rows(&records, GainPeriodKind::Monthly),
        );
    }

    ExitCode::from(0)
}

fn run_session_command(args: Vec<String>) -> ExitCode {
    let mut iter = args.into_iter();
    let Some(subcommand) = iter.next() else {
        print_session_usage();
        return ExitCode::from(2);
    };

    let mut ticket_id: Option<String> = None;
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--ticket-id" | "--ticketId" => {
                let Some(value) = iter.next() else {
                    eprintln!("missing value for --ticket-id");
                    return ExitCode::from(2);
                };
                if value.trim().is_empty() {
                    eprintln!("invalid ticketId: {value}");
                    return ExitCode::from(2);
                }
                ticket_id = Some(value);
            }
            "--help" | "-h" => {
                print_session_usage();
                return ExitCode::from(0);
            }
            other => {
                eprintln!("unknown session argument: {other}");
                print_session_usage();
                return ExitCode::from(2);
            }
        }
    }

    let store_dir = runtime_store_dir();
    match subcommand.as_str() {
        "start" => match start_session(&store_dir, ticket_id) {
            Ok(session) => {
                println!(
                    "session started: ticketId={} session={}",
                    session.ticket_id, session.id
                );
                ExitCode::from(0)
            }
            Err(err) => {
                eprintln!("failed to start session: {err}");
                ExitCode::from(1)
            }
        },
        "end" => match end_session(&store_dir) {
            Ok(session) => {
                println!(
                    "session ended: ticketId={} session={}",
                    session.ticket_id, session.id
                );
                ExitCode::from(0)
            }
            Err(err) => {
                eprintln!("failed to end session: {err}");
                ExitCode::from(1)
            }
        },
        "--help" | "-h" | "help" => {
            print_session_usage();
            ExitCode::from(0)
        }
        other => {
            eprintln!("unknown session subcommand: {other}");
            print_session_usage();
            ExitCode::from(2)
        }
    }
}

fn print_gain_usage() {
    eprintln!(
        "Usage: dtk gain [--limit N] [--json] [--ticket-id ID|--ticketId ID] [--group-by command|domain|details|signature] [--all|--daily|--weekly|--monthly]"
    );
    eprintln!("  dtk gain");
    eprintln!("  dtk gain --limit 20");
    eprintln!("  dtk gain --json");
    eprintln!("  dtk gain --ticket-id abc123");
    eprintln!("  dtk gain --group-by domain");
    eprintln!("  dtk gain --group-by command");
    eprintln!("  dtk gain --group-by details");
    eprintln!("  dtk gain --group-by signature");
    eprintln!("  dtk gain --all");
    eprintln!("  dtk gain --daily");
    eprintln!("  dtk gain --weekly");
    eprintln!("  dtk gain --monthly");
    eprintln!("Grouping values:");
    eprintln!("  command   group by executable name, like curl or git");
    eprintln!("  domain    group by host for curl URLs, like dummyjson.com");
    eprintln!("  details   group by the normalized full command line");
    eprintln!("  signature group by the full command/domain/details triple");
    eprintln!("Filters:");
    eprintln!("  ticketId  filter by session ticket with --ticket-id or --ticketId");
}

fn print_session_usage() {
    eprintln!("Usage: dtk session <start|end> [--ticket-id ID|--ticketId ID]");
    eprintln!("Session commands:");
    eprintln!("  dtk session start");
    eprintln!("  dtk session start --ticket-id abc123");
    eprintln!("  dtk session start --ticketId abc123");
    eprintln!("  dtk session end");
    eprintln!("  Records the active ticketId on metrics while the session is open.");
}

fn print_gain_report(
    summary: &GainSummaryJson,
    rows: &[GainRow],
    group_by: GainGroupBy,
    ticket_id: Option<&str>,
    color_enabled: bool,
) {
    let savings_pct = summary.saved_pct;
    let title = if ticket_id.is_some() {
        "DTK Token Savings (Ticket Scope)"
    } else {
        "DTK Token Savings (Local Scope)"
    };

    println!("{}", paint(title, "1;36", color_enabled));
    println!("{}", paint(&"═".repeat(56), "36", color_enabled));
    println!();
    print_metric_line("Total runs", &summary.runs.to_string(), color_enabled);
    print_metric_line(
        "Input tokens",
        &compact_number(summary.original_tokens),
        color_enabled,
    );
    print_metric_line(
        "Output tokens",
        &compact_number(summary.filtered_tokens),
        color_enabled,
    );
    print_metric_line(
        "Tokens saved",
        &format!(
            "{} ({savings_pct:.1}%)",
            compact_number(summary.token_delta)
        ),
        color_enabled,
    );
    println!(
        "Efficiency meter: {} {}",
        paint(
            &savings_bar(savings_pct, 24, color_enabled),
            "32",
            color_enabled
        ),
        paint(&format!("{savings_pct:.1}%"), "1;32", color_enabled)
    );
    println!();
    println!("{}", paint("By Group", "1", color_enabled));
    print_gain_table(rows, group_by, color_enabled);
}

fn print_metric_line(label: &str, value: &str, color_enabled: bool) {
    let _ = color_enabled;
    println!("{:<16} {}", label, value);
}

fn print_gain_table(rows: &[GainRow], group_by: GainGroupBy, color_enabled: bool) {
    if rows.is_empty() {
        return;
    }

    let row_width = rows.len().to_string().len().max(1);
    let (label_width, label_header) = match group_by {
        GainGroupBy::Signature => ("Command".len().max("Command".len()), "Command"),
        GainGroupBy::Command => (
            rows.iter()
                .filter_map(|row| row.command.as_deref())
                .map(|value| value.chars().count())
                .max()
                .unwrap_or(7)
                .max("Command".len())
                .min(18),
            "Command",
        ),
        GainGroupBy::Domain => (
            rows.iter()
                .filter_map(|row| row.domain.as_deref())
                .map(|value| value.chars().count())
                .max()
                .unwrap_or(6)
                .max("Domain".len())
                .min(18),
            "Domain",
        ),
        GainGroupBy::Details => (
            rows.iter()
                .filter_map(|row| row.details.as_deref())
                .map(|value| value.chars().count())
                .max()
                .unwrap_or(7)
                .max("Details".len())
                .min(20),
            "Details",
        ),
    };
    let count_width = rows
        .iter()
        .map(|row| row.runs.to_string().len())
        .max()
        .unwrap_or(5)
        .max("Count".len());
    let saved_width = rows
        .iter()
        .map(|row| compact_number(row.token_delta).len())
        .max()
        .unwrap_or(5)
        .max("Saved".len());
    let avg_width = 6usize.max("Avg%".len());
    let impact_width = 10usize.max("Impact".len());

    let table_width = match group_by {
        GainGroupBy::Signature => {
            3 + 2
                + row_width
                + 2
                + 7
                + 2
                + 6
                + 2
                + 7
                + 2
                + count_width
                + 2
                + saved_width
                + 2
                + avg_width
                + 2
                + impact_width
        }
        _ => {
            3 + 2
                + row_width
                + 2
                + label_width
                + 2
                + count_width
                + 2
                + saved_width
                + 2
                + avg_width
                + 2
                + impact_width
        }
    };

    println!("{}", "─".repeat(table_width));
    match group_by {
        GainGroupBy::Signature => {
            println!(
                "  {}  {}  {}  {}  {}  {}  {}  {}",
                pad_right("#", row_width),
                pad_right("Command", 7),
                pad_right("Domain", 6),
                pad_right("Details", 7),
                pad_right("Count", count_width),
                pad_right("Saved", saved_width),
                pad_right("Avg%", avg_width),
                pad_right("Impact", impact_width),
            );
        }
        _ => {
            println!(
                "  {}  {}  {}  {}  {}  {}  {}",
                pad_right("#", row_width),
                pad_right(label_header, label_width),
                pad_right("Count", count_width),
                pad_right("Saved", saved_width),
                pad_right("Avg%", avg_width),
                pad_right("Impact", impact_width),
                "",
            );
        }
    }
    println!("{}", "─".repeat(table_width));

    for (idx, row) in rows.iter().enumerate() {
        let impact = savings_bar(row.saved_pct, impact_width.min(10), color_enabled);
        let _count = pad_left(&row.runs.to_string(), count_width);
        let count = pad_left(&row.runs.to_string(), count_width);
        let saved = pad_left(&compact_number(row.token_delta), saved_width);
        let avg = pad_left(&format!("{:.1}%", row.saved_pct), avg_width);

        match group_by {
            GainGroupBy::Signature => {
                let command =
                    pad_right(&truncate_text(row.command.as_deref().unwrap_or("-"), 18), 7);
                let domain = pad_right(&truncate_text(row.domain.as_deref().unwrap_or("-"), 6), 6);
                let details =
                    pad_right(&truncate_text(row.details.as_deref().unwrap_or("-"), 7), 7);
                println!(
                    "  {}  {}  {}  {}  {}  {}  {}  {}",
                    pad_left(&(idx + 1).to_string(), row_width),
                    paint(&command, "34", color_enabled),
                    domain,
                    details,
                    count,
                    paint(&saved, "1;32", color_enabled),
                    paint(&avg, "1;32", color_enabled),
                    impact,
                );
            }
            GainGroupBy::Command => {
                let value = pad_right(
                    &truncate_text(row.command.as_deref().unwrap_or("-"), label_width),
                    label_width,
                );
                println!(
                    "  {}  {}  {}  {}  {}  {}",
                    pad_left(&(idx + 1).to_string(), row_width),
                    paint(&value, "34", color_enabled),
                    count,
                    paint(&saved, "1;32", color_enabled),
                    paint(&avg, "1;32", color_enabled),
                    impact,
                );
            }
            GainGroupBy::Domain => {
                let value = pad_right(
                    &truncate_text(row.domain.as_deref().unwrap_or("-"), label_width),
                    label_width,
                );
                println!(
                    "  {}  {}  {}  {}  {}  {}",
                    pad_left(&(idx + 1).to_string(), row_width),
                    value,
                    count,
                    paint(&saved, "1;32", color_enabled),
                    paint(&avg, "1;32", color_enabled),
                    impact,
                );
            }
            GainGroupBy::Details => {
                let value = pad_right(
                    &truncate_text(row.details.as_deref().unwrap_or("-"), label_width),
                    label_width,
                );
                println!(
                    "  {}  {}  {}  {}  {}  {}",
                    pad_left(&(idx + 1).to_string(), row_width),
                    value,
                    count,
                    paint(&saved, "1;32", color_enabled),
                    paint(&avg, "1;32", color_enabled),
                    impact,
                );
            }
        }
    }

    println!("{}", "─".repeat(table_width));
}

fn load_telemetry_records(connection: &Connection) -> io::Result<Vec<TelemetryRecord>> {
    let mut statement = connection
        .prepare(
            "SELECT em.created_at_unix_ms, cs.command, cs.domain, cs.details, em.ticket_id, em.original_tokens, em.filtered_tokens, em.token_delta
             FROM exec_metrics em
             JOIN command_signatures cs ON cs.id = em.signature_id
             ORDER BY em.created_at_unix_ms ASC, em.ref_id ASC",
        )
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("prepare telemetry query: {err}")))?;

    let rows = statement
        .query_map([], |row| {
            Ok(TelemetryRecord {
                created_at_unix_ms: row.get(0)?,
                command: row.get(1)?,
                domain: row.get(2)?,
                details: row.get(3)?,
                ticket_id: row.get(4)?,
                original_tokens: row.get(5)?,
                filtered_tokens: row.get(6)?,
                token_delta: row.get(7)?,
            })
        })
        .map_err(|err| {
            io::Error::new(io::ErrorKind::Other, format!("query telemetry rows: {err}"))
        })?;

    let mut records = Vec::new();
    for row in rows {
        records.push(row.map_err(|err| {
            io::Error::new(io::ErrorKind::Other, format!("read telemetry row: {err}"))
        })?);
    }

    Ok(records)
}

fn summarize_telemetry(records: &[TelemetryRecord]) -> GainSummaryJson {
    let runs = records.len() as i64;
    let original_tokens = records.iter().map(|row| row.original_tokens).sum::<i64>();
    let filtered_tokens = records.iter().map(|row| row.filtered_tokens).sum::<i64>();
    let token_delta = records.iter().map(|row| row.token_delta).sum::<i64>();
    let saved_pct = if original_tokens > 0 {
        (token_delta as f64 / original_tokens as f64) * 100.0
    } else {
        0.0
    };

    GainSummaryJson {
        runs,
        original_tokens,
        filtered_tokens,
        token_delta,
        saved_pct,
    }
}

fn group_telemetry_rows(records: &[TelemetryRecord], group_by: GainGroupBy) -> Vec<GainRow> {
    let mut groups: BTreeMap<String, GainRow> = BTreeMap::new();

    for record in records {
        let (command, domain, details, key) = match group_by {
            GainGroupBy::Signature => (
                Some(record.command.clone()),
                Some(record.domain.clone()).filter(|value| !value.is_empty()),
                Some(record.details.clone()),
                format!(
                    "{}\u{0}{}\u{0}{}",
                    record.command, record.domain, record.details
                ),
            ),
            GainGroupBy::Command => (
                Some(record.command.clone()),
                None,
                None,
                record.command.clone(),
            ),
            GainGroupBy::Domain => (
                None,
                Some(record.domain.clone()).filter(|value| !value.is_empty()),
                None,
                record.domain.clone(),
            ),
            GainGroupBy::Details => (
                None,
                None,
                Some(record.details.clone()),
                record.details.clone(),
            ),
        };

        let entry = groups.entry(key).or_insert_with(|| GainRow {
            command,
            domain,
            details,
            runs: 0,
            original_tokens: 0,
            filtered_tokens: 0,
            token_delta: 0,
            saved_pct: 0.0,
        });

        entry.runs += 1;
        entry.original_tokens += record.original_tokens;
        entry.filtered_tokens += record.filtered_tokens;
        entry.token_delta += record.token_delta;
    }

    let mut rows: Vec<GainRow> = groups
        .into_values()
        .map(|mut row| {
            row.saved_pct = if row.original_tokens > 0 {
                (row.token_delta as f64 / row.original_tokens as f64) * 100.0
            } else {
                0.0
            };
            row
        })
        .collect();

    rows.sort_by(|left, right| {
        right
            .token_delta
            .cmp(&left.token_delta)
            .then(right.runs.cmp(&left.runs))
            .then(left.command.cmp(&right.command))
            .then(left.domain.cmp(&right.domain))
            .then(left.details.cmp(&right.details))
    });

    rows
}

fn group_row_to_json(row: &GainRow) -> GainGroupJson {
    GainGroupJson {
        command: row.command.clone(),
        domain: row.domain.clone(),
        details: row.details.clone(),
        runs: row.runs,
        original_tokens: row.original_tokens,
        filtered_tokens: row.filtered_tokens,
        token_delta: row.token_delta,
        saved_pct: row.saved_pct,
    }
}

fn period_telemetry_rows(records: &[TelemetryRecord], kind: GainPeriodKind) -> Vec<GainPeriodJson> {
    let mut groups: BTreeMap<String, GainPeriodJson> = BTreeMap::new();

    for record in records {
        let period = match kind {
            GainPeriodKind::Daily => format_utc_date(record.created_at_unix_ms),
            GainPeriodKind::Weekly => format_utc_week_range(record.created_at_unix_ms),
            GainPeriodKind::Monthly => format_utc_month(record.created_at_unix_ms),
        };

        let entry = groups
            .entry(period.clone())
            .or_insert_with(|| GainPeriodJson {
                period,
                runs: 0,
                original_tokens: 0,
                filtered_tokens: 0,
                token_delta: 0,
                saved_pct: 0.0,
            });

        entry.runs += 1;
        entry.original_tokens += record.original_tokens;
        entry.filtered_tokens += record.filtered_tokens;
        entry.token_delta += record.token_delta;
    }

    let mut rows: Vec<GainPeriodJson> = groups
        .into_values()
        .map(|mut row| {
            row.saved_pct = if row.original_tokens > 0 {
                (row.token_delta as f64 / row.original_tokens as f64) * 100.0
            } else {
                0.0
            };
            row
        })
        .collect();

    rows.sort_by(|left, right| left.period.cmp(&right.period));
    rows
}

fn print_period_section(title: &str, period_label: &str, rows: &[GainPeriodJson]) {
    println!("{}", title);
    println!("{}", "─".repeat(72));
    println!(
        "  {:<12}  {:>5}  {:>12}  {:>12}  {:>12}  {:>6}",
        period_label, "Runs", "Input", "Output", "Saved", "Avg%"
    );
    println!("{}", "─".repeat(72));

    for row in rows {
        println!(
            "  {:<12}  {:>5}  {:>12}  {:>12}  {:>12}  {:>6.1}%",
            truncate_text(&row.period, 12),
            row.runs,
            compact_number(row.original_tokens),
            compact_number(row.filtered_tokens),
            compact_number(row.token_delta),
            row.saved_pct,
        );
    }

    println!("{}", "─".repeat(72));
}

fn format_utc_date(unix_ms: i64) -> String {
    let days = unix_ms.div_euclid(86_400_000);
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02}")
}

fn format_utc_month(unix_ms: i64) -> String {
    let days = unix_ms.div_euclid(86_400_000);
    let (year, month, _) = civil_from_days(days);
    format!("{year:04}-{month:02}")
}

fn format_utc_week_range(unix_ms: i64) -> String {
    let days = unix_ms.div_euclid(86_400_000);
    let weekday = (days + 4).rem_euclid(7);
    let start_days = days - weekday;
    let end_days = start_days + 6;
    let (start_year, start_month, start_day) = civil_from_days(start_days);
    let (end_year, end_month, end_day) = civil_from_days(end_days);
    format!(
        "{start_year:04}-{start_month:02}-{start_day:02}..{end_year:04}-{end_month:02}-{end_day:02}"
    )
}

fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if month <= 2 { 1 } else { 0 };
    (year as i32, month as u32, day as u32)
}

fn savings_bar(percent: f64, width: usize, color_enabled: bool) -> String {
    let filled = ((percent / 100.0) * width as f64).round() as usize;
    let filled = filled.min(width);
    let empty = width.saturating_sub(filled);
    let filled_part = paint(&"█".repeat(filled), "32", color_enabled);
    let empty_part = "░".repeat(empty);
    format!("{filled_part}{empty_part}")
}

fn compact_number(value: i64) -> String {
    let abs = value.abs() as f64;
    if abs >= 1_000_000.0 {
        format!("{:.1}M", value as f64 / 1_000_000.0)
    } else if abs >= 1_000.0 {
        format!("{:.1}K", value as f64 / 1_000.0)
    } else {
        value.to_string()
    }
}

fn truncate_text(value: &str, max_chars: usize) -> String {
    let chars: Vec<char> = value.chars().collect();
    if chars.len() <= max_chars {
        return value.to_string();
    }

    let keep = max_chars.saturating_sub(1);
    chars[..keep].iter().collect::<String>() + "…"
}

fn pad_right(value: &str, width: usize) -> String {
    format!("{value:<width$}")
}

fn pad_left(value: &str, width: usize) -> String {
    format!("{value:>width$}")
}

fn supports_color() -> bool {
    io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none()
}

fn paint(text: &str, code: &str, enabled: bool) -> String {
    if enabled {
        format!("\x1b[{code}m{text}\x1b[0m")
    } else {
        text.to_string()
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
    use super::*;

    #[test]
    fn usage_mentions_install_and_uninstall() {
        let usage =
            "Usage: dtk <install|uninstall|doctor|hook|exec|retrieve|cache|session|gain|version|help> [--agent all|codex|claude|cursor]";
        assert!(usage.contains("install"));
        assert!(usage.contains("uninstall"));
        assert!(usage.contains("exec"));
        assert!(usage.contains("retrieve"));
        assert!(usage.contains("cache"));
        assert!(usage.contains("session"));
        assert!(usage.contains("gain"));
        assert!(usage.contains("version"));
    }

    #[test]
    fn groups_telemetry_by_domain() {
        let records = vec![
            TelemetryRecord {
                created_at_unix_ms: 1_715_520_000_000,
                command: "curl".to_string(),
                domain: "dummyjson.com".to_string(),
                details: "curl -sS https://dummyjson.com/users".to_string(),
                ticket_id: "ticket-1".to_string(),
                original_tokens: 100,
                filtered_tokens: 25,
                token_delta: 75,
            },
            TelemetryRecord {
                created_at_unix_ms: 1_715_520_100_000,
                command: "git".to_string(),
                domain: String::new(),
                details: "git status".to_string(),
                ticket_id: String::new(),
                original_tokens: 80,
                filtered_tokens: 20,
                token_delta: 60,
            },
        ];

        let grouped = group_telemetry_rows(&records, GainGroupBy::Domain);
        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped[0].domain.as_deref(), Some("dummyjson.com"));
        assert_eq!(grouped[0].runs, 1);
    }

    #[test]
    fn formats_utc_day_and_month() {
        assert_eq!(format_utc_date(0), "1970-01-01");
        assert_eq!(format_utc_month(0), "1970-01");
    }
}
