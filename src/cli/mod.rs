mod cache;
mod config;
mod gain;
mod install;
mod session;

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, IsTerminal};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

use dtk::{
    end_session, filtered_payload_path, init_usage_schema, read_store_index, runtime_store_dir,
    runtime_usage_dir, start_session, token_count_for_path, usage_db_path, AgentTarget,
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

struct GainIssueRow {
    command: String,
    domain: String,
    original_tokens: i64,
    filtered_tokens: i64,
    token_delta: i64,
    token_delta_pct: f64,
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
struct UsageRecord {
    created_at_unix_ms: i64,
    command: String,
    domain: String,
    details: String,
    ticket_id: String,
    original_tokens: i64,
    filtered_tokens: i64,
    token_delta: i64,
}

#[derive(Debug, Clone)]
struct UsageIssueRecord {
    created_at_unix_ms: i64,
    command: String,
    domain: String,
    details: String,
    ticket_id: String,
    issue_kind: String,
    original_tokens: i64,
    filtered_tokens: i64,
    token_delta: i64,
    token_delta_pct: f64,
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
    issues: Option<Vec<GainIssueJson>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    daily: Option<Vec<GainPeriodJson>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    weekly: Option<Vec<GainPeriodJson>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    monthly: Option<Vec<GainPeriodJson>>,
}

#[derive(Debug, Clone, Serialize)]
struct GainIssueJson {
    command: String,
    domain: String,
    details: String,
    ticket_id: String,
    issue_kind: String,
    original_tokens: i64,
    filtered_tokens: i64,
    token_delta: i64,
    token_delta_pct: f64,
    created_at_unix_ms: i64,
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
    issues: bool,
    all: bool,
    daily: bool,
    weekly: bool,
    monthly: bool,
}

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
    eprintln!("  dtk retrieve [dtk_retrieve_json args...]");
    eprintln!("  dtk config list");
    eprintln!("  dtk config allow <add|remove> <config> <field>");
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

fn summarize_usage(records: &[UsageRecord]) -> GainSummaryJson {
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

fn group_usage_rows(records: &[UsageRecord], group_by: GainGroupBy) -> Vec<GainRow> {
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

fn issue_record_to_row(record: &UsageIssueRecord) -> GainIssueRow {
    GainIssueRow {
        command: record.command.clone(),
        domain: record.domain.clone(),
        original_tokens: record.original_tokens,
        filtered_tokens: record.filtered_tokens,
        token_delta: record.token_delta,
        token_delta_pct: record.token_delta_pct,
    }
}

fn issue_record_to_json(record: &UsageIssueRecord) -> GainIssueJson {
    GainIssueJson {
        command: record.command.clone(),
        domain: record.domain.clone(),
        details: record.details.clone(),
        ticket_id: record.ticket_id.clone(),
        issue_kind: record.issue_kind.clone(),
        original_tokens: record.original_tokens,
        filtered_tokens: record.filtered_tokens,
        token_delta: record.token_delta,
        token_delta_pct: record.token_delta_pct,
        created_at_unix_ms: record.created_at_unix_ms,
    }
}

fn print_gain_issue_report(rows: &[GainIssueRow], color_enabled: bool) {
    if rows.is_empty() {
        return;
    }

    println!("{}", paint("Recent Fallbacks", "1", color_enabled));
    println!("{}", "─".repeat(74));
    println!(
        "  {:<4}  {:<8}  {:<16}  {:>8}  {:>8}  {:>8}  {:>7}",
        "#", "Command", "Domain", "Input", "Output", "Delta", "Avg%"
    );
    println!("{}", "─".repeat(74));

    for (idx, row) in rows.iter().enumerate() {
        let command = paint(
            &pad_right(&truncate_text(&row.command, 8), 8),
            "34",
            color_enabled,
        );
        let domain = pad_right(&truncate_text(&row.domain, 16), 16);
        let input = pad_left(&compact_number(row.original_tokens), 8);
        let output = pad_left(&compact_number(row.filtered_tokens), 8);
        let delta = pad_left(&compact_number(row.token_delta), 8);
        let avg = paint(
            &pad_left(&format!("{:.1}%", row.token_delta_pct), 7),
            "1;31",
            color_enabled,
        );

        println!(
            "  {:<4}  {}  {}  {}  {}  {}  {}",
            idx + 1,
            command,
            domain,
            input,
            output,
            delta,
            avg,
        );
    }

    println!("{}", "─".repeat(74));
}

fn period_usage_rows(records: &[UsageRecord], kind: GainPeriodKind) -> Vec<GainPeriodJson> {
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

    #[test]
    fn groups_usage_by_domain() {
        let records = vec![
            UsageRecord {
                created_at_unix_ms: 1_715_520_000_000,
                command: "curl".to_string(),
                domain: "dummyjson.com".to_string(),
                details: "curl -sS https://dummyjson.com/users".to_string(),
                ticket_id: "ticket-1".to_string(),
                original_tokens: 100,
                filtered_tokens: 25,
                token_delta: 75,
            },
            UsageRecord {
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

        let grouped = group_usage_rows(&records, GainGroupBy::Domain);
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
