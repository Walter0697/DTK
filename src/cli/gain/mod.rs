mod data;
mod render;

use std::process::ExitCode;

use dtk::{init_usage_schema, runtime_usage_dir, usage_db_path};
use rusqlite::Connection;

use data::{
    group_row_to_json, group_usage_rows, issue_record_to_json, issue_record_to_row,
    load_usage_issues, load_usage_records, period_usage_rows, summarize_usage, GainGroupBy,
    GainOptions, GainPeriodKind, GainReportJson,
};
use render::{print_gain_issue_report, print_gain_report, print_period_section, supports_color};

pub(super) fn run_gain_command(args: Vec<String>) -> ExitCode {
    let mut iter = args.into_iter();
    let mut options = GainOptions {
        json_output: false,
        group_by: GainGroupBy::Signature,
        limit: Some(10),
        ticket_id: None,
        issues: false,
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
            "--issues" => {
                options.issues = true;
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
            "--daily" => options.daily = true,
            "--weekly" => options.weekly = true,
            "--monthly" => options.monthly = true,
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

    let usage_dir = runtime_usage_dir();
    let db_path = usage_db_path(&usage_dir);
    if !db_path.exists() {
        println!("no usage entries");
        return ExitCode::from(0);
    }

    let connection = match Connection::open(&db_path) {
        Ok(connection) => connection,
        Err(err) => {
            eprintln!("failed to open usage db: {err}");
            return ExitCode::from(1);
        }
    };
    if let Err(err) = init_usage_schema(&connection) {
        eprintln!("failed to initialize usage db: {err}");
        return ExitCode::from(1);
    }

    let records = match load_usage_records(&connection) {
        Ok(records) => records,
        Err(err) => {
            eprintln!("failed to read usage data: {err}");
            return ExitCode::from(1);
        }
    };

    let issues = match load_usage_issues(&connection) {
        Ok(issues) => issues,
        Err(err) => {
            eprintln!("failed to read usage issues: {err}");
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

    let issues = if let Some(ticket_id) = options.ticket_id.as_deref() {
        issues
            .into_iter()
            .filter(|issue| issue.ticket_id == ticket_id)
            .collect::<Vec<_>>()
    } else {
        issues
    };

    if records.is_empty() {
        println!("no usage entries");
        return ExitCode::from(0);
    }

    let summary = summarize_usage(&records);
    let color_enabled = supports_color();

    let group_rows = group_usage_rows(&records, options.group_by);
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
            issues: if options.issues || !issues.is_empty() {
                Some(issues.iter().take(5).map(issue_record_to_json).collect())
            } else {
                None
            },
            daily: if options.all || options.daily {
                Some(period_usage_rows(&records, GainPeriodKind::Daily))
            } else {
                None
            },
            weekly: if options.all || options.weekly {
                Some(period_usage_rows(&records, GainPeriodKind::Weekly))
            } else {
                None
            },
            monthly: if options.all || options.monthly {
                Some(period_usage_rows(&records, GainPeriodKind::Monthly))
            } else {
                None
            },
        };

        match serde_json::to_string_pretty(&report) {
            Ok(text) => println!("{text}"),
            Err(err) => {
                eprintln!("failed to render usage json: {err}");
                return ExitCode::from(1);
            }
        }

        return ExitCode::from(0);
    }

    if options.issues {
        if issues.is_empty() {
            println!("no fallback issues");
            return ExitCode::from(0);
        }
        let issue_rows = issues
            .iter()
            .take(5)
            .map(issue_record_to_row)
            .collect::<Vec<_>>();
        print_gain_issue_report(&issue_rows, color_enabled);
        return ExitCode::from(0);
    }

    if (options.daily || options.weekly || options.monthly) && !options.all {
        if options.daily {
            print_period_section(
                "Daily Breakdown",
                "Date",
                &period_usage_rows(&records, GainPeriodKind::Daily),
            );
            println!();
        }
        if options.weekly {
            print_period_section(
                "Weekly Breakdown",
                "Week",
                &period_usage_rows(&records, GainPeriodKind::Weekly),
            );
            println!();
        }
        if options.monthly {
            print_period_section(
                "Monthly Breakdown",
                "Month",
                &period_usage_rows(&records, GainPeriodKind::Monthly),
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

    let recent_issue_rows = issues
        .iter()
        .take(5)
        .map(issue_record_to_row)
        .collect::<Vec<_>>();
    if !recent_issue_rows.is_empty() {
        println!();
        print_gain_issue_report(&recent_issue_rows, color_enabled);
    }

    if options.all {
        println!();
        print_period_section(
            "Daily Breakdown",
            "Date",
            &period_usage_rows(&records, GainPeriodKind::Daily),
        );
        println!();
        print_period_section(
            "Weekly Breakdown",
            "Week",
            &period_usage_rows(&records, GainPeriodKind::Weekly),
        );
        println!();
        print_period_section(
            "Monthly Breakdown",
            "Month",
            &period_usage_rows(&records, GainPeriodKind::Monthly),
        );
    }

    ExitCode::from(0)
}

fn print_gain_usage() {
    eprintln!(
        "Usage: dtk gain [--limit N] [--json] [--issues] [--ticket-id ID|--ticketId ID] [--group-by command|domain|details|signature] [--all|--daily|--weekly|--monthly]"
    );
    eprintln!("  dtk gain");
    eprintln!("  dtk gain --limit 20");
    eprintln!("  dtk gain --json");
    eprintln!("  dtk gain --issues");
    eprintln!("  dtk gain --ticket-id abc123");
    eprintln!("  dtk gain --group-by domain");
    eprintln!("  dtk gain --group-by command");
    eprintln!("  dtk gain --group-by details");
    eprintln!("  dtk gain --group-by signature");
    eprintln!("  dtk gain --all");
    eprintln!("  dtk gain --daily");
    eprintln!("  dtk gain --weekly");
    eprintln!("  dtk gain --monthly");
    eprintln!(
        "  --issues  show fallback cases where parsed output was larger than original; pair with --ticket-id to inspect a session"
    );
    eprintln!("Grouping values:");
    eprintln!("  command   group by executable name, like curl or git");
    eprintln!("  domain    group by host for curl URLs, like dummyjson.com");
    eprintln!("  details   group by the normalized full command line");
    eprintln!("  signature group by the full command/domain/details triple");
    eprintln!("Filters:");
    eprintln!("  ticketId  filter by session ticket with --ticket-id or --ticketId");
}
