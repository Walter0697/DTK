use super::data::{GainGroupBy, GainIssueRow, GainPeriodJson, GainRow, GainSummaryJson};
use std::io::{self, IsTerminal};

pub(super) fn print_gain_report(
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

pub(super) fn print_gain_issue_report(rows: &[GainIssueRow], color_enabled: bool) {
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

pub(super) fn print_period_section(title: &str, period_label: &str, rows: &[GainPeriodJson]) {
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

pub(super) fn supports_color() -> bool {
    io::stdout().is_terminal() && std::env::var_os("NO_COLOR").is_none()
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

fn paint(text: &str, code: &str, enabled: bool) -> String {
    if enabled {
        format!("\x1b[{code}m{text}\x1b[0m")
    } else {
        text.to_string()
    }
}
