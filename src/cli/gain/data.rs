use rusqlite::Connection;
use serde::Serialize;
use std::collections::BTreeMap;
use std::io;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum GainGroupBy {
    Signature,
    Command,
    Domain,
    Details,
}

impl GainGroupBy {
    pub(super) fn parse(value: &str) -> Option<Self> {
        match value {
            "signature" => Some(Self::Signature),
            "command" => Some(Self::Command),
            "domain" => Some(Self::Domain),
            "details" => Some(Self::Details),
            _ => None,
        }
    }

    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Signature => "signature",
            Self::Command => "command",
            Self::Domain => "domain",
            Self::Details => "details",
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct UsageRecord {
    pub(super) created_at_unix_ms: i64,
    pub(super) command: String,
    pub(super) domain: String,
    pub(super) details: String,
    pub(super) ticket_id: String,
    pub(super) original_tokens: i64,
    pub(super) filtered_tokens: i64,
    pub(super) token_delta: i64,
}

#[derive(Debug, Clone)]
pub(super) struct UsageIssueRecord {
    pub(super) created_at_unix_ms: i64,
    pub(super) command: String,
    pub(super) domain: String,
    pub(super) details: String,
    pub(super) ticket_id: String,
    pub(super) issue_kind: String,
    pub(super) original_tokens: i64,
    pub(super) filtered_tokens: i64,
    pub(super) token_delta: i64,
    pub(super) token_delta_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct GainSummaryJson {
    pub(super) runs: i64,
    pub(super) original_tokens: i64,
    pub(super) filtered_tokens: i64,
    pub(super) token_delta: i64,
    pub(super) saved_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct GainGroupJson {
    pub(super) command: Option<String>,
    pub(super) domain: Option<String>,
    pub(super) details: Option<String>,
    pub(super) runs: i64,
    pub(super) original_tokens: i64,
    pub(super) filtered_tokens: i64,
    pub(super) token_delta: i64,
    pub(super) saved_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct GainPeriodJson {
    pub(super) period: String,
    pub(super) runs: i64,
    pub(super) original_tokens: i64,
    pub(super) filtered_tokens: i64,
    pub(super) token_delta: i64,
    pub(super) saved_pct: f64,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct GainReportJson {
    pub(super) group_by: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) ticket_id: Option<String>,
    pub(super) summary: GainSummaryJson,
    pub(super) groups: Vec<GainGroupJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) issues: Option<Vec<GainIssueJson>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) daily: Option<Vec<GainPeriodJson>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) weekly: Option<Vec<GainPeriodJson>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) monthly: Option<Vec<GainPeriodJson>>,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct GainIssueJson {
    pub(super) command: String,
    pub(super) domain: String,
    pub(super) details: String,
    pub(super) ticket_id: String,
    pub(super) issue_kind: String,
    pub(super) original_tokens: i64,
    pub(super) filtered_tokens: i64,
    pub(super) token_delta: i64,
    pub(super) token_delta_pct: f64,
    pub(super) created_at_unix_ms: i64,
}

#[derive(Debug, Clone)]
pub(super) struct GainRow {
    pub(super) command: Option<String>,
    pub(super) domain: Option<String>,
    pub(super) details: Option<String>,
    pub(super) runs: i64,
    pub(super) original_tokens: i64,
    pub(super) filtered_tokens: i64,
    pub(super) token_delta: i64,
    pub(super) saved_pct: f64,
}

#[derive(Debug, Clone)]
pub(super) struct GainIssueRow {
    pub(super) command: String,
    pub(super) domain: String,
    pub(super) original_tokens: i64,
    pub(super) filtered_tokens: i64,
    pub(super) token_delta: i64,
    pub(super) token_delta_pct: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum GainPeriodKind {
    Daily,
    Weekly,
    Monthly,
}

pub(super) struct GainOptions {
    pub(super) json_output: bool,
    pub(super) group_by: GainGroupBy,
    pub(super) limit: Option<usize>,
    pub(super) ticket_id: Option<String>,
    pub(super) issues: bool,
    pub(super) all: bool,
    pub(super) daily: bool,
    pub(super) weekly: bool,
    pub(super) monthly: bool,
}

pub(super) fn summarize_usage(records: &[UsageRecord]) -> GainSummaryJson {
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

pub(super) fn group_usage_rows(records: &[UsageRecord], group_by: GainGroupBy) -> Vec<GainRow> {
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

pub(super) fn group_row_to_json(row: &GainRow) -> GainGroupJson {
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

pub(super) fn issue_record_to_row(record: &UsageIssueRecord) -> GainIssueRow {
    GainIssueRow {
        command: record.command.clone(),
        domain: record.domain.clone(),
        original_tokens: record.original_tokens,
        filtered_tokens: record.filtered_tokens,
        token_delta: record.token_delta,
        token_delta_pct: record.token_delta_pct,
    }
}

pub(super) fn issue_record_to_json(record: &UsageIssueRecord) -> GainIssueJson {
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

pub(super) fn period_usage_rows(
    records: &[UsageRecord],
    kind: GainPeriodKind,
) -> Vec<GainPeriodJson> {
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

pub(super) fn load_usage_records(connection: &Connection) -> io::Result<Vec<UsageRecord>> {
    let mut statement = connection
        .prepare(
            "SELECT em.created_at_unix_ms, cs.command, cs.domain, cs.details, em.ticket_id, em.original_tokens, em.filtered_tokens, em.token_delta
             FROM exec_metrics em
             JOIN command_signatures cs ON cs.id = em.signature_id
             ORDER BY em.created_at_unix_ms ASC, em.ref_id ASC",
        )
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("prepare usage query: {err}")))?;

    let rows = statement
        .query_map([], |row| {
            Ok(UsageRecord {
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
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("query usage rows: {err}")))?;

    let mut records = Vec::new();
    for row in rows {
        records.push(row.map_err(|err| {
            io::Error::new(io::ErrorKind::Other, format!("read usage row: {err}"))
        })?);
    }

    Ok(records)
}

pub(super) fn load_usage_issues(connection: &Connection) -> io::Result<Vec<UsageIssueRecord>> {
    let mut statement = connection
        .prepare(
            "SELECT emi.created_at_unix_ms, cs.command, cs.domain, cs.details, emi.ticket_id, emi.issue_kind, emi.original_tokens, emi.filtered_tokens, emi.token_delta, emi.token_delta_pct
             FROM exec_metric_issues emi
             JOIN command_signatures cs ON cs.id = emi.signature_id
             ORDER BY emi.created_at_unix_ms DESC, emi.ref_id DESC",
        )
        .map_err(|err| {
            io::Error::new(io::ErrorKind::Other, format!("prepare usage issue query: {err}"))
        })?;

    let rows = statement
        .query_map([], |row| {
            Ok(UsageIssueRecord {
                created_at_unix_ms: row.get(0)?,
                command: row.get(1)?,
                domain: row.get(2)?,
                details: row.get(3)?,
                ticket_id: row.get(4)?,
                issue_kind: row.get(5)?,
                original_tokens: row.get(6)?,
                filtered_tokens: row.get(7)?,
                token_delta: row.get(8)?,
                token_delta_pct: row.get(9)?,
            })
        })
        .map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("query usage issue rows: {err}"),
            )
        })?;

    let mut issues = Vec::new();
    for row in rows {
        issues.push(row.map_err(|err| {
            io::Error::new(io::ErrorKind::Other, format!("read usage issue row: {err}"))
        })?);
    }

    Ok(issues)
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

#[cfg(test)]
mod tests {
    use super::*;

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
