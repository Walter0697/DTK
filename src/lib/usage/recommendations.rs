use super::context::{load_field_access_context, resolved_usage_dir};
use super::schema::init_usage_schema;
use crate::{
    field_is_allowlisted, load_filter_config, normalize_field_path_for_config,
    normalize_repeated_field_path, usage_db_path, ConfigRecommendation, RecommendationThresholds,
};
use rusqlite::{params, Connection};
use std::fs;
use std::io;

pub fn load_config_recommendations(
    store_dir: impl AsRef<std::path::Path>,
    thresholds: RecommendationThresholds,
) -> io::Result<Vec<ConfigRecommendation>> {
    let usage_dir = resolved_usage_dir(store_dir);
    fs::create_dir_all(&usage_dir)?;
    let db_path = usage_db_path(&usage_dir);
    let connection = Connection::open(db_path)
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("open usage db: {err}")))?;
    init_usage_schema(&connection)?;

    let mut recommendations = Vec::new();
    recommendations.extend(load_expand_recommendations(&connection, thresholds)?);
    recommendations.extend(load_fallback_recommendations(&connection, thresholds)?);
    recommendations.sort_by(|left, right| {
        right
            .event_count
            .cmp(&left.event_count)
            .then_with(|| left.recommendation_kind.cmp(&right.recommendation_kind))
            .then_with(|| left.config_id.cmp(&right.config_id))
    });
    Ok(recommendations)
}

pub fn recommendation_notices_for_retrieve(
    store_dir: impl AsRef<std::path::Path>,
    ref_id: &str,
    fields: &[String],
) -> io::Result<Vec<String>> {
    let Some(context) = load_field_access_context(store_dir.as_ref(), ref_id)? else {
        return Ok(Vec::new());
    };

    let config = load_filter_config(&context.config_path).ok();
    let requested = dedup_field_paths(fields)
        .into_iter()
        .filter_map(|field| {
            let normalized = normalize_repeated_field_path(&field).unwrap_or(field);
            match config.as_ref() {
                Some(config) => normalize_field_path_for_config(&normalized, config),
                None => Some(normalized),
            }
        })
        .collect::<Vec<_>>();
    if requested.is_empty() {
        return Ok(Vec::new());
    }

    let recommendations =
        load_config_recommendations(store_dir, RecommendationThresholds::default())?;
    let mut notices = Vec::new();
    for recommendation in recommendations {
        if recommendation.recommendation_kind != "expand_allowlist" {
            continue;
        }
        if recommendation.config_id != context.config_id {
            continue;
        }
        let Some(field_path) = recommendation.field_path.as_deref() else {
            continue;
        };
        if !requested.iter().any(|field| field == field_path) {
            continue;
        }
        notices.push(format!(
            "DTK recommendation: ask the user whether to add `{field_path}` to config `{}`. If they agree, run `dtk config list` to confirm the target config id, then `dtk config allow add <config> <field>`. This field has been requested repeatedly for the same endpoint.",
            recommendation.config_id
        ));
    }

    notices.sort();
    notices.dedup();
    Ok(notices)
}

pub fn recommendation_notices_for_exec(
    store_dir: impl AsRef<std::path::Path>,
    config_id: &str,
    details: &str,
) -> io::Result<Vec<String>> {
    let recommendations =
        load_config_recommendations(store_dir, RecommendationThresholds::default())?;
    let mut notices = Vec::new();
    for recommendation in recommendations {
        if recommendation.config_id != config_id || recommendation.details != details {
            continue;
        }
        match recommendation.recommendation_kind.as_str() {
            "tighten_allowlist" => notices.push(format!(
                "DTK recommendation: ask the user whether to tighten config `{config_id}`. If they agree, run `dtk config list` to confirm the target config id, then use `dtk config allow add/remove <config> <field>` to tighten the config. DTK is falling back repeatedly for this endpoint."
            )),
            "remove_config" => notices.push(format!(
                "DTK recommendation: ask the user whether to remove or disable config `{config_id}` for this endpoint. If they agree, run `dtk config list` to confirm the target config id, then `dtk config delete <config>`. DTK is falling back repeatedly and may not be suitable here."
            )),
            _ => {}
        }
    }

    notices.sort();
    notices.dedup();
    Ok(notices)
}

pub(super) fn dedup_field_paths(fields: &[String]) -> Vec<String> {
    let mut deduped = Vec::new();
    for field in fields {
        let trimmed = field.trim();
        if trimmed.is_empty() {
            continue;
        }
        if deduped.iter().any(|existing: &String| existing == trimmed) {
            continue;
        }
        deduped.push(trimmed.to_string());
    }
    deduped
}

fn load_expand_recommendations(
    connection: &Connection,
    thresholds: RecommendationThresholds,
) -> io::Result<Vec<ConfigRecommendation>> {
    let mut statement = connection
        .prepare(
            "SELECT fa.config_id, fa.config_path, cs.command, cs.domain, cs.details, fa.field_path, COUNT(*) as access_count
             FROM field_access_events fa
             LEFT JOIN command_signatures cs ON cs.id = fa.signature_id
             WHERE fa.config_id != ''
             GROUP BY fa.config_id, fa.config_path, fa.signature_id, fa.field_path
             HAVING COUNT(*) >= ?1
             ORDER BY access_count DESC",
        )
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("prepare expand recommendations: {err}")))?;

    let rows = statement
        .query_map(params![thresholds.expand_field_access_count], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                row.get::<_, Option<String>>(4)?.unwrap_or_default(),
                row.get::<_, String>(5)?,
                row.get::<_, i64>(6)?,
            ))
        })
        .map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("query expand recommendations: {err}"),
            )
        })?;

    let mut recommendations = Vec::new();
    for row in rows {
        let (config_id, config_path, command, domain, details, field_path, access_count) = row
            .map_err(|err| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("read expand recommendation: {err}"),
                )
            })?;
        let config = load_filter_config(&config_path).ok();
        if config
            .as_ref()
            .is_some_and(|config| field_is_allowlisted(config, &field_path))
        {
            continue;
        }
        let display_field_path = config
            .as_ref()
            .and_then(|config| normalize_field_path_for_config(&field_path, config))
            .unwrap_or_else(|| field_path.to_string());

        recommendations.push(ConfigRecommendation {
            config_id: config_id.clone(),
            config_path: config_path.clone(),
            command,
            domain,
            details,
            recommendation_kind: "expand_allowlist".to_string(),
            field_path: Some(display_field_path.clone()),
            event_count: access_count,
            summary: format!(
                "Field `{display_field_path}` was retrieved {access_count} times for config `{config_id}` and may belong in the allowlist."
            ),
        });
    }

    Ok(recommendations)
}

fn load_fallback_recommendations(
    connection: &Connection,
    thresholds: RecommendationThresholds,
) -> io::Result<Vec<ConfigRecommendation>> {
    let mut statement = connection
        .prepare(
            "SELECT emi.config_id, emi.config_path, cs.command, cs.domain, cs.details, COUNT(*) as issue_count
             FROM exec_metric_issues emi
             LEFT JOIN command_signatures cs ON cs.id = emi.signature_id
             WHERE emi.config_id != '' AND emi.issue_kind = 'filtered_larger_than_original'
             GROUP BY emi.config_id, emi.config_path, emi.signature_id
             HAVING COUNT(*) >= ?1
             ORDER BY issue_count DESC",
        )
        .map_err(|err| io::Error::new(io::ErrorKind::Other, format!("prepare fallback recommendations: {err}")))?;

    let rows = statement
        .query_map(params![thresholds.tighten_fallback_count], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, Option<String>>(2)?.unwrap_or_default(),
                row.get::<_, Option<String>>(3)?.unwrap_or_default(),
                row.get::<_, Option<String>>(4)?.unwrap_or_default(),
                row.get::<_, i64>(5)?,
            ))
        })
        .map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("query fallback recommendations: {err}"),
            )
        })?;

    let mut recommendations = Vec::new();
    for row in rows {
        let (config_id, config_path, command, domain, details, issue_count) =
            row.map_err(|err| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("read fallback recommendation: {err}"),
                )
            })?;
        let allow_count = load_filter_config(&config_path)
            .map(|config| config.allow.len())
            .unwrap_or(0);
        let recommendation_kind = if issue_count >= thresholds.remove_fallback_count {
            "remove_config"
        } else if allow_count >= thresholds.tighten_allow_count_min {
            "tighten_allowlist"
        } else {
            "remove_config"
        };
        let summary = if recommendation_kind == "tighten_allowlist" {
            format!(
                "Config `{config_id}` fell back {issue_count} times and exposes {allow_count} allowlist paths; consider shrinking it for this command signature."
            )
        } else {
            format!(
                "Config `{config_id}` fell back {issue_count} times for the same command signature and may not be suitable for this endpoint."
            )
        };

        recommendations.push(ConfigRecommendation {
            config_id,
            config_path,
            command,
            domain,
            details,
            recommendation_kind: recommendation_kind.to_string(),
            field_path: None,
            event_count: issue_count,
            summary,
        });
    }

    Ok(recommendations)
}
