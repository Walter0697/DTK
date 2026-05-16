use std::path::PathBuf;

use super::{
    apply_pii_transform, cleanup_expired_payloads, collect_field_paths, default_store_dir,
    end_session, field_is_allowlisted, filter_json_payload, filter_json_payload_with_metadata,
    is_json_payload, is_structured_payload, load_config_recommendations, load_filter_config,
    normalize_field_path_for_config, parse_json_payload, parse_structured_format,
    parse_structured_payload, parse_structured_payload_with_hint, platform_data_dir,
    preview_expired_payloads, recommendation_notices_for_exec, recommendation_notices_for_retrieve,
    record_exec_metrics, record_field_access, recover_original_payload, resolve_filter_config_id,
    retrieve_json_payload, retrieve_original_payload, runtime_store_dir, stable_ref_id,
    start_session, store_original_payload, store_original_payload_with_retention,
    summarize_command_signature, usage_db_path, windows_data_dir, xdg_data_dir, ExecMetricsInput,
    FieldAccessRecordInput, FilterConfig, RecommendationThresholds, StructuredFormat,
};

fn temp_store_dir(name: &str) -> PathBuf {
    std::env::temp_dir().join("dtk-tests").join(name)
}

fn now_unix_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

#[test]
fn detects_object() {
    assert!(is_json_payload(r#"{"name":"dtk","count":1}"#));
}

#[test]
fn detects_array() {
    assert!(is_json_payload(r#"[1, 2, 3]"#));
}

#[test]
fn rejects_plain_text() {
    assert!(!is_json_payload("hello world"));
}

#[test]
fn rejects_empty_text() {
    assert!(!is_json_payload("   \n\t  "));
}

#[test]
fn rejects_json_primitives() {
    assert!(!is_json_payload(r#""string""#));
    assert!(!is_json_payload("42"));
    assert!(!is_json_payload("true"));
}

#[test]
fn detects_structured_yaml_payload() {
    let payload = "users:\n  - firstName: Emily\n    age: 28\n";

    assert!(is_structured_payload(payload));
}

#[test]
fn parses_structured_yaml_payload() {
    let payload = "users:\n  - firstName: Emily\n    age: 28\n";

    let parsed = parse_structured_payload(payload);

    assert_eq!(
        parsed,
        Some(serde_json::json!({
            "users": [
                {
                    "firstName": "Emily",
                    "age": 28
                }
            ]
        }))
    );
}

#[test]
fn rejects_yaml_primitives_as_structured_payloads() {
    assert!(!is_structured_payload("hello"));
    assert!(parse_structured_payload("hello").is_none());
}

#[test]
fn parses_structured_format_aliases() {
    assert_eq!(
        parse_structured_format("json"),
        Some(StructuredFormat::Json)
    );
    assert_eq!(
        parse_structured_format("yaml"),
        Some(StructuredFormat::Yaml)
    );
    assert_eq!(parse_structured_format("yml"), Some(StructuredFormat::Yaml));
    assert_eq!(
        parse_structured_format("toml"),
        Some(StructuredFormat::Toml)
    );
    assert_eq!(parse_structured_format("hcl"), Some(StructuredFormat::Hcl));
    assert_eq!(parse_structured_format("tf"), Some(StructuredFormat::Hcl));
    assert_eq!(parse_structured_format("csv"), Some(StructuredFormat::Csv));
    assert_eq!(parse_structured_format("ini"), Some(StructuredFormat::Ini));
    assert_eq!(
        parse_structured_format("xaml"),
        Some(StructuredFormat::Xaml)
    );
    assert_eq!(parse_structured_format("xml"), Some(StructuredFormat::Xaml));
}

#[test]
fn respects_yaml_format_hint() {
    let payload = "users:\n  - firstName: Emily\n    age: 28\n";

    let parsed = parse_structured_payload_with_hint(payload, Some(StructuredFormat::Yaml));

    assert_eq!(
        parsed,
        Some(serde_json::json!({
            "users": [
                {
                    "firstName": "Emily",
                    "age": 28
                }
            ]
        }))
    );
}

#[test]
fn json_format_hint_does_not_fallback_to_yaml() {
    let payload = "users:\n  - firstName: Emily\n    age: 28\n";

    assert!(parse_structured_payload_with_hint(payload, Some(StructuredFormat::Json)).is_none());
}

#[test]
fn detects_structured_hcl_payload() {
    let payload = r#"
variable "app_name" {
  type = string
}
"#;

    assert!(is_structured_payload(payload));
}

#[test]
fn parses_structured_hcl_payload() {
    let payload = r#"
variable "app_name" {
  type        = string
  default     = "atlas-platform"
  description = "Primary application name used in release metadata."
  nullable    = false
}

variable "desired_capacity" {
  type        = number
  default     = 4
  description = "Target number of instances to keep online."
  nullable    = false
}
"#;

    let parsed = parse_structured_payload_with_hint(payload, Some(StructuredFormat::Hcl));

    assert_eq!(
        parsed,
        Some(serde_json::json!({
            "variable": [
                {
                    "name": "app_name",
                    "type": "string",
                    "default": "atlas-platform",
                    "description": "Primary application name used in release metadata.",
                    "nullable": false
                },
                {
                    "name": "desired_capacity",
                    "type": "number",
                    "default": 4,
                    "description": "Target number of instances to keep online.",
                    "nullable": false
                }
            ]
        }))
    );
}

#[test]
fn parses_structured_toml_payload() {
    let payload = r#"
version = 4

[[package]]
name = "dtk"
version = "0.0.2"
source = "path+file:///home/git/DTK"
dependencies = ["serde", "toml"]
"#;

    let parsed = parse_structured_payload(payload);

    assert_eq!(
        parsed,
        Some(serde_json::json!({
            "version": 4,
            "package": [
                {
                    "name": "dtk",
                    "version": "0.0.2",
                    "source": "path+file:///home/git/DTK",
                    "dependencies": [
                        "serde",
                        "toml"
                    ]
                }
            ]
        }))
    );
}

#[test]
fn respects_toml_format_hint() {
    let payload = r#"
[[package]]
name = "dtk"
version = "0.0.2"
"#;

    let parsed = parse_structured_payload_with_hint(payload, Some(StructuredFormat::Toml));

    assert_eq!(
        parsed,
        Some(serde_json::json!({
            "package": [
                {
                    "name": "dtk",
                    "version": "0.0.2"
                }
            ]
        }))
    );
}

#[test]
fn json_format_hint_does_not_fallback_to_toml() {
    let payload = r#"
[[package]]
name = "dtk"
version = "0.0.2"
"#;

    assert!(parse_structured_payload_with_hint(payload, Some(StructuredFormat::Json)).is_none());
}

#[test]
fn parses_structured_csv_payload() {
    let payload = r#"
sku,name,warehouse,region,status,quantity,unit_cost,retail_price
SKU-1001,Universal Adapter,SEA-01,us-west,active,48,4.20,9.99
SKU-1002,Cable Kit,SEA-01,us-west,active,72,1.75,4.99
SKU-1003,Notebook Pack,DAL-02,us-central,active,110,2.10,5.49
"#;

    let parsed = parse_structured_payload_with_hint(payload, Some(StructuredFormat::Csv));

    assert_eq!(
        parsed,
        Some(serde_json::json!({
            "rows": [
                {
                    "sku": "SKU-1001",
                    "name": "Universal Adapter",
                    "warehouse": "SEA-01",
                    "region": "us-west",
                    "status": "active",
                    "quantity": "48",
                    "unit_cost": "4.20",
                    "retail_price": "9.99"
                },
                {
                    "sku": "SKU-1002",
                    "name": "Cable Kit",
                    "warehouse": "SEA-01",
                    "region": "us-west",
                    "status": "active",
                    "quantity": "72",
                    "unit_cost": "1.75",
                    "retail_price": "4.99"
                },
                {
                    "sku": "SKU-1003",
                    "name": "Notebook Pack",
                    "warehouse": "DAL-02",
                    "region": "us-central",
                    "status": "active",
                    "quantity": "110",
                    "unit_cost": "2.10",
                    "retail_price": "5.49"
                }
            ]
        }))
    );
}

#[test]
fn detects_structured_csv_payload() {
    let payload = r#"
sku,name,warehouse,region,status,quantity
SKU-1001,Universal Adapter,SEA-01,us-west,active,48
SKU-1002,Cable Kit,SEA-01,us-west,active,72
"#;

    assert!(parse_structured_payload(payload).is_some());
}

#[test]
fn csv_format_hint_does_not_fallback_to_json() {
    let payload = r#"{"sku":"SKU-1001","name":"Universal Adapter"}"#;

    assert!(parse_structured_payload_with_hint(payload, Some(StructuredFormat::Csv)).is_none());
}

#[test]
fn parses_structured_ini_payload() {
    let payload = r#"
[plugin]
name = "telemetry"
enabled = true
channel = "stable"
notes = "keep the telemetry exporter enabled for desktop sync and verify rollout health before every release"

[plugin]
name = "theme"
enabled = false
channel = "beta"
notes = "keep the theme pack disabled until brand review and accessibility validation are complete"

[plugin]
name = "sync"
enabled = true
channel = "stable"
notes = "keep the sync worker enabled for the internal desktop fleet and watch conflict handling"
"#;

    let parsed = parse_structured_payload(payload);

    assert_eq!(
        parsed,
        Some(serde_json::json!({
            "plugin": [
                {
                    "name": "telemetry",
                    "enabled": true,
                    "channel": "stable",
                    "notes": "keep the telemetry exporter enabled for desktop sync and verify rollout health before every release"
                },
                {
                    "name": "theme",
                    "enabled": false,
                    "channel": "beta",
                    "notes": "keep the theme pack disabled until brand review and accessibility validation are complete"
                },
                {
                    "name": "sync",
                    "enabled": true,
                    "channel": "stable",
                    "notes": "keep the sync worker enabled for the internal desktop fleet and watch conflict handling"
                }
            ]
        }))
    );
}

#[test]
fn respects_ini_format_hint() {
    let payload = r#"
[plugin]
name = "telemetry"
enabled = true
"#;

    let parsed = parse_structured_payload_with_hint(payload, Some(StructuredFormat::Ini));

    assert_eq!(
        parsed,
        Some(serde_json::json!({
            "plugin": {
                "name": "telemetry",
                "enabled": true
            }
        }))
    );
}

#[test]
fn json_format_hint_does_not_fallback_to_ini() {
    let payload = r#"
[plugin]
name = "telemetry"
enabled = true
"#;

    assert!(parse_structured_payload_with_hint(payload, Some(StructuredFormat::Json)).is_none());
}

#[test]
fn parses_structured_xaml_payload() {
    let payload = r#"
<ResourceDictionary xmlns="http://schemas.microsoft.com/winfx/2006/xaml/presentation"
                    xmlns:x="http://schemas.microsoft.com/winfx/2006/xaml">
  <Color x:Key="PrimaryColor">#1E40AF</Color>
  <Style x:Key="PrimaryButtonStyle" TargetType="Button">
    <Setter Property="Background" Value="{StaticResource PrimaryColor}" />
  </Style>
</ResourceDictionary>
"#;

    let parsed = parse_structured_payload(payload);

    assert_eq!(
        parsed,
        Some(serde_json::json!({
            "ResourceDictionary": {
                "Color": {
                    "Key": "PrimaryColor",
                    "text": "#1E40AF"
                },
                "Style": {
                    "Setter": {
                        "Property": "Background",
                        "Value": "{StaticResource PrimaryColor}"
                    },
                    "Key": "PrimaryButtonStyle",
                    "TargetType": "Button",
                }
            }
        }))
    );
}

#[test]
fn respects_xaml_format_hint() {
    let payload = r#"
<ResourceDictionary xmlns="http://schemas.microsoft.com/winfx/2006/xaml/presentation"
                    xmlns:x="http://schemas.microsoft.com/winfx/2006/xaml">
  <Color x:Key="PrimaryColor">#1E40AF</Color>
</ResourceDictionary>
"#;

    let parsed = parse_structured_payload_with_hint(payload, Some(StructuredFormat::Xaml));

    assert_eq!(
        parsed,
        Some(serde_json::json!({
            "ResourceDictionary": {
                "Color": {
                    "Key": "PrimaryColor",
                    "text": "#1E40AF"
                }
            }
        }))
    );
}

#[test]
fn json_format_hint_does_not_fallback_to_xaml() {
    let payload = r#"
<ResourceDictionary xmlns="http://schemas.microsoft.com/winfx/2006/xaml/presentation"
                    xmlns:x="http://schemas.microsoft.com/winfx/2006/xaml">
  <Color x:Key="PrimaryColor">#1E40AF</Color>
</ResourceDictionary>
"#;

    assert!(parse_structured_payload_with_hint(payload, Some(StructuredFormat::Json)).is_none());
}

#[test]
fn parses_object_payload() {
    let parsed = parse_json_payload(r#"{"name":"dtk","nested":{"id":1}}"#);
    assert!(parsed.is_some());
}

#[test]
fn parses_array_payload() {
    let parsed = parse_json_payload(r#"[{"id":1},{"id":2}]"#);
    assert!(parsed.is_some());
}

#[test]
fn rejects_non_structured_json() {
    assert!(parse_json_payload(r#""string""#).is_none());
    assert!(parse_json_payload("42").is_none());
}

#[test]
fn collects_object_field_paths() {
    let value = parse_json_payload(r#"{"user":{"id":1,"email":"a@b.com"},"status":"ok"}"#)
        .expect("expected structured json");
    let paths = collect_field_paths(&value);
    assert_eq!(
        paths,
        vec![
            "status".to_string(),
            "user".to_string(),
            "user.email".to_string(),
            "user.id".to_string()
        ]
    );
}

#[test]
fn collects_array_field_paths() {
    let value = parse_json_payload(r#"[{"id":1,"name":"a"},{"id":2,"name":"b"}]"#)
        .expect("expected structured json");
    let paths = collect_field_paths(&value);
    assert_eq!(
        paths,
        vec![
            "[0]".to_string(),
            "[0].id".to_string(),
            "[0].name".to_string(),
            "[1]".to_string(),
            "[1].id".to_string(),
            "[1].name".to_string()
        ]
    );
}

#[test]
fn filters_array_payload_by_allowlist() {
    let value = parse_json_payload(
            r#"[{"id":7,"title":"AlienBot Configuration","description":"","created_by":{"username":"waltercheng","id":1}}]"#,
        )
        .expect("expected structured json");

    let config = FilterConfig {
        id: None,
        name: None,
        source: None,
        request: None,
        notes: None,
        format: None,
        content_path: None,
        allow: vec![
            "[].id".to_string(),
            "[].title".to_string(),
            "[].created_by.username".to_string(),
        ],
        pii: vec![],
    };

    let filtered = filter_json_payload(&value, &config).expect("expected filtered json");
    let rendered = serde_json::to_value(filtered).expect("expected json value");

    assert_eq!(
        rendered,
        serde_json::json!([
            {
                "id": 7,
                "title": "AlienBot Configuration",
                "created_by": {
                    "username": "waltercheng"
                }
            }
        ])
    );
}

#[test]
fn allowlist_filters_only_explicit_fields() {
    let value =
        parse_json_payload(r#"{"title":"hello","secret":"x"}"#).expect("expected structured json");

    let config = FilterConfig {
        id: None,
        name: None,
        source: None,
        request: None,
        notes: None,
        format: None,
        content_path: None,
        allow: vec!["title".to_string()],
        pii: vec![],
    };

    let filtered = filter_json_payload(&value, &config).expect("expected filtered json");
    assert_eq!(filtered, serde_json::json!({"title":"hello"}));
}

#[test]
fn adds_metadata_to_object_payload() {
    let value =
        parse_json_payload(r#"{"title":"hello","secret":"x"}"#).expect("expected structured json");

    let config = FilterConfig {
        id: None,
        name: None,
        source: None,
        request: None,
        notes: None,
        format: None,
        content_path: None,
        allow: vec!["title".to_string()],
        pii: vec![],
    };

    let filtered =
        filter_json_payload_with_metadata(&value, &config).expect("expected filtered json");
    assert_eq!(
        filtered,
        serde_json::json!({
            "title": "hello",
            "_dtk": {
                "root_kind": "object",
                "available_fields": ["secret", "title"],
                "content_path": null,
                "store_hint": "local"
            }
        })
    );
}

#[test]
fn wraps_array_payload_with_metadata() {
    let value = parse_json_payload(r#"[{"id":1,"title":"a","updated":"x"}]"#)
        .expect("expected structured json");

    let config = FilterConfig {
        id: None,
        name: None,
        source: None,
        request: None,
        notes: None,
        format: None,
        content_path: None,
        allow: vec!["[].id".to_string(), "[].title".to_string()],
        pii: vec![],
    };

    let filtered =
        filter_json_payload_with_metadata(&value, &config).expect("expected filtered json");
    assert_eq!(
        filtered,
        serde_json::json!({
            "result": [
                {
                    "id": 1,
                    "title": "a"
                }
            ],
            "_dtk": {
                "root_kind": "array",
                "item_kind": "object",
                "available_fields": ["[]", "[].id", "[].title", "[].updated"],
                "content_path": "[]",
                "store_hint": "local"
            }
        })
    );
}

#[test]
fn exposes_nested_available_fields_in_metadata() {
    let value = parse_json_payload(
            r#"{"limit":30,"users":[{"id":1,"firstName":"Jane","lastName":"Doe","hair":{"color":"black","type":"wavy"},"address":{"city":"Austin","state":"TX"}}]}"#,
        )
        .expect("expected structured json");

    let config = FilterConfig {
        id: None,
        name: None,
        source: None,
        request: None,
        notes: None,
        format: None,
        content_path: None,
        allow: vec!["users[].id".to_string()],
        pii: vec![],
    };

    let filtered =
        filter_json_payload_with_metadata(&value, &config).expect("expected filtered json");

    assert_eq!(
        filtered,
        serde_json::json!({
            "users": [
                {
                    "id": 1
                }
            ],
            "_dtk": {
                "root_kind": "object",
                "available_fields": [
                    "limit",
                    "users",
                    "users[]",
                    "users[].address",
                    "users[].firstName",
                    "users[].hair",
                    "users[].id",
                    "users[].lastName"
                ],
                "content_path": "users",
                "store_hint": "local"
            }
        })
    );
}

#[test]
fn filters_content_path_subtree_while_preserving_envelope() {
    let value = parse_json_payload(
            r#"{"limit":30,"skip":0,"total":1,"users":[{"id":1,"firstName":"Jane","lastName":"Doe","secret":"x"}]}"#,
        )
        .expect("expected structured json");

    let config = FilterConfig {
        id: None,
        name: None,
        source: None,
        request: None,
        notes: None,
        format: None,
        content_path: Some("users".to_string()),
        allow: vec![
            "[].id".to_string(),
            "[].firstName".to_string(),
            "[].lastName".to_string(),
        ],
        pii: vec![],
    };

    let filtered =
        filter_json_payload_with_metadata(&value, &config).expect("expected filtered json");

    assert_eq!(
        filtered,
        serde_json::json!({
            "limit": 30,
            "skip": 0,
            "total": 1,
            "users": [
                {
                    "id": 1,
                    "firstName": "Jane",
                    "lastName": "Doe"
                }
            ],
            "_dtk": {
                "root_kind": "object",
                "available_fields": [
                    "limit",
                    "skip",
                    "total",
                    "users",
                    "users[]",
                    "users[].firstName",
                    "users[].id",
                    "users[].lastName",
                    "users[].secret"
                ],
                "content_path": "users",
                "store_hint": "local"
            }
        })
    );
}

#[test]
fn filters_content_path_nested_subtree_when_parent_field_is_allowed() {
    let value = parse_json_payload(
        r#"{"users":[{"id":1,"hair":{"color":"black","type":"wavy"},"secret":"x"}]}"#,
    )
    .expect("expected structured json");

    let config = FilterConfig {
        id: None,
        name: None,
        source: None,
        request: None,
        notes: None,
        format: None,
        content_path: Some("users".to_string()),
        allow: vec!["[].hair".to_string()],
        pii: vec![],
    };

    let filtered = filter_json_payload(&value, &config).expect("expected filtered json");
    assert_eq!(
        filtered,
        serde_json::json!({
            "users": [
                {
                    "hair": {
                        "color": "black",
                        "type": "wavy"
                    }
                }
            ]
        })
    );
}

#[test]
fn filters_wildcard_subtree_for_dynamic_object_keys() {
    let value = parse_json_payload(
            r#"{"connections":{"Alpha":{"main":[[{"node":"A"}]]},"Beta":{"ai_tool":[[{"node":"B"}]]}},"name":"wf"}"#,
        )
        .expect("expected structured json");

    let config = FilterConfig {
        id: None,
        name: None,
        source: None,
        request: None,
        notes: None,
        format: None,
        content_path: None,
        allow: vec!["connections.**".to_string()],
        pii: vec![],
    };

    let filtered = filter_json_payload(&value, &config).expect("expected filtered json");

    assert_eq!(
        filtered,
        serde_json::json!({
            "connections": {
                "Alpha": {"main": [[{"node": "A"}]]},
                "Beta": {"ai_tool": [[{"node": "B"}]]}
            }
        })
    );
}

#[test]
fn stable_ref_id_is_deterministic() {
    let left = stable_ref_id(r#"{"a":1,"b":2}"#).expect("expected ref id");
    let right = stable_ref_id(r#"{"a":1,"b":2}"#).expect("expected ref id");
    assert_eq!(left, right);
    assert!(left.starts_with("dtk_"));
}

#[test]
fn summarizes_curl_command_with_domain() {
    let args = vec![
        "curl".to_string(),
        "-sS".to_string(),
        "https://dummyjson.com/users".to_string(),
    ];

    let signature = summarize_command_signature(&args).expect("expected signature");

    assert_eq!(signature.command, "curl");
    assert_eq!(signature.domain, "dummyjson.com");
    assert_eq!(signature.details, "curl -sS https://dummyjson.com/users");
}

#[test]
fn summarizes_non_network_command_without_domain() {
    let args = vec!["git".to_string(), "status".to_string()];

    let signature = summarize_command_signature(&args).expect("expected signature");

    assert_eq!(signature.command, "git");
    assert_eq!(signature.domain, "");
    assert_eq!(signature.details, "git status");
}

#[test]
fn records_exec_metrics_with_deduplicated_signatures() {
    let store_dir = temp_store_dir("unit-test-usage");
    let created_at_unix_ms = now_unix_ms();
    let first = ExecMetricsInput {
        ref_id: "dtk_abc_1".to_string(),
        created_at_unix_ms,
        signature: summarize_command_signature(&[
            "curl".to_string(),
            "-sS".to_string(),
            "https://dummyjson.com/users".to_string(),
        ])
        .expect("expected signature"),
        config_id: "dummyjson_users".to_string(),
        config_path: "/tmp/dummyjson_users.json".to_string(),
        original_tokens: 120,
        filtered_tokens: 30,
    };
    let second = ExecMetricsInput {
        ref_id: "dtk_abc_2".to_string(),
        created_at_unix_ms: created_at_unix_ms + 1,
        signature: summarize_command_signature(&[
            "curl".to_string(),
            "-sS".to_string(),
            "https://dummyjson.com/users".to_string(),
        ])
        .expect("expected signature"),
        config_id: "dummyjson_users".to_string(),
        config_path: "/tmp/dummyjson_users.json".to_string(),
        original_tokens: 220,
        filtered_tokens: 40,
    };

    record_exec_metrics(&store_dir, &first).expect("expected usage write");
    record_exec_metrics(&store_dir, &second).expect("expected usage write");

    let connection =
        rusqlite::Connection::open(usage_db_path(&store_dir)).expect("expected usage db");
    let signature_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM command_signatures", [], |row| {
            row.get(0)
        })
        .expect("expected signature count");
    let metric_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM exec_metrics", [], |row| row.get(0))
        .expect("expected metric count");
    let domain: String = connection
        .query_row("SELECT domain FROM command_signatures LIMIT 1", [], |row| {
            row.get(0)
        })
        .expect("expected domain");

    assert_eq!(signature_count, 1);
    assert_eq!(metric_count, 2);
    assert_eq!(domain, "dummyjson.com");
    let _ = std::fs::remove_dir_all(store_dir);
}

#[test]
fn prunes_old_usage_rows_while_retaining_recent_entries() {
    let store_dir = temp_store_dir("unit-test-usage-prune");
    let stale_created_at_unix_ms = now_unix_ms().saturating_sub(31_u128 * 24 * 60 * 60 * 1000);
    let fresh_created_at_unix_ms = now_unix_ms();

    let stale = ExecMetricsInput {
        ref_id: "dtk_stale".to_string(),
        created_at_unix_ms: stale_created_at_unix_ms,
        signature: summarize_command_signature(&["git".to_string(), "status".to_string()])
            .expect("expected signature"),
        config_id: "stale_cfg".to_string(),
        config_path: "/tmp/stale.json".to_string(),
        original_tokens: 10,
        filtered_tokens: 5,
    };
    let fresh = ExecMetricsInput {
        ref_id: "dtk_fresh".to_string(),
        created_at_unix_ms: fresh_created_at_unix_ms,
        signature: summarize_command_signature(&[
            "curl".to_string(),
            "-sS".to_string(),
            "https://dummyjson.com/users".to_string(),
        ])
        .expect("expected signature"),
        config_id: "fresh_cfg".to_string(),
        config_path: "/tmp/fresh.json".to_string(),
        original_tokens: 20,
        filtered_tokens: 10,
    };

    record_exec_metrics(&store_dir, &stale).expect("stale usage write");
    record_exec_metrics(&store_dir, &fresh).expect("fresh usage write");

    let connection =
        rusqlite::Connection::open(usage_db_path(&store_dir)).expect("expected usage db");
    let metric_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM exec_metrics", [], |row| row.get(0))
        .expect("expected metric count");
    let signature_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM command_signatures", [], |row| {
            row.get(0)
        })
        .expect("expected signature count");

    assert_eq!(metric_count, 1);
    assert_eq!(signature_count, 1);
    let _ = std::fs::remove_dir_all(store_dir);
}

#[test]
fn resets_old_usage_schema_without_migrating_columns() {
    let store_dir = temp_store_dir("unit-test-usage-reset");
    let db_path = usage_db_path(&store_dir);
    std::fs::create_dir_all(&store_dir).expect("create store dir");

    {
        let connection = rusqlite::Connection::open(&db_path).expect("open old db");
        connection
            .execute_batch(
                r#"
                    CREATE TABLE exec_metrics (
                        ref_id TEXT PRIMARY KEY,
                        created_at_unix_ms INTEGER NOT NULL
                    );
                    PRAGMA user_version = 1;
                    "#,
            )
            .expect("seed old schema");
    }

    let connection = rusqlite::Connection::open(&db_path).expect("open db");
    super::init_usage_schema(&connection).expect("reset schema");

    let mut statement = connection
        .prepare("PRAGMA table_info(exec_metrics)")
        .expect("prepare table info");
    let mut rows = statement.query([]).expect("query table info");
    let mut has_config_id = false;
    while let Some(row) = rows.next().expect("read table info") {
        let name: String = row.get(1).expect("read column name");
        if name == "config_id" {
            has_config_id = true;
            break;
        }
    }
    let schema_version: i32 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .expect("schema version");

    assert!(has_config_id);
    assert_eq!(schema_version, super::USAGE_SCHEMA_VERSION);
    let _ = std::fs::remove_dir_all(store_dir);
}

#[test]
fn start_and_end_session_round_trip() {
    let store_dir = temp_store_dir("unit-test-usage-session");

    let started = start_session(&store_dir, None).expect("expected session start");
    assert!(started.ticket_id.starts_with("dtk-sess-"));
    assert!(started.ended_at_unix_ms.is_none());

    let ended = end_session(&store_dir).expect("expected session end");
    assert_eq!(ended.id, started.id);
    assert_eq!(ended.ticket_id, started.ticket_id);
    assert!(ended.ended_at_unix_ms.is_some());

    let connection =
        rusqlite::Connection::open(usage_db_path(&store_dir)).expect("expected usage db");
    let session_count: i64 = connection
        .query_row("SELECT COUNT(*) FROM sessions", [], |row| row.get(0))
        .expect("expected session count");
    let active_count: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM sessions WHERE ended_at_unix_ms IS NULL",
            [],
            |row| row.get(0),
        )
        .expect("expected active session count");

    assert_eq!(session_count, 1);
    assert_eq!(active_count, 0);
    let _ = std::fs::remove_dir_all(store_dir);
}

#[test]
fn records_exec_metrics_with_active_ticket_id() {
    let store_dir = temp_store_dir("unit-test-usage-ticket");
    let session = start_session(&store_dir, Some("ticket-123".to_string())).expect("start");
    assert_eq!(session.ticket_id, "ticket-123");
    let created_at_unix_ms = now_unix_ms();

    let metrics = ExecMetricsInput {
        ref_id: "dtk_abc_3".to_string(),
        created_at_unix_ms,
        signature: summarize_command_signature(&[
            "curl".to_string(),
            "-sS".to_string(),
            "https://dummyjson.com/users".to_string(),
        ])
        .expect("expected signature"),
        config_id: "dummyjson_users".to_string(),
        config_path: "/tmp/dummyjson_users.json".to_string(),
        original_tokens: 200,
        filtered_tokens: 50,
    };

    record_exec_metrics(&store_dir, &metrics).expect("expected usage write");

    let connection =
        rusqlite::Connection::open(usage_db_path(&store_dir)).expect("expected usage db");
    let ticket_id: String = connection
        .query_row(
            "SELECT ticket_id FROM exec_metrics WHERE ref_id = ?1",
            ["dtk_abc_3"],
            |row| row.get(0),
        )
        .expect("expected ticket id");
    let session_id: i64 = connection
        .query_row(
            "SELECT session_id FROM exec_metrics WHERE ref_id = ?1",
            ["dtk_abc_3"],
            |row| row.get(0),
        )
        .expect("expected session id");

    assert_eq!(ticket_id, "ticket-123");
    assert_eq!(session_id, session.id);
    let _ = std::fs::remove_dir_all(store_dir);
}

#[test]
fn resolves_filter_config_id_from_explicit_id_then_name_then_path() {
    let with_id = FilterConfig {
        id: Some("cfg-users".to_string()),
        name: Some("users".to_string()),
        source: None,
        request: None,
        notes: None,
        format: None,
        content_path: None,
        allow: vec![],
        pii: vec![],
    };
    let with_name = FilterConfig {
        id: None,
        name: Some("users-name".to_string()),
        source: None,
        request: None,
        notes: None,
        format: None,
        content_path: None,
        allow: vec![],
        pii: vec![],
    };
    let anonymous = FilterConfig {
        id: None,
        name: None,
        source: None,
        request: None,
        notes: None,
        format: None,
        content_path: None,
        allow: vec![],
        pii: vec![],
    };

    assert_eq!(
        resolve_filter_config_id(&with_id, "/tmp/example.json"),
        "cfg-users"
    );
    assert_eq!(
        resolve_filter_config_id(&with_name, "/tmp/example.json"),
        "users-name"
    );
    assert_eq!(
        resolve_filter_config_id(&anonymous, "/tmp/example.json"),
        "example"
    );
}

#[test]
fn records_field_access_and_generates_expand_recommendation() {
    let store_dir = temp_store_dir("unit-test-field-access-expand");
    let _ = std::fs::remove_dir_all(&store_dir);
    std::fs::create_dir_all(&store_dir).expect("create store dir");
    let config_path = store_dir.join("users-config.json");
    std::fs::write(
        &config_path,
        serde_json::to_string_pretty(&serde_json::json!({
            "id": "users_cfg",
            "name": "users_cfg",
            "allow": ["users[].id"]
        }))
        .expect("config json"),
    )
    .expect("write config");
    let created_at_unix_ms = now_unix_ms();

    let metrics = ExecMetricsInput {
        ref_id: "dtk_expand_1".to_string(),
        created_at_unix_ms,
        signature: summarize_command_signature(&[
            "curl".to_string(),
            "-sS".to_string(),
            "https://dummyjson.com/users".to_string(),
        ])
        .expect("expected signature"),
        config_id: "users_cfg".to_string(),
        config_path: config_path.to_string_lossy().to_string(),
        original_tokens: 200,
        filtered_tokens: 50,
    };
    record_exec_metrics(&store_dir, &metrics).expect("metrics");

    for created_at_unix_ms in [
        created_at_unix_ms + 1,
        created_at_unix_ms + 2,
        created_at_unix_ms + 3,
    ] {
        let access = FieldAccessRecordInput {
            ref_id: "dtk_expand_1".to_string(),
            created_at_unix_ms,
            fields: vec!["users[].email".to_string()],
            array_index: None,
            all: false,
            access_kind: "retrieve".to_string(),
        };
        record_field_access(&store_dir, &access).expect("field access");
    }

    let recommendations = load_config_recommendations(
        &store_dir,
        RecommendationThresholds {
            expand_field_access_count: 3,
            tighten_fallback_count: 3,
            remove_fallback_count: 6,
            tighten_allow_count_min: 6,
        },
    )
    .expect("recommendations");

    assert!(recommendations.iter().any(|recommendation| {
        recommendation.recommendation_kind == "expand_allowlist"
            && recommendation.config_id == "users_cfg"
            && recommendation.field_path.as_deref() == Some("users[].email")
    }));
    let notices = recommendation_notices_for_retrieve(
        &store_dir,
        "dtk_expand_1",
        &["users[].email".to_string()],
    )
    .expect("notices");
    assert!(notices
        .iter()
        .any(|notice| notice.contains("add `users[].email` to config `users_cfg`")));
    let _ = std::fs::remove_dir_all(store_dir);
}

#[test]
fn broader_allowlist_covers_deeper_fields() {
    let config = FilterConfig {
        id: None,
        name: None,
        source: None,
        request: None,
        notes: None,
        format: None,
        content_path: Some("users".to_string()),
        allow: vec!["[].hair".to_string()],
        pii: vec![],
    };

    assert!(field_is_allowlisted(&config, "users[].hair.color"));
    assert!(field_is_allowlisted(&config, "users[0].hair.color"));
}

#[test]
fn broader_allowlist_suppresses_nested_recommendation_notice() {
    let store_dir = temp_store_dir("unit-test-broader-allowlist-notice");
    let _ = std::fs::remove_dir_all(&store_dir);
    std::fs::create_dir_all(&store_dir).expect("create store dir");
    let config_path = store_dir.join("users-config.json");
    std::fs::write(
        &config_path,
        serde_json::to_string_pretty(&serde_json::json!({
            "id": "users_cfg",
            "name": "users_cfg",
            "content_path": "users",
            "allow": ["[].hair"]
        }))
        .expect("config json"),
    )
    .expect("write config");
    let config = load_filter_config(&config_path).expect("load config");
    assert_eq!(
        normalize_field_path_for_config("users[].hair.color", &config),
        Some("[].hair.color".to_string())
    );
    assert!(field_is_allowlisted(&config, "users[].hair.color"));
    let created_at_unix_ms = now_unix_ms();

    let metrics = ExecMetricsInput {
        ref_id: "dtk_hair_1".to_string(),
        created_at_unix_ms,
        signature: summarize_command_signature(&[
            "curl".to_string(),
            "-sS".to_string(),
            "https://dummyjson.com/users".to_string(),
        ])
        .expect("expected signature"),
        config_id: "users_cfg".to_string(),
        config_path: config_path.to_string_lossy().to_string(),
        original_tokens: 200,
        filtered_tokens: 50,
    };
    record_exec_metrics(&store_dir, &metrics).expect("metrics");

    for (offset, field_path) in [
        "users[0].hair.color",
        "users[1].hair.color",
        "users[2].hair.color",
    ]
    .iter()
    .enumerate()
    {
        let access = FieldAccessRecordInput {
            ref_id: "dtk_hair_1".to_string(),
            created_at_unix_ms: created_at_unix_ms + offset as u128 + 1,
            fields: vec![(*field_path).to_string()],
            array_index: None,
            all: false,
            access_kind: "retrieve".to_string(),
        };
        record_field_access(&store_dir, &access).expect("field access");
    }

    let recommendations =
        load_config_recommendations(&store_dir, RecommendationThresholds::default())
            .expect("recommendations");
    assert!(!recommendations.iter().any(|recommendation| {
        recommendation.config_id == "users_cfg"
            && recommendation.recommendation_kind == "expand_allowlist"
    }));

    let notices = recommendation_notices_for_retrieve(
        &store_dir,
        "dtk_hair_1",
        &["users[3].hair.color".to_string()],
    )
    .expect("notices");
    assert!(notices.is_empty());
    let _ = std::fs::remove_dir_all(store_dir);
}

#[test]
fn treats_indexed_retrievals_as_the_same_repeat_pattern() {
    let store_dir = temp_store_dir("unit-test-index-normalization");
    let _ = std::fs::remove_dir_all(&store_dir);
    std::fs::create_dir_all(&store_dir).expect("create store dir");
    let config_path = store_dir.join("users-config.json");
    std::fs::write(
        &config_path,
        serde_json::to_string_pretty(&serde_json::json!({
            "id": "users_cfg",
            "name": "users_cfg",
            "allow": ["users[].id"]
        }))
        .expect("config json"),
    )
    .expect("write config");

    let metrics = ExecMetricsInput {
        ref_id: "dtk_index_1".to_string(),
        created_at_unix_ms: now_unix_ms(),
        signature: summarize_command_signature(&[
            "curl".to_string(),
            "-sS".to_string(),
            "https://dummyjson.com/users".to_string(),
        ])
        .expect("expected signature"),
        config_id: "users_cfg".to_string(),
        config_path: config_path.to_string_lossy().to_string(),
        original_tokens: 200,
        filtered_tokens: 50,
    };
    record_exec_metrics(&store_dir, &metrics).expect("metrics");

    for (offset, field_path) in [
        "users[0].hair.color",
        "users[1].hair.color",
        "users[2].hair.color",
    ]
    .iter()
    .enumerate()
    {
        let access = FieldAccessRecordInput {
            ref_id: "dtk_index_1".to_string(),
            created_at_unix_ms: now_unix_ms() + offset as u128,
            fields: vec![(*field_path).to_string()],
            array_index: None,
            all: false,
            access_kind: "retrieve".to_string(),
        };
        record_field_access(&store_dir, &access).expect("field access");
    }

    let recommendations =
        load_config_recommendations(&store_dir, RecommendationThresholds::default())
            .expect("recommendations");
    assert!(recommendations.iter().any(|recommendation| {
        recommendation.recommendation_kind == "expand_allowlist"
            && recommendation.field_path.as_deref() == Some("users[].hair.color")
    }));

    let notices = recommendation_notices_for_retrieve(
        &store_dir,
        "dtk_index_1",
        &["users[3].hair.color".to_string()],
    )
    .expect("notices");
    assert!(notices
        .iter()
        .any(|notice| notice.contains("add `users[].hair.color` to config `users_cfg`")));
    let _ = std::fs::remove_dir_all(store_dir);
}

#[test]
fn generates_tighten_or_remove_recommendation_for_repeated_fallbacks() {
    let store_dir = temp_store_dir("unit-test-fallback-recommendation");
    let _ = std::fs::remove_dir_all(&store_dir);
    std::fs::create_dir_all(&store_dir).expect("create store dir");
    let config_path = store_dir.join("wide-config.json");
    std::fs::write(
        &config_path,
        serde_json::to_string_pretty(&serde_json::json!({
            "id": "wide_cfg",
            "allow": [
                "users[].id",
                "users[].name",
                "users[].email",
                "users[].phone",
                "users[].address",
                "users[].company"
            ]
        }))
        .expect("config json"),
    )
    .expect("write config");
    let created_at_unix_ms = now_unix_ms();

    for (offset, ref_id) in ["dtk_fb_1", "dtk_fb_2", "dtk_fb_3"].iter().enumerate() {
        let metrics = ExecMetricsInput {
            ref_id: (*ref_id).to_string(),
            created_at_unix_ms: created_at_unix_ms + offset as u128,
            signature: summarize_command_signature(&[
                "curl".to_string(),
                "-sS".to_string(),
                "https://dummyjson.com/users".to_string(),
            ])
            .expect("expected signature"),
            config_id: "wide_cfg".to_string(),
            config_path: config_path.to_string_lossy().to_string(),
            original_tokens: 100,
            filtered_tokens: 100,
        };
        record_exec_metrics(&store_dir, &metrics).expect("metrics");
        let issue = super::ExecMetricIssueInput {
            ref_id: (*ref_id).to_string(),
            created_at_unix_ms: created_at_unix_ms + 100 + offset as u128,
            signature: metrics.signature.clone(),
            config_id: "wide_cfg".to_string(),
            config_path: config_path.to_string_lossy().to_string(),
            original_tokens: 100,
            filtered_tokens: 140,
            issue_kind: "filtered_larger_than_original".to_string(),
        };
        super::record_exec_metric_issue(&store_dir, &issue).expect("issue");
    }

    let recommendations = load_config_recommendations(
        &store_dir,
        RecommendationThresholds {
            expand_field_access_count: 3,
            tighten_fallback_count: 3,
            remove_fallback_count: 6,
            tighten_allow_count_min: 6,
        },
    )
    .expect("recommendations");

    assert!(recommendations.iter().any(|recommendation| {
        recommendation.config_id == "wide_cfg"
            && recommendation.recommendation_kind == "tighten_allowlist"
    }));
    let notices = recommendation_notices_for_exec(
        &store_dir,
        "wide_cfg",
        "curl -sS https://dummyjson.com/users",
    )
    .expect("notices");
    assert!(notices
        .iter()
        .any(|notice| notice.contains("tighten config `wide_cfg`")));
    let _ = std::fs::remove_dir_all(store_dir);
}

#[test]
fn stores_and_recovers_payload() {
    let store_dir = temp_store_dir("unit-test-store");
    let payload = r#"{"hello":"world"}"#;

    let ref_id = store_original_payload(payload, &store_dir).expect("expected store to succeed");
    let recovered = recover_original_payload(&ref_id, &store_dir).expect("expected recovery");

    assert_eq!(recovered, payload);
    let _ = std::fs::remove_dir_all(store_dir);
}

#[test]
fn retrieves_requested_fields_from_object_payload() {
    let store_dir = temp_store_dir("unit-test-retrieve-object");
    let payload =
        r#"{"users":[{"age":30,"address":{"city":"Austin","state":"TX"},"name":"Ada"}],"total":1}"#;
    let ref_id = store_original_payload(payload, &store_dir).expect("expected store to succeed");

    let retrieved = retrieve_original_payload(
        &ref_id,
        &store_dir,
        &["users[].age".to_string(), "users[].address".to_string()],
        None,
        false,
    )
    .expect("expected retrieve to succeed");

    assert_eq!(
        retrieved,
        serde_json::json!({
            "users": [
                {
                    "age": 30,
                    "address": {
                        "city": "Austin",
                        "state": "TX"
                    }
                }
            ]
        })
    );

    let _ = std::fs::remove_dir_all(store_dir);
}

#[test]
fn retrieves_array_item_by_index() {
    let value = parse_json_payload(r#"[{"name":"first","age":1},{"name":"second","age":2}]"#)
        .expect("expected structured json");

    let retrieved = retrieve_json_payload(&value, &["name".to_string()], Some(1), false)
        .expect("expected retrieval");

    assert_eq!(retrieved, serde_json::json!({"name":"second"}));
}

#[test]
fn retrieves_all_items_from_array() {
    let value = parse_json_payload(r#"[{"name":"first","age":1},{"name":"second","age":2}]"#)
        .expect("expected structured json");

    let retrieved = retrieve_json_payload(&value, &["name".to_string()], None, true)
        .expect("expected retrieval");

    assert_eq!(
        retrieved,
        serde_json::json!([
            {"name":"first"},
            {"name":"second"}
        ])
    );
}

#[test]
fn retrieves_nested_array_item_by_index_path() {
    let value = parse_json_payload(
            r#"{"users":[{"firstName":"Ada","lastName":"Lovelace"},{"firstName":"Grace","lastName":"Hopper"}]}"#,
        )
        .expect("expected structured json");

    let retrieved = retrieve_json_payload(&value, &["users[0].firstName".to_string()], None, false)
        .expect("expected retrieval");

    assert_eq!(
        retrieved,
        serde_json::json!({
            "users": [
                {
                    "firstName": "Ada"
                }
            ]
        })
    );
}

#[test]
fn stores_same_payload_as_distinct_runs() {
    let store_dir = temp_store_dir("unit-test-store-duplicate-runs");
    let _ = std::fs::remove_dir_all(&store_dir);
    let payload = r#"{"hello":"world"}"#;

    let first_ref =
        store_original_payload(payload, &store_dir).expect("expected first store to succeed");
    let second_ref =
        store_original_payload(payload, &store_dir).expect("expected second store to succeed");

    assert_ne!(first_ref, second_ref);
    assert!(recover_original_payload(&first_ref, &store_dir).is_ok());
    assert!(recover_original_payload(&second_ref, &store_dir).is_ok());

    let refs_dir = store_dir.join("refs");
    let refs_count = std::fs::read_dir(&refs_dir)
        .expect("expected refs dir")
        .count();
    assert_eq!(refs_count, 2);

    let _ = std::fs::remove_dir_all(store_dir);
}

#[test]
fn uses_same_stable_ref_for_equivalent_json_and_yaml_payloads() {
    let json_payload = r#"{"users":[{"firstName":"Emily","age":28}]}"#;
    let yaml_payload = "users:\n  - firstName: Emily\n    age: 28\n";

    assert_eq!(stable_ref_id(json_payload), stable_ref_id(yaml_payload));
}

#[test]
fn retrieves_fields_from_stored_yaml_payload() {
    let store_dir = temp_store_dir("unit-test-store-yaml");
    let _ = std::fs::remove_dir_all(&store_dir);
    let payload = "users:\n  - firstName: Emily\n    age: 28\n";

    let ref_id =
        store_original_payload(payload, &store_dir).expect("expected yaml store to succeed");
    let retrieved = retrieve_original_payload(
        &ref_id,
        &store_dir,
        &["users[0].firstName".to_string()],
        None,
        false,
    )
    .expect("expected yaml retrieval to succeed");

    assert_eq!(
        retrieved,
        serde_json::json!({
            "users": [
                {
                    "firstName": "Emily"
                }
            ]
        })
    );

    let _ = std::fs::remove_dir_all(store_dir);
}

#[test]
fn stores_retention_metadata_in_index() {
    let store_dir = temp_store_dir("unit-test-store-retention");
    let payload = r#"{"hello":"world"}"#;

    let ref_id = store_original_payload_with_retention(payload, &store_dir, Some(7))
        .expect("expected store to succeed");
    let index_text = std::fs::read_to_string(store_dir.join("index.json")).expect("expected index");
    let index: serde_json::Value = serde_json::from_str(&index_text).expect("expected json");
    let entry = &index[&ref_id];

    assert_eq!(entry["retention_days"], 7);
    assert!(entry["created_at_unix_ms"].as_u64().is_some());
    assert!(entry["expires_at_unix_ms"].as_u64().is_some());
    let _ = std::fs::remove_dir_all(store_dir);
}

#[test]
fn cleanup_removes_expired_payloads() {
    let store_dir = temp_store_dir("unit-test-cleanup");
    let expired_payload = r#"{"expired":true}"#;
    let active_payload = r#"{"active":true}"#;

    let expired_ref = store_original_payload_with_retention(expired_payload, &store_dir, Some(0))
        .expect("expected expired store to succeed");
    let active_ref = store_original_payload_with_retention(active_payload, &store_dir, Some(7))
        .expect("expected active store to succeed");

    let report = cleanup_expired_payloads(&store_dir).expect("expected cleanup");

    assert_eq!(report.removed_count, 1);
    assert_eq!(report.remaining_count, 1);
    assert!(recover_original_payload(&active_ref, &store_dir).is_ok());
    assert!(recover_original_payload(&expired_ref, &store_dir).is_err());
    let _ = std::fs::remove_dir_all(store_dir);
}

#[test]
fn filters_nested_array_fields_with_object_prefix() {
    let value = parse_json_payload(
        r#"{"data":[{"id":"a","name":"alpha","secret":"x"}],"nextCursor":"abc"}"#,
    )
    .expect("expected structured json");

    let config = FilterConfig {
        id: None,
        name: None,
        source: None,
        request: None,
        notes: None,
        format: None,
        content_path: None,
        allow: vec!["data[].id".to_string(), "nextCursor".to_string()],
        pii: vec![],
    };

    let filtered =
        filter_json_payload_with_metadata(&value, &config).expect("expected filtered json");
    assert_eq!(
        filtered,
        serde_json::json!({
            "data": [
                {
                    "id": "a"
                }
            ],
            "nextCursor": "abc",
            "_dtk": {
                "available_fields": [
                    "data",
                    "data[]",
                    "data[].id",
                    "data[].name",
                    "data[].secret",
                    "nextCursor"
                ],
                "content_path": "data",
                "root_kind": "object",
                "store_hint": "local"
            }
        })
    );
}

#[test]
fn preview_lists_expired_payloads_without_removing() {
    let store_dir = temp_store_dir("unit-test-preview");
    let payload = r#"{"expired":true}"#;

    let ref_id = store_original_payload_with_retention(payload, &store_dir, Some(0))
        .expect("expected store to succeed");
    let preview = preview_expired_payloads(&store_dir).expect("expected preview");

    assert_eq!(preview.expired_ref_ids, vec![ref_id.clone()]);
    assert_eq!(preview.remaining_count, 1);
    assert!(recover_original_payload(&ref_id, &store_dir).is_ok());
    let _ = std::fs::remove_dir_all(store_dir);
}

#[test]
fn default_store_dir_is_user_scoped() {
    let store_dir = default_store_dir();
    let data_root = platform_data_dir();
    assert!(store_dir.starts_with(&data_root));
}

#[test]
fn runtime_store_dir_honors_explicit_override() {
    let store_dir = temp_store_dir("runtime-store-override");
    std::env::set_var("DTK_STORE_DIR", &store_dir);

    let resolved = runtime_store_dir();

    assert_eq!(resolved, store_dir);

    std::env::remove_var("DTK_STORE_DIR");
    let _ = std::fs::remove_dir_all(store_dir);
}

#[test]
fn platform_data_dir_selects_expected_root() {
    let data_dir = platform_data_dir();
    if cfg!(windows) {
        assert!(data_dir.starts_with(windows_data_dir()) || data_dir == PathBuf::from("."));
    } else {
        assert_eq!(data_dir, xdg_data_dir());
    }
}

#[test]
fn loads_config_metadata_and_rules() {
    let config = serde_json::from_value::<FilterConfig>(serde_json::json!({
        "name": "n8n_workflows_list",
        "source": "n8n",
        "request": "curl -sS ...",
        "notes": "workflow list",
        "allow": ["data[].id", "nextCursor"],
        "pii": [
            {
                "path": "data[].email",
                "action": "mask"
            }
        ]
    }))
    .expect("expected config to deserialize");

    assert_eq!(config.name.as_deref(), Some("n8n_workflows_list"));
    assert_eq!(config.source.as_deref(), Some("n8n"));
    assert_eq!(config.allow, vec!["data[].id", "nextCursor"]);
    assert_eq!(config.pii.len(), 1);
}

#[test]
fn applies_pii_rules_after_filtering() {
    let value = parse_json_payload(
        r#"{"users":[{"email":"ada@example.com","id":1,"name":"Ada"}],"token":"abc"}"#,
    )
    .expect("expected structured json");
    let config = serde_json::from_value::<FilterConfig>(serde_json::json!({
        "allow": ["users[].email", "users[].id", "users[].name"],
        "pii": [
            {
                "path": "users[].email",
                "action": "mask"
            }
        ]
    }))
    .expect("expected config to deserialize");

    let filtered = filter_json_payload_with_metadata(&value, &config).expect("expected filtered");

    assert_eq!(
        filtered,
        serde_json::json!({
            "users": [
                {
                    "email": "[PII INFORMATION]",
                    "id": 1,
                    "name": "Ada"
                }
            ],
            "_dtk": {
                "available_fields": [
                    "token",
                    "users",
                    "users[]",
                    "users[].email",
                    "users[].id",
                    "users[].name"
                ],
                "content_path": "users",
                "root_kind": "object",
                "store_hint": "local"
            }
        })
    );
}

#[test]
fn applies_deterministic_uuid_and_wildcard_precedence_to_all_array_items() {
    let value = parse_json_payload(
        r#"{"users":[{"email":"ada@example.com","ssn":5},{"email":"grace@example.com","ssn":42}]}"#,
    )
    .expect("expected structured json");
    let config = serde_json::from_value::<FilterConfig>(serde_json::json!({
        "allow": ["users[].email", "users[].ssn"],
        "pii": [
            {
                "path": "users[].**",
                "action": "mask",
                "replacement": "[REDACTED]"
            },
            {
                "path": "users[].ssn",
                "action": "uuid",
                "method": "template",
                "template": "DTK-GOVID-{value:04}"
            }
        ]
    }))
    .expect("expected config to deserialize");

    let filtered = filter_json_payload_with_metadata(&value, &config).expect("expected filtered");

    assert_eq!(
        filtered,
        serde_json::json!({
            "users": [
                {
                    "email": "[REDACTED]",
                    "ssn": "DTK-GOVID-0005"
                },
                {
                    "email": "[REDACTED]",
                    "ssn": "DTK-GOVID-0042"
                }
            ],
            "_dtk": {
                "available_fields": [
                    "users",
                    "users[]",
                    "users[].email",
                    "users[].ssn"
                ],
                "content_path": "users",
                "root_kind": "object",
                "store_hint": "local"
            }
        })
    );

    let reapplied = apply_pii_transform(
        &retrieve_json_payload(
            &value,
            &["users[].email".to_string(), "users[].ssn".to_string()],
            None,
            false,
        )
        .expect("expected retrieve"),
        &config,
    );

    assert_eq!(
        reapplied,
        serde_json::json!({
            "users": [
                {
                    "email": "[REDACTED]",
                    "ssn": "DTK-GOVID-0005"
                },
                {
                    "email": "[REDACTED]",
                    "ssn": "DTK-GOVID-0042"
                }
            ]
        })
    );
}

#[test]
fn codex_agents_normalization_drops_markers_and_keeps_plain_lines() {
    let existing = "@/home/walter/.codex/RTK.md\n\n<!-- DTK-START -->\n@/home/walter/.codex/DTK.md\n<!-- DTK-END -->\n";
    let next =
        super::normalize_codex_agents_content(existing, Some("@/home/walter/.codex/DTK.md"), None)
            .expect("expected content");
    assert_eq!(
        next,
        "@/home/walter/.codex/RTK.md\n@/home/walter/.codex/DTK.md\n"
    );
}

#[test]
fn codex_agents_normalization_removes_target_line_on_uninstall() {
    let existing = "@/home/walter/.codex/RTK.md\n@/home/walter/.codex/DTK.md\n";
    let next =
        super::normalize_codex_agents_content(existing, None, Some("@/home/walter/.codex/DTK.md"))
            .expect("expected content");
    assert_eq!(next, "@/home/walter/.codex/RTK.md\n");
}
