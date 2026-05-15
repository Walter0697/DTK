use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use assert_cmd::Command;

fn temp_test_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0);
    std::env::temp_dir()
        .join("dtk-tests")
        .join(format!("{name}-{nonce}"))
}

#[test]
fn exec_filters_yaml_command_output() {
    let base_dir = temp_test_dir("exec-yaml");
    let config_path = base_dir.join("yaml-users.json");
    let store_dir = base_dir.join("store");
    fs::create_dir_all(&store_dir).expect("create store dir");

    fs::write(
        &config_path,
        r#"{
  "name": "yaml_users",
  "format": "yaml",
  "content_path": "users",
  "allow": [
    "[].firstName",
    "[].age"
  ]
}
"#,
    )
    .expect("write config");

    let output = Command::cargo_bin("dtk_exec")
        .expect("binary exists")
        .env("DTK_STORE_DIR", &store_dir)
        .arg("--config")
        .arg(&config_path)
        .arg("--")
        .arg("/bin/sh")
        .arg("-c")
        .arg(
            "printf 'users:\n  - firstName: Emily\n    age: 28\n    email: emily@example.com\n    username: emilyj\n    role: admin\n    address:\n      city: Seattle\n      state: WA\n      country: United States\n    company:\n      name: Northwind\n      title: Product Manager\n      department: Operations\n    metadata:\n      createdAt: 2026-05-14T00:00:00Z\n      updatedAt: 2026-05-14T00:00:00Z\n      tags:\n        - internal\n        - beta\n  - firstName: Grace\n    age: 41\n    email: grace@example.com\n    username: graceh\n    role: editor\n    address:\n      city: New York\n      state: NY\n      country: United States\n    company:\n      name: Contoso\n      title: Engineering Manager\n      department: Platform\n    metadata:\n      createdAt: 2026-05-13T00:00:00Z\n      updatedAt: 2026-05-14T00:00:00Z\n      tags:\n        - premium\n        - west\n  - firstName: Ada\n    age: 36\n    email: ada@example.com\n    username: adal\n    role: analyst\n    address:\n      city: Austin\n      state: TX\n      country: United States\n    company:\n      name: Fabrikam\n      title: Data Scientist\n      department: Research\n    metadata:\n      createdAt: 2026-05-12T00:00:00Z\n      updatedAt: 2026-05-14T00:00:00Z\n      tags:\n        - east\n        - partner\n  - firstName: Linus\n    age: 33\n    email: linus@example.com\n    username: linust\n    role: viewer\n    address:\n      city: Portland\n      state: OR\n      country: United States\n    company:\n      name: Globex\n      title: Site Reliability Engineer\n      department: Infrastructure\n    metadata:\n      createdAt: 2026-05-11T00:00:00Z\n      updatedAt: 2026-05-14T00:00:00Z\n      tags:\n        - infra\n        - core\n'",
        )
        .output()
        .expect("run dtk_exec");

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let value: serde_json::Value = serde_json::from_str(&stdout).expect("json stdout");

    assert_eq!(value["users"][0]["firstName"], "Emily");
    assert_eq!(value["users"][0]["age"], 28);
    assert!(value["users"][0].get("email").is_none());
    assert!(value["_dtk"]["ref_id"].as_str().is_some());

    let _ = fs::remove_dir_all(base_dir);
}
