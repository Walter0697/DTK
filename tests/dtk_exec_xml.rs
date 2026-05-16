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
fn exec_filters_xml_rss_feed_output() {
    let base_dir = temp_test_dir("exec-xml");
    let config_path = base_dir.join("xml-rss-feed.json");
    let store_dir = base_dir.join("store");
    fs::create_dir_all(&store_dir).expect("create store dir");

    fs::write(
        &config_path,
        r#"{
  "name": "xml_rss_feed",
  "format": "xml",
  "content_path": "rss.channel",
  "allow": [
    "title.text",
    "item[].title.text",
    "item[].pubDate.text"
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
        .arg("/bin/cat")
        .arg("samples/payload.xml_rss_feed.xml")
        .output()
        .expect("run dtk_exec");

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let value: serde_json::Value = serde_json::from_str(&stdout).expect("json stdout");

    assert_eq!(
        value["rss"]["channel"]["title"]["text"],
        "Desktop Tooling Weekly"
    );
    assert_eq!(
        value["rss"]["channel"]["item"][0]["title"]["text"],
        "DTK expands structured sample coverage"
    );
    assert!(value["rss"]["channel"]["item"][1].get("category").is_none());
    assert!(value["rss"]["channel"]["item"][0].get("guid").is_none());
    assert!(value["_dtk"]["ref_id"].as_str().is_some());

    let _ = fs::remove_dir_all(base_dir);
}
