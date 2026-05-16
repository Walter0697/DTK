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
fn exec_filters_toml_command_output() {
    let base_dir = temp_test_dir("exec-toml");
    let config_path = base_dir.join("toml-packages.json");
    let store_dir = base_dir.join("store");
    fs::create_dir_all(&store_dir).expect("create store dir");

    fs::write(
        &config_path,
        r#"{
  "name": "toml_packages",
  "format": "toml",
  "content_path": "package",
  "allow": [
    "[].name",
    "[].version"
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
            "printf 'version = 4\n\n[[package]]\nname = \"dtk\"\nversion = \"0.0.2\"\nsource = \"path+file:///home/git/DTK\"\nchecksum = \"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\"\ndependencies = [\"dialoguer\", \"rusqlite\", \"serde\", \"serde_json\", \"serde_yaml\", \"sha2\", \"toml\"]\n\n[[package]]\nname = \"dialoguer\"\nversion = \"0.12.0\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\nchecksum = \"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\"\ndependencies = [\"console\", \"shell-words\"]\n\n[[package]]\nname = \"rusqlite\"\nversion = \"0.32.1\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\nchecksum = \"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc\"\ndependencies = [\"bitflags\", \"libsqlite3-sys\"]\n\n[[package]]\nname = \"serde\"\nversion = \"1.0.228\"\nsource = \"registry+https://github.com/rust-lang/crates.io-index\"\nchecksum = \"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd\"\ndependencies = [\"serde_derive\"]\n'",
        )
        .output()
        .expect("run dtk_exec");

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let value: serde_json::Value = serde_json::from_str(&stdout).expect("json stdout");

    assert_eq!(value["package"][0]["name"], "dtk");
    assert_eq!(value["package"][0]["version"], "0.0.2");
    assert!(value["package"][0].get("source").is_none());
    assert!(value["_dtk"]["ref_id"].as_str().is_some());

    let _ = fs::remove_dir_all(base_dir);
}
