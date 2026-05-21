use std::io;
use std::path::PathBuf;

use super::template::{install_text_file, remove_if_exists};

const KILOCODE_RULES: &str = r#"# DTK - Structured Payload Routing (Kilo Code)

Use `dtk exec` for commands that produce structured payloads you may want to filter or retrieve later.

## Rule

Prefer `dtk exec -- <command>` for curl/API responses, JSON, YAML, TOML, HCL, CSV, INI, XML, and XAML output.

## Follow-up

Use `dtk retrieve <ref_id> [fields]` when you need more fields from a stored payload.
Use `dtk config allow add/remove` to adjust the allowlist after a config exists.
"#;

fn rules_path() -> PathBuf {
    PathBuf::from(".kilocode")
        .join("rules")
        .join("rtk-rules.md")
}

pub(crate) fn install_kilocode_guidance() -> io::Result<bool> {
    install_text_file(rules_path(), KILOCODE_RULES)
}

pub(crate) fn uninstall_kilocode_guidance() -> io::Result<bool> {
    remove_if_exists(rules_path())
}
