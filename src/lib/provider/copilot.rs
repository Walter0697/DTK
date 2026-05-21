use std::io;
use std::path::PathBuf;

use super::template::{install_text_file, remove_if_exists};

const COPILOT_HOOK_JSON: &str = r#"{
  "hooks": {
    "PreToolUse": [
      {
        "type": "command",
        "command": "dtk_hook_route --provider copilot",
        "cwd": ".",
        "timeout": 5
      }
    ]
  }
}
"#;

const COPILOT_INSTRUCTIONS: &str = r#"# DTK - Structured Payload Routing

Use `dtk exec` when a command returns structured payloads that should be filtered before reaching the model.

## Rule

Prefer `dtk exec -- <command>` for payload-heavy commands such as `curl`, `kubectl`, `cat`, `git show`, and API responses.

## Follow-up

Use `dtk retrieve <ref_id> [fields]` to recover fields later.
Use `dtk config allow add/remove` to tune the allowlist after a config exists.
"#;

fn copilot_hooks_path() -> PathBuf {
    PathBuf::from(".github")
        .join("hooks")
        .join("dtk-rewrite.json")
}

fn copilot_instructions_path() -> PathBuf {
    PathBuf::from(".github").join("copilot-instructions.md")
}

pub(crate) fn install_copilot_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= install_text_file(copilot_hooks_path(), COPILOT_HOOK_JSON)?;
    changed |= install_text_file(copilot_instructions_path(), COPILOT_INSTRUCTIONS)?;
    Ok(changed)
}

pub(crate) fn uninstall_copilot_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= remove_if_exists(copilot_hooks_path())?;
    changed |= remove_if_exists(copilot_instructions_path())?;
    Ok(changed)
}
