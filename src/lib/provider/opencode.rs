use std::io;
use std::path::PathBuf;

use super::template::{install_text_file, remove_if_exists};

const OPENCODE_PLUGIN: &str = r#"import type { Plugin } from "@opencode-ai/plugin"

export const DtkOpenCodePlugin: Plugin = async ({ $ }) => {
  try {
    await $`which dtk`.quiet()
  } catch {
    console.warn("[dtk] dtk binary not found in PATH — plugin disabled")
    return {}
  }

  return {
    "tool.execute.before": async (input, output) => {
      const tool = String(input?.tool ?? "").toLowerCase()
      if (tool !== "bash" && tool !== "shell") return
      const args = output?.args
      if (!args || typeof args !== "object") return

      const command = (args as Record<string, unknown>).command
      if (typeof command !== "string" || !command) return
      if (command.startsWith("dtk exec --")) return

      ;(args as Record<string, unknown>).command = `dtk exec -- ${command}`
    },
  }
}
"#;

fn plugin_path() -> PathBuf {
    PathBuf::from(".config")
        .join("opencode")
        .join("plugins")
        .join("dtk.ts")
}

pub(crate) fn install_opencode_guidance() -> io::Result<bool> {
    install_text_file(plugin_path(), OPENCODE_PLUGIN)
}

pub(crate) fn uninstall_opencode_guidance() -> io::Result<bool> {
    remove_if_exists(plugin_path())
}
