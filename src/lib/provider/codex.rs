use crate::DTK_GUIDE;
use std::io;

use super::{codex_dir, install_text_file, normalize_codex_agents, remove_if_exists};

pub(crate) fn install_codex_guidance() -> io::Result<bool> {
    let mut changed = false;
    changed |= install_text_file(codex_dir().join("DTK.md"), DTK_GUIDE)?;
    Ok(changed)
}

pub(crate) fn uninstall_codex_guidance() -> io::Result<bool> {
    remove_if_exists(codex_dir().join("DTK.md"))
}

pub(crate) fn install_codex_agents_attachment() -> io::Result<bool> {
    let path = codex_dir().join("AGENTS.md");
    let guide_path = codex_dir().join("DTK.md");
    let include_line = format!("@{}", guide_path.display());
    normalize_codex_agents(path, Some(include_line), None)
}

pub(crate) fn uninstall_codex_agents_attachment() -> io::Result<bool> {
    let path = codex_dir().join("AGENTS.md");
    let guide_path = codex_dir().join("DTK.md");
    let remove_line = format!("@{}", guide_path.display());
    normalize_codex_agents(path, None, Some(remove_line))
}

pub(crate) fn install_codex_skill() -> io::Result<bool> {
    install_text_file(
        codex_dir().join("skills").join("dtk").join("SKILL.md"),
        crate::DTK_CONFIG_ASSISTANT_SKILL,
    )
}
