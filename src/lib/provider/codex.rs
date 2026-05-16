use std::io;

use super::template::{codex_dir, normalize_codex_agents, ProviderTemplate};

struct CodexProvider;

impl ProviderTemplate for CodexProvider {
    fn base_dir() -> std::path::PathBuf {
        codex_dir()
    }
}

pub(crate) fn install_codex_guidance() -> io::Result<bool> {
    CodexProvider::install_guidance_file()
}

pub(crate) fn uninstall_codex_guidance() -> io::Result<bool> {
    CodexProvider::uninstall_guidance_file()
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
    CodexProvider::install_skill_file()
}
