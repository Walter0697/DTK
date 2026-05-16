use crate::DTK_GUIDE;
use std::io;
use std::path::PathBuf;

pub(crate) use crate::install::{
    claude_dir, codex_dir, cursor_dir, hooks_are_empty, install_text_file, load_json_file,
    normalize_codex_agents, remove_if_exists, write_json_file,
};

pub(crate) trait ProviderTemplate {
    fn base_dir() -> PathBuf;

    fn guidance_path() -> PathBuf {
        Self::base_dir().join("DTK.md")
    }

    fn skill_path() -> PathBuf {
        Self::base_dir().join("skills").join("dtk").join("SKILL.md")
    }

    fn install_guidance_file() -> io::Result<bool> {
        crate::install::install_text_file(Self::guidance_path(), DTK_GUIDE)
    }

    fn uninstall_guidance_file() -> io::Result<bool> {
        crate::install::remove_if_exists(Self::guidance_path())
    }

    fn install_skill_file() -> io::Result<bool> {
        crate::install::install_text_file(Self::skill_path(), crate::DTK_CONFIG_ASSISTANT_SKILL)
    }
}
