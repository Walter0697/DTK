pub(crate) mod claude;
pub(crate) mod codex;
pub(crate) mod cursor;

pub(crate) use crate::install::{
    claude_dir, codex_dir, cursor_dir, hooks_are_empty, install_text_file, load_json_file,
    normalize_codex_agents, remove_if_exists, write_json_file,
};
