use std::path::{Path, PathBuf};

pub fn default_store_dir() -> PathBuf {
    std::env::var("DTK_STORE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| platform_data_dir().join("dtk").join("store"))
}

pub fn runtime_store_dir() -> PathBuf {
    default_store_dir()
}

pub fn default_usage_dir() -> PathBuf {
    std::env::var("DTK_USAGE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| default_store_dir())
}

pub fn runtime_usage_dir() -> PathBuf {
    if std::env::var("DTK_USAGE_DIR").is_ok() {
        return default_usage_dir();
    }

    let preferred = default_usage_dir();
    if dir_is_writable(&preferred) {
        preferred
    } else {
        std::env::temp_dir().join("dtk").join("usage")
    }
}

pub fn default_config_dir() -> PathBuf {
    std::env::var("DTK_CONFIG_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| platform_config_dir().join("dtk"))
}

pub fn platform_data_dir() -> PathBuf {
    if cfg!(windows) {
        windows_data_dir()
    } else {
        xdg_data_dir()
    }
}

pub fn platform_config_dir() -> PathBuf {
    if cfg!(windows) {
        windows_config_dir()
    } else {
        xdg_config_dir()
    }
}

pub fn xdg_data_dir() -> PathBuf {
    std::env::var("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("HOME").map(|home| PathBuf::from(home).join(".local/share")))
        .unwrap_or_else(|_| PathBuf::from(".local/share"))
}

pub fn xdg_config_dir() -> PathBuf {
    std::env::var("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("HOME").map(|home| PathBuf::from(home).join(".config")))
        .unwrap_or_else(|_| PathBuf::from(".config"))
}

pub fn windows_data_dir() -> PathBuf {
    std::env::var("LOCALAPPDATA")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("APPDATA").map(PathBuf::from))
        .unwrap_or_else(|_| PathBuf::from("."))
}

pub fn windows_config_dir() -> PathBuf {
    std::env::var("APPDATA")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("LOCALAPPDATA").map(PathBuf::from))
        .unwrap_or_else(|_| PathBuf::from("."))
}

pub fn filtered_payload_path(store_dir: impl AsRef<Path>, ref_id: &str) -> PathBuf {
    store_dir
        .as_ref()
        .join("filtered")
        .join(format!("{ref_id}.json"))
}

pub fn usage_db_path(store_dir: impl AsRef<Path>) -> PathBuf {
    store_dir.as_ref().join("usage.sqlite3")
}

pub(crate) fn dir_is_writable(path: &Path) -> bool {
    if std::fs::create_dir_all(path).is_err() {
        return false;
    }

    let test_path = path.join(".dtk-write-test");
    match std::fs::write(&test_path, b"ok") {
        Ok(()) => {
            let _ = std::fs::remove_file(&test_path);
            true
        }
        Err(_) => false,
    }
}
