use dtk::{default_config_dir, load_filter_config};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use super::{print_config_list_usage, resolve::resolve_config_identity};

#[derive(Debug, Clone)]
struct ConfigEntry {
    identifier: String,
    config_id: Option<String>,
    path: PathBuf,
}

pub(super) fn run_config_list_command(args: Vec<String>) -> ExitCode {
    if !args.is_empty() {
        eprintln!("unexpected extra arguments");
        print_config_list_usage();
        return ExitCode::from(2);
    }

    let config_dir = default_config_dir().join("configs");
    let entries = match list_config_entries(&config_dir) {
        Ok(entries) => entries,
        Err(err) => {
            eprintln!("failed to list configs in {}: {err}", config_dir.display());
            return ExitCode::from(1);
        }
    };

    if entries.is_empty() {
        println!("no configs found");
        return ExitCode::from(0);
    }

    println!("{:<24} {:<24} {}", "identifier", "config_id", "path");
    for entry in entries {
        println!(
            "{:<24} {:<24} {}",
            entry.identifier,
            entry.config_id.unwrap_or_else(|| "-".to_string()),
            entry.path.display()
        );
    }

    ExitCode::from(0)
}

fn list_config_entries(config_dir: &Path) -> io::Result<Vec<ConfigEntry>> {
    if !config_dir.exists() {
        return Ok(Vec::new());
    }

    let mut entries = Vec::new();
    for entry in fs::read_dir(config_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() || path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }

        let config = match load_filter_config(&path) {
            Ok(config) => config,
            Err(err) => {
                eprintln!("skipping invalid config {}: {err}", path.display());
                continue;
            }
        };
        let identifier = path
            .strip_prefix(config_dir)
            .ok()
            .and_then(|relative| {
                relative
                    .to_str()
                    .map(|value| value.trim_end_matches(".json").to_string())
            })
            .or_else(|| {
                path.file_stem()
                    .and_then(|value| value.to_str())
                    .map(|value| value.to_string())
            })
            .unwrap_or_else(|| path.display().to_string());
        entries.push(ConfigEntry {
            identifier,
            config_id: resolve_config_identity(&config, &path),
            path,
        });
    }

    entries.sort_by(|left, right| left.identifier.cmp(&right.identifier));
    Ok(entries)
}
