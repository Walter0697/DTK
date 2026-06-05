use dtk::{default_config_dir, FilterConfig};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

const DEFAULT_MARKETPLACE_REPO: &str = "Walter0697/dtk-marketplace";
const MARKETPLACE_MANIFEST_VERSION: u32 = 1;

#[derive(Debug, Clone)]
struct MarketplaceFile {
    path: String,
}

#[derive(Debug, Deserialize)]
struct GitTreeResponse {
    sha: String,
    tree: Vec<GitTreeEntry>,
}

#[derive(Debug, Deserialize)]
struct GitTreeEntry {
    path: String,
    #[serde(rename = "type")]
    kind: String,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct MarketplaceManifest {
    version: u32,
    repository: String,
    revision: String,
    entries: BTreeMap<String, InstalledEntry>,
}

#[derive(Debug, Deserialize, Serialize)]
struct InstalledEntry {
    source_path: String,
    checksum: String,
}

struct MarketplaceSource {
    repository: String,
    revision: String,
    files: Vec<MarketplaceFile>,
    local_root: Option<PathBuf>,
}

pub(super) fn run_marketplace_command(args: Vec<String>) -> ExitCode {
    let mut args = args.into_iter();
    let Some(subcommand) = args.next() else {
        print_usage();
        return ExitCode::from(2);
    };
    let remaining = args.collect::<Vec<_>>();

    match subcommand.as_str() {
        "list" | "ls" => run_list(remaining),
        "search" => run_search(remaining),
        "install" | "add" => run_install(remaining),
        "update" => run_update(remaining),
        "help" | "--help" | "-h" => {
            print_usage();
            ExitCode::from(0)
        }
        other => {
            eprintln!("unknown marketplace subcommand: {other}");
            print_usage();
            ExitCode::from(2)
        }
    }
}

fn run_list(args: Vec<String>) -> ExitCode {
    if args.len() > 1 {
        eprintln!("marketplace list accepts at most one category");
        print_usage();
        return ExitCode::from(2);
    }

    let source = match load_source() {
        Ok(source) => source,
        Err(err) => return fail("load marketplace", err),
    };
    let categories = group_files_by_category(&source.files);
    if let Some(category) = args.first() {
        let Some(files) = categories.get(category) else {
            eprintln!("unknown marketplace category: {category}");
            return ExitCode::from(1);
        };
        print_category_tree(category, files, "config");
    } else {
        println!("DTK Marketplace");
        println!("{}", "═".repeat(56));
        println!();
        println!("{:<16} {}", "Categories", categories.len());
        println!("{:<16} {}", "Configs", source.files.len());
        println!();
        println!("By Category");
        print_category_table(&categories);
        println!("\nUse `dtk marketplace list <category>` to browse its configs.");
    }
    println!("Revision: {}", short_revision(&source.revision));
    ExitCode::from(0)
}

fn run_search(args: Vec<String>) -> ExitCode {
    if args.len() != 1 {
        eprintln!("marketplace search requires one query");
        print_usage();
        return ExitCode::from(2);
    }
    let query = args[0].to_ascii_lowercase();
    let source = match load_source() {
        Ok(source) => source,
        Err(err) => return fail("load marketplace", err),
    };
    let matches = source
        .files
        .iter()
        .filter(|file| file.path.to_ascii_lowercase().contains(&query))
        .collect::<Vec<_>>();

    if matches.is_empty() {
        println!("no marketplace configs matched: {}", args[0]);
    } else {
        let grouped = group_file_refs_by_category(matches);
        let match_count = grouped.values().map(|files| files.len()).sum::<usize>();
        println!(
            "Marketplace search: {} - {} matches in {} categories\n",
            args[0],
            match_count,
            grouped.len()
        );
        for (category, files) in grouped {
            print_category_tree(&category, &files, "match");
            println!();
        }
    }
    ExitCode::from(0)
}

fn run_install(args: Vec<String>) -> ExitCode {
    let (positionals, force) = parse_force(args);
    if positionals.len() != 1 {
        eprintln!("marketplace install requires one category or config");
        print_usage();
        return ExitCode::from(2);
    }

    let source = match load_source() {
        Ok(source) => source,
        Err(err) => return fail("load marketplace", err),
    };
    let selected = select_files(&source.files, &positionals[0]);
    if selected.is_empty() {
        eprintln!(
            "no marketplace category or config matched: {}",
            positionals[0]
        );
        return ExitCode::from(1);
    }

    let mut manifest = match load_manifest() {
        Ok(manifest) => manifest,
        Err(err) => return fail("load marketplace install manifest", err),
    };
    let configs_dir = default_config_dir().join("configs");
    if let Err(err) = fs::create_dir_all(&configs_dir) {
        return fail("create config directory", err);
    }

    let mut installed = 0;
    let mut skipped = 0;
    for file in selected {
        let content = match source.read_file(&file.path) {
            Ok(content) => content,
            Err(err) => return fail(&format!("download {}", file.path), err),
        };
        if let Err(err) = validate_config(&content) {
            return fail(&format!("validate {}", file.path), err);
        }

        let Some(filename) = Path::new(&file.path)
            .file_name()
            .and_then(|name| name.to_str())
        else {
            eprintln!("invalid marketplace config path: {}", file.path);
            return ExitCode::from(1);
        };
        let destination = configs_dir.join(filename);
        if destination.exists() && !force {
            eprintln!(
                "skipping existing config {} (use --force to overwrite)",
                destination.display()
            );
            skipped += 1;
            continue;
        }
        if let Err(err) = fs::write(&destination, &content) {
            return fail(&format!("write {}", destination.display()), err);
        }
        manifest.entries.insert(
            filename.to_string(),
            InstalledEntry {
                source_path: file.path.clone(),
                checksum: checksum(&content),
            },
        );
        println!("installed: {} -> {}", file.path, destination.display());
        installed += 1;
    }

    manifest.version = MARKETPLACE_MANIFEST_VERSION;
    manifest.repository = source.repository;
    manifest.revision = source.revision;
    if let Err(err) = write_manifest(&manifest) {
        return fail("write marketplace install manifest", err);
    }
    println!("installed {installed}, skipped {skipped}");
    ExitCode::from(0)
}

fn run_update(args: Vec<String>) -> ExitCode {
    let (positionals, force) = parse_force(args);
    if !positionals.is_empty() {
        eprintln!("unexpected extra arguments");
        print_usage();
        return ExitCode::from(2);
    }
    let source = match load_source() {
        Ok(source) => source,
        Err(err) => return fail("load marketplace", err),
    };
    let mut manifest = match load_manifest() {
        Ok(manifest) => manifest,
        Err(err) => return fail("load marketplace install manifest", err),
    };
    if manifest.entries.is_empty() {
        println!("no marketplace-installed configs found");
        return ExitCode::from(0);
    }

    let available = source
        .files
        .iter()
        .map(|file| file.path.as_str())
        .collect::<BTreeSet<_>>();
    let configs_dir = default_config_dir().join("configs");
    let mut updated = 0;
    let mut unchanged = 0;
    let mut conflicts = 0;
    let mut missing = 0;

    for (filename, entry) in &mut manifest.entries {
        if !available.contains(entry.source_path.as_str()) {
            eprintln!("upstream config missing: {}", entry.source_path);
            missing += 1;
            continue;
        }
        let destination = configs_dir.join(filename);
        let local = match fs::read(&destination) {
            Ok(content) => content,
            Err(err) if err.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(err) => return fail(&format!("read {}", destination.display()), err),
        };
        let local_checksum = if local.is_empty() {
            None
        } else {
            Some(checksum(&local))
        };
        if local_checksum.as_deref() != Some(entry.checksum.as_str())
            && local_checksum.is_some()
            && !force
        {
            eprintln!(
                "skipping locally modified config {} (use --force to overwrite)",
                destination.display()
            );
            conflicts += 1;
            continue;
        }

        let remote = match source.read_file(&entry.source_path) {
            Ok(content) => content,
            Err(err) => return fail(&format!("download {}", entry.source_path), err),
        };
        if let Err(err) = validate_config(&remote) {
            return fail(&format!("validate {}", entry.source_path), err);
        }
        let remote_checksum = checksum(&remote);
        if local_checksum.as_deref() == Some(remote_checksum.as_str()) {
            unchanged += 1;
            continue;
        }
        if let Err(err) = fs::write(&destination, &remote) {
            return fail(&format!("write {}", destination.display()), err);
        }
        entry.checksum = remote_checksum;
        println!("updated: {}", destination.display());
        updated += 1;
    }

    manifest.version = MARKETPLACE_MANIFEST_VERSION;
    manifest.repository = source.repository;
    manifest.revision = source.revision;
    if let Err(err) = write_manifest(&manifest) {
        return fail("write marketplace install manifest", err);
    }
    println!(
        "updated {updated}, unchanged {unchanged}, conflicts {conflicts}, missing upstream {missing}"
    );
    ExitCode::from(if conflicts > 0 || missing > 0 { 1 } else { 0 })
}

impl MarketplaceSource {
    fn read_file(&self, path: &str) -> io::Result<Vec<u8>> {
        if let Some(root) = &self.local_root {
            return fs::read(root.join(path));
        }
        let url = format!(
            "https://raw.githubusercontent.com/{}/{}/{}",
            self.repository, self.revision, path
        );
        curl_get(&url)
    }
}

fn load_source() -> io::Result<MarketplaceSource> {
    if let Ok(path) = std::env::var("DTK_MARKETPLACE_PATH") {
        let root = PathBuf::from(path);
        let mut paths = Vec::new();
        collect_json_files(&root, &root, &mut paths)?;
        paths.sort();
        return Ok(MarketplaceSource {
            repository: root.display().to_string(),
            revision: "local".to_string(),
            files: paths
                .into_iter()
                .map(|path| MarketplaceFile { path })
                .collect(),
            local_root: Some(root),
        });
    }

    let repository =
        std::env::var("DTK_MARKETPLACE_REPO").unwrap_or_else(|_| DEFAULT_MARKETPLACE_REPO.into());
    let url = format!("https://api.github.com/repos/{repository}/git/trees/main?recursive=1");
    let response = curl_get(&url)?;
    let tree = serde_json::from_slice::<GitTreeResponse>(&response).map_err(invalid_data)?;
    let mut files = tree
        .tree
        .into_iter()
        .filter(|entry| entry.kind == "blob" && entry.path.ends_with(".json"))
        .map(|entry| MarketplaceFile { path: entry.path })
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(MarketplaceSource {
        repository,
        revision: tree.sha,
        files,
        local_root: None,
    })
}

fn collect_json_files(root: &Path, current: &Path, output: &mut Vec<String>) -> io::Result<()> {
    for entry in fs::read_dir(current)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_json_files(root, &path, output)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("json") {
            let relative = path.strip_prefix(root).map_err(invalid_data)?;
            output.push(relative.to_string_lossy().replace('\\', "/"));
        }
    }
    Ok(())
}

fn select_files<'a>(files: &'a [MarketplaceFile], target: &str) -> Vec<&'a MarketplaceFile> {
    let target = target.trim_matches('/');
    let exact = if target.ends_with(".json") {
        target.to_string()
    } else {
        format!("{target}.json")
    };
    files
        .iter()
        .filter(|file| file.path == exact || file.path.starts_with(&format!("{target}/")))
        .collect()
}

fn group_files_by_category(files: &[MarketplaceFile]) -> BTreeMap<String, Vec<&MarketplaceFile>> {
    group_file_refs_by_category(files.iter().collect())
}

fn group_file_refs_by_category(
    files: Vec<&MarketplaceFile>,
) -> BTreeMap<String, Vec<&MarketplaceFile>> {
    let mut categories = BTreeMap::<String, Vec<&MarketplaceFile>>::new();
    for file in files {
        if let Some(category) = file.path.split('/').next() {
            categories
                .entry(category.to_string())
                .or_default()
                .push(file);
        }
    }
    categories
}

fn print_category_tree(category: &str, files: &[&MarketplaceFile], singular_label: &str) {
    let label = if files.len() == 1 {
        singular_label.to_string()
    } else if singular_label == "match" {
        "matches".to_string()
    } else {
        format!("{singular_label}s")
    };
    println!("{category} ({} {label})", files.len());
    for file in files {
        let child = file
            .path
            .strip_prefix(&format!("{category}/"))
            .unwrap_or(&file.path)
            .trim_end_matches(".json");
        println!("  {child}");
    }
}

fn print_category_table(categories: &BTreeMap<String, Vec<&MarketplaceFile>>) {
    let row_width = categories.len().to_string().len().max(1);
    let category_width = categories
        .keys()
        .map(String::len)
        .max()
        .unwrap_or_default()
        .max("CATEGORY".len());
    let configs_width = categories
        .values()
        .map(|files| files.len().to_string().len())
        .max()
        .unwrap_or_default()
        .max("CONFIGS".len());
    let table_width = 2 + row_width + 2 + category_width + 2 + configs_width;

    println!("{}", "─".repeat(table_width));
    println!(
        "  {:<row_width$}  {:<category_width$}  {:>configs_width$}",
        "#", "Category", "Configs"
    );
    println!("{}", "─".repeat(table_width));
    for (index, (category, files)) in categories.iter().enumerate() {
        println!(
            "  {:>row_width$}  {category:<category_width$}  {:>configs_width$}",
            index + 1,
            files.len()
        );
    }
    println!("{}", "─".repeat(table_width));
}

fn short_revision(revision: &str) -> &str {
    revision.get(..12).unwrap_or(revision)
}

fn validate_config(content: &[u8]) -> io::Result<()> {
    let config = serde_json::from_slice::<FilterConfig>(content).map_err(invalid_data)?;
    if config
        .id
        .as_deref()
        .map(str::trim)
        .unwrap_or_default()
        .is_empty()
        && config
            .name
            .as_deref()
            .map(str::trim)
            .unwrap_or_default()
            .is_empty()
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "marketplace config must have an id or name",
        ));
    }
    Ok(())
}

fn load_manifest() -> io::Result<MarketplaceManifest> {
    let path = manifest_path();
    match fs::read(path) {
        Ok(content) => serde_json::from_slice(&content).map_err(invalid_data),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(MarketplaceManifest::default()),
        Err(err) => Err(err),
    }
}

fn write_manifest(manifest: &MarketplaceManifest) -> io::Result<()> {
    let path = manifest_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut content = serde_json::to_vec_pretty(manifest).map_err(invalid_data)?;
    content.push(b'\n');
    fs::write(path, content)
}

fn manifest_path() -> PathBuf {
    default_config_dir().join("marketplace.json")
}

fn parse_force(args: Vec<String>) -> (Vec<String>, bool) {
    let mut force = false;
    let mut positionals = Vec::new();
    for arg in args {
        if arg == "--force" {
            force = true;
        } else {
            positionals.push(arg);
        }
    }
    (positionals, force)
}

fn checksum(content: &[u8]) -> String {
    format!("{:x}", Sha256::digest(content))
}

fn curl_get(url: &str) -> io::Result<Vec<u8>> {
    let output = Command::new("curl")
        .args(["-fsSL", "-H", "User-Agent: dtk", url])
        .output()
        .map_err(|err| io::Error::new(err.kind(), format!("failed to run curl: {err}")))?;
    if !output.status.success() {
        return Err(io::Error::other(format!(
            "curl request failed with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(output.stdout)
}

fn invalid_data(err: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, err.to_string())
}

fn fail(action: &str, err: impl std::fmt::Display) -> ExitCode {
    eprintln!("failed to {action}: {err}");
    ExitCode::from(1)
}

fn print_usage() {
    eprintln!("usage: dtk marketplace <list|search|install|update> ...");
    eprintln!("  dtk marketplace list [category]");
    eprintln!("  dtk marketplace search <query>");
    eprintln!("  dtk marketplace install <category|config> [--force]");
    eprintln!("  dtk marketplace update [--force]");
}

#[cfg(test)]
mod tests {
    use super::{checksum, group_files_by_category, select_files, short_revision, MarketplaceFile};

    #[test]
    fn selects_category_recursively() {
        let files = vec![
            MarketplaceFile {
                path: "notion/pat/notion_search_pat.json".to_string(),
            },
            MarketplaceFile {
                path: "n8n/n8n_workflows_list.json".to_string(),
            },
        ];
        let selected = select_files(&files, "notion");
        assert_eq!(selected.len(), 1);
        assert_eq!(selected[0].path, "notion/pat/notion_search_pat.json");
    }

    #[test]
    fn selects_config_without_json_suffix() {
        let files = vec![MarketplaceFile {
            path: "n8n/n8n_workflows_list.json".to_string(),
        }];
        let selected = select_files(&files, "n8n/n8n_workflows_list");
        assert_eq!(selected.len(), 1);
    }

    #[test]
    fn checksum_is_stable() {
        assert_eq!(
            checksum(b"dtk"),
            "48f11ae0b92d42d5bfe89702270e475fa1bec9b7845c800064514a1ce7d06179"
        );
    }

    #[test]
    fn groups_files_by_top_level_category() {
        let files = vec![
            MarketplaceFile {
                path: "notion/pat/notion_search_pat.json".to_string(),
            },
            MarketplaceFile {
                path: "notion/connector/notion_search_connector.json".to_string(),
            },
            MarketplaceFile {
                path: "n8n/n8n_workflows_list.json".to_string(),
            },
        ];
        let grouped = group_files_by_category(&files);
        assert_eq!(grouped["notion"].len(), 2);
        assert_eq!(grouped["n8n"].len(), 1);
    }

    #[test]
    fn shortens_git_revisions_but_preserves_local_revision() {
        assert_eq!(short_revision("f3d3b78c8162f932"), "f3d3b78c8162");
        assert_eq!(short_revision("local"), "local");
    }
}
