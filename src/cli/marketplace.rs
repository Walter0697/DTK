use dtk::{default_config_dir, FilterConfig};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, ExitCode};

const DEFAULT_MARKETPLACE_REPO: &str = "Walter0697/dtk-marketplace";
const MARKETPLACE_MANIFEST_VERSION: u32 = 1;
const MARKETPLACE_CACHE_VERSION: u32 = 2;

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

#[derive(Debug, Deserialize, Serialize)]
struct MarketplaceCacheMetadata {
    version: u32,
    repository: String,
    revision: String,
    files: Vec<String>,
    checksums: BTreeMap<String, String>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalStatus {
    Current,
    Modified,
    Missing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpdateAction {
    Update,
    Unchanged,
    Conflict,
    MissingUpstream,
}

struct MarketplaceSource {
    repository: String,
    revision: String,
    files: Vec<MarketplaceFile>,
    local_root: Option<PathBuf>,
    checksums: BTreeMap<String, String>,
}

pub(super) fn run_marketplace_command(args: Vec<String>) -> ExitCode {
    let mut args = args.into_iter();
    let Some(subcommand) = args.next() else {
        print_usage();
        return ExitCode::from(2);
    };
    let mut offline = false;
    let remaining = args
        .filter(|arg| {
            if arg == "--offline" {
                offline = true;
                false
            } else {
                true
            }
        })
        .collect::<Vec<_>>();

    match subcommand.as_str() {
        "refresh" => run_refresh(remaining, offline),
        "list" | "ls" => run_list(remaining, offline),
        "search" => run_search(remaining, offline),
        "installed" => run_installed(remaining),
        "info" => run_info(remaining, offline),
        "install" | "add" => run_install(remaining, offline),
        "uninstall" | "remove" => run_uninstall(remaining),
        "update" => run_update(remaining, offline),
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

fn run_refresh(args: Vec<String>, offline: bool) -> ExitCode {
    if offline {
        eprintln!("marketplace refresh cannot run with --offline");
        return ExitCode::from(2);
    }
    if !args.is_empty() {
        eprintln!("unexpected extra arguments");
        print_usage();
        return ExitCode::from(2);
    }
    if std::env::var("DTK_MARKETPLACE_PATH").is_ok() {
        eprintln!("marketplace refresh is unavailable when DTK_MARKETPLACE_PATH is set");
        return ExitCode::from(2);
    }
    match refresh_remote_source() {
        Ok(source) => {
            println!(
                "refreshed {} configs from {} at {}",
                source.files.len(),
                source.repository,
                short_revision(&source.revision)
            );
            ExitCode::from(0)
        }
        Err(err) => fail("refresh marketplace cache", err),
    }
}

fn run_installed(args: Vec<String>) -> ExitCode {
    if !args.is_empty() {
        eprintln!("unexpected extra arguments");
        print_usage();
        return ExitCode::from(2);
    }
    let manifest = match load_manifest() {
        Ok(manifest) => manifest,
        Err(err) => return fail("load marketplace install manifest", err),
    };
    if manifest.entries.is_empty() {
        println!("no marketplace-installed configs found");
        return ExitCode::from(0);
    }

    println!("Installed Marketplace Configs");
    println!("{}", "═".repeat(56));
    println!();
    println!("{:<16} {}", "Repository", manifest.repository);
    println!("{:<16} {}", "Revision", short_revision(&manifest.revision));
    println!("{:<16} {}", "Configs", manifest.entries.len());
    println!();
    print_installed_table(&manifest);
    ExitCode::from(0)
}

fn run_info(args: Vec<String>, offline: bool) -> ExitCode {
    if args.len() != 1 {
        eprintln!("marketplace info requires one category or config");
        print_usage();
        return ExitCode::from(2);
    }
    let source = match load_source(offline) {
        Ok(source) => source,
        Err(err) => return fail("load marketplace", err),
    };
    let selected = select_files(&source.files, &args[0]);
    if selected.is_empty() {
        eprintln!("no marketplace category or config matched: {}", args[0]);
        return ExitCode::from(1);
    }
    let manifest = match load_manifest() {
        Ok(manifest) => manifest,
        Err(err) => return fail("load marketplace install manifest", err),
    };

    if !is_exact_file_match(selected[0], &args[0]) {
        let installed = selected
            .iter()
            .filter(|file| manifest_entry_for_source(&manifest, &file.path).is_some())
            .count();
        println!("Marketplace Category");
        println!("{}", "═".repeat(56));
        println!();
        println!("{:<16} {}", "Category", args[0].trim_matches('/'));
        println!("{:<16} {}", "Configs", selected.len());
        println!("{:<16} {}", "Installed", installed);
        println!("{:<16} {}", "Revision", short_revision(&source.revision));
        println!();
        for file in selected {
            let status = manifest_entry_for_source(&manifest, &file.path)
                .map(|(filename, entry)| local_status(filename, entry))
                .unwrap_or(LocalStatus::Missing);
            let installed_label = if manifest_entry_for_source(&manifest, &file.path).is_some() {
                status.as_str()
            } else {
                "not installed"
            };
            println!("  {:<52} {}", trim_json_suffix(&file.path), installed_label);
        }
        return ExitCode::from(0);
    }

    let file = selected[0];
    let content = match source.read_file(&file.path) {
        Ok(content) => content,
        Err(err) => return fail(&format!("download {}", file.path), err),
    };
    let config = match serde_json::from_slice::<FilterConfig>(&content) {
        Ok(config) => config,
        Err(err) => return fail(&format!("parse {}", file.path), err),
    };
    println!("Marketplace Config");
    println!("{}", "═".repeat(56));
    println!();
    println!("{:<16} {}", "Path", trim_json_suffix(&file.path));
    println!(
        "{:<16} {}",
        "ID",
        config
            .id
            .as_deref()
            .or(config.name.as_deref())
            .unwrap_or("-")
    );
    println!(
        "{:<16} {}",
        "Source",
        config.source.as_deref().unwrap_or("-")
    );
    println!(
        "{:<16} {}",
        "Format",
        config.format.as_deref().unwrap_or("auto")
    );
    println!(
        "{:<16} {}",
        "Content path",
        config.content_path.as_deref().unwrap_or("-")
    );
    println!("{:<16} {}", "Allow fields", config.allow.len());
    if let Some(notes) = config.notes.as_deref() {
        println!("{:<16} {}", "Notes", notes);
    }
    match manifest_entry_for_source(&manifest, &file.path) {
        Some((filename, entry)) => {
            println!(
                "{:<16} {}",
                "Installed",
                local_status(filename, entry).as_str()
            );
            println!(
                "{:<16} {}",
                "Local path",
                default_config_dir()
                    .join("configs")
                    .join(filename)
                    .display()
            );
        }
        None => println!("{:<16} no", "Installed"),
    }
    println!("{:<16} {}", "Revision", short_revision(&source.revision));
    ExitCode::from(0)
}

fn run_list(args: Vec<String>, offline: bool) -> ExitCode {
    if args.len() > 1 {
        eprintln!("marketplace list accepts at most one category");
        print_usage();
        return ExitCode::from(2);
    }

    let source = match load_list_source(offline) {
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

fn run_search(args: Vec<String>, offline: bool) -> ExitCode {
    if args.len() != 1 {
        eprintln!("marketplace search requires one query");
        print_usage();
        return ExitCode::from(2);
    }
    let query = args[0].to_ascii_lowercase();
    let source = match load_source(offline) {
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

fn run_install(args: Vec<String>, offline: bool) -> ExitCode {
    let options = parse_options(args);
    if options.dry_run {
        eprintln!("--dry-run is only supported by marketplace update");
        print_usage();
        return ExitCode::from(2);
    }
    if options.positionals.len() != 1 {
        eprintln!("marketplace install requires one category or config");
        print_usage();
        return ExitCode::from(2);
    }

    let source = match load_source(offline) {
        Ok(source) => source,
        Err(err) => return fail("load marketplace", err),
    };
    let selected = select_files(&source.files, &options.positionals[0]);
    if selected.is_empty() {
        eprintln!(
            "no marketplace category or config matched: {}",
            options.positionals[0]
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
        if destination.exists() && !options.force {
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

fn run_uninstall(args: Vec<String>) -> ExitCode {
    let options = parse_options(args);
    if options.positionals.len() != 1 || options.dry_run {
        eprintln!("marketplace uninstall requires one category or config");
        print_usage();
        return ExitCode::from(2);
    }
    let target = &options.positionals[0];
    let mut manifest = match load_manifest() {
        Ok(manifest) => manifest,
        Err(err) => return fail("load marketplace install manifest", err),
    };
    let selected = select_manifest_entries(&manifest, target);
    if selected.is_empty() {
        eprintln!("no installed marketplace category or config matched: {target}");
        return ExitCode::from(1);
    }

    let mut removed = 0;
    let mut conflicts = 0;
    for filename in selected {
        let Some(entry) = manifest.entries.get(&filename) else {
            continue;
        };
        let destination = default_config_dir().join("configs").join(&filename);
        let status = local_status(&filename, entry);
        if status == LocalStatus::Modified && !options.force {
            eprintln!(
                "skipping locally modified config {} (use --force to remove)",
                destination.display()
            );
            conflicts += 1;
            continue;
        }
        if status != LocalStatus::Missing {
            if let Err(err) = fs::remove_file(&destination) {
                return fail(&format!("remove {}", destination.display()), err);
            }
        }
        manifest.entries.remove(&filename);
        println!("removed: {}", destination.display());
        removed += 1;
    }
    if let Err(err) = write_manifest(&manifest) {
        return fail("write marketplace install manifest", err);
    }
    println!("removed {removed}, conflicts {conflicts}");
    ExitCode::from(if conflicts > 0 { 1 } else { 0 })
}

fn run_update(args: Vec<String>, offline: bool) -> ExitCode {
    let options = parse_options(args);
    if !options.positionals.is_empty() {
        eprintln!("unexpected extra arguments");
        print_usage();
        return ExitCode::from(2);
    }
    let source = match load_source(offline) {
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
        let destination = configs_dir.join(filename);
        let local = match fs::read(&destination) {
            Ok(content) => Some(content),
            Err(err) if err.kind() == io::ErrorKind::NotFound => None,
            Err(err) => return fail(&format!("read {}", destination.display()), err),
        };
        let local_checksum = local.as_deref().map(checksum);
        let (action, remote) = if !available.contains(entry.source_path.as_str()) {
            (UpdateAction::MissingUpstream, None)
        } else {
            let remote = match source.read_file(&entry.source_path) {
                Ok(content) => content,
                Err(err) => return fail(&format!("download {}", entry.source_path), err),
            };
            if let Err(err) = validate_config(&remote) {
                return fail(&format!("validate {}", entry.source_path), err);
            }
            let action = plan_update_action(
                local_checksum.as_deref(),
                &entry.checksum,
                &checksum(&remote),
                options.force,
            );
            (action, Some(remote))
        };

        match action {
            UpdateAction::MissingUpstream => {
                eprintln!("missing upstream: {}", entry.source_path);
                missing += 1;
            }
            UpdateAction::Conflict => {
                eprintln!(
                    "conflict: {} is locally modified (use --force to overwrite)",
                    destination.display()
                );
                conflicts += 1;
            }
            UpdateAction::Unchanged => {
                if options.dry_run {
                    println!("unchanged: {}", destination.display());
                }
                unchanged += 1;
            }
            UpdateAction::Update => {
                if options.dry_run {
                    println!("would update: {}", destination.display());
                } else {
                    let remote = remote.expect("update action requires remote content");
                    if let Err(err) = fs::write(&destination, &remote) {
                        return fail(&format!("write {}", destination.display()), err);
                    }
                    entry.checksum = checksum(&remote);
                    println!("updated: {}", destination.display());
                }
                updated += 1;
            }
        }
    }

    if !options.dry_run {
        manifest.version = MARKETPLACE_MANIFEST_VERSION;
        manifest.repository = source.repository;
        manifest.revision = source.revision;
        if let Err(err) = write_manifest(&manifest) {
            return fail("write marketplace install manifest", err);
        }
    }
    println!(
        "{} {updated}, unchanged {unchanged}, conflicts {conflicts}, missing upstream {missing}",
        if options.dry_run {
            "would update"
        } else {
            "updated"
        }
    );
    ExitCode::from(if conflicts > 0 || missing > 0 { 1 } else { 0 })
}

impl LocalStatus {
    fn as_str(self) -> &'static str {
        match self {
            Self::Current => "current",
            Self::Modified => "modified",
            Self::Missing => "missing",
        }
    }
}

impl MarketplaceSource {
    fn read_file(&self, path: &str) -> io::Result<Vec<u8>> {
        if let Some(root) = &self.local_root {
            let content = fs::read(root.join(path))?;
            if let Some(expected) = self.checksums.get(path) {
                if checksum(&content) != *expected {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("cached marketplace checksum mismatch for {path}"),
                    ));
                }
            }
            return Ok(content);
        }
        let url = format!(
            "https://raw.githubusercontent.com/{}/{}/{}",
            self.repository, self.revision, path
        );
        curl_get(&url)
    }
}

fn load_source(offline: bool) -> io::Result<MarketplaceSource> {
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
            checksums: BTreeMap::new(),
        });
    }

    match load_cached_source() {
        Ok(source) => Ok(source),
        Err(err) if err.kind() == io::ErrorKind::NotFound && !offline => refresh_remote_source(),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Err(io::Error::new(
            io::ErrorKind::NotFound,
            "marketplace cache is unavailable; run `dtk marketplace refresh` while online",
        )),
        Err(err) => Err(err),
    }
}

fn load_list_source(offline: bool) -> io::Result<MarketplaceSource> {
    if offline || std::env::var("DTK_MARKETPLACE_PATH").is_ok() {
        return load_source(offline);
    }
    load_remote_catalog()
}

fn load_remote_catalog() -> io::Result<MarketplaceSource> {
    let repository = configured_repository();
    let tree = load_remote_tree(&repository)?;
    Ok(MarketplaceSource {
        repository,
        revision: tree.sha,
        files: marketplace_files_from_tree(tree.tree),
        local_root: None,
        checksums: BTreeMap::new(),
    })
}

fn refresh_remote_source() -> io::Result<MarketplaceSource> {
    let repository = configured_repository();
    let tree = load_remote_tree(&repository)?;
    let files = marketplace_files_from_tree(tree.tree);
    let snapshot_root = marketplace_cache_dir().join("snapshots").join(&tree.sha);
    let temporary_root = marketplace_cache_dir()
        .join("snapshots")
        .join(format!("{}.tmp", tree.sha));
    if temporary_root.exists() {
        fs::remove_dir_all(&temporary_root)?;
    }
    fs::create_dir_all(&temporary_root)?;
    let mut snapshot_checksums = BTreeMap::new();
    for file in &files {
        validate_marketplace_path(&file.path)?;
        let content = curl_get(&format!(
            "https://raw.githubusercontent.com/{}/{}/{}",
            repository, tree.sha, file.path
        ))?;
        validate_config(&content)?;
        snapshot_checksums.insert(file.path.clone(), checksum(&content));
        let destination = temporary_root.join(&file.path);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(destination, content)?;
    }
    if snapshot_root.exists() {
        fs::remove_dir_all(&snapshot_root)?;
    }
    fs::rename(temporary_root, &snapshot_root)?;
    let metadata = MarketplaceCacheMetadata {
        version: MARKETPLACE_CACHE_VERSION,
        repository,
        revision: tree.sha,
        files: files.iter().map(|file| file.path.clone()).collect(),
        checksums: snapshot_checksums,
    };
    write_cache_metadata(&metadata)?;
    source_from_cache_metadata(metadata)
}

fn load_remote_tree(repository: &str) -> io::Result<GitTreeResponse> {
    let url = format!("https://api.github.com/repos/{repository}/git/trees/main?recursive=1");
    let response = curl_get(&url)?;
    serde_json::from_slice::<GitTreeResponse>(&response).map_err(invalid_data)
}

fn marketplace_files_from_tree(tree: Vec<GitTreeEntry>) -> Vec<MarketplaceFile> {
    let mut files = tree
        .into_iter()
        .filter(|entry| {
            entry.kind == "blob"
                && entry.path.ends_with(".json")
                && entry.path != "marketplace-index.json"
        })
        .map(|entry| MarketplaceFile { path: entry.path })
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.path.cmp(&right.path));
    files
}

fn load_cached_source() -> io::Result<MarketplaceSource> {
    let content = fs::read(marketplace_cache_dir().join("current.json"))?;
    let metadata =
        serde_json::from_slice::<MarketplaceCacheMetadata>(&content).map_err(invalid_data)?;
    if metadata.version != MARKETPLACE_CACHE_VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "marketplace cache version is unsupported; run `dtk marketplace refresh`",
        ));
    }
    if metadata.repository != configured_repository() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "marketplace cache belongs to a different repository",
        ));
    }
    source_from_cache_metadata(metadata)
}

fn source_from_cache_metadata(metadata: MarketplaceCacheMetadata) -> io::Result<MarketplaceSource> {
    for path in &metadata.files {
        validate_marketplace_path(path)?;
    }
    let root = marketplace_cache_dir()
        .join("snapshots")
        .join(&metadata.revision);
    if !root.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "marketplace cache snapshot is missing",
        ));
    }
    Ok(MarketplaceSource {
        repository: metadata.repository,
        revision: metadata.revision,
        files: metadata
            .files
            .into_iter()
            .map(|path| MarketplaceFile { path })
            .collect(),
        local_root: Some(root),
        checksums: metadata.checksums,
    })
}

fn validate_marketplace_path(path: &str) -> io::Result<()> {
    if path.is_empty()
        || Path::new(path)
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsafe marketplace path: {path}"),
        ));
    }
    Ok(())
}

fn write_cache_metadata(metadata: &MarketplaceCacheMetadata) -> io::Result<()> {
    let cache_dir = marketplace_cache_dir();
    fs::create_dir_all(&cache_dir)?;
    let path = cache_dir.join("current.json");
    let temporary = cache_dir.join("current.json.tmp");
    let mut content = serde_json::to_vec_pretty(metadata).map_err(invalid_data)?;
    content.push(b'\n');
    fs::write(&temporary, content)?;
    fs::rename(temporary, path)
}

fn marketplace_cache_dir() -> PathBuf {
    std::env::var("DTK_MARKETPLACE_CACHE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            default_config_dir()
                .join("marketplace-cache")
                .join("default")
        })
}

fn configured_repository() -> String {
    std::env::var("DTK_MARKETPLACE_REPO").unwrap_or_else(|_| DEFAULT_MARKETPLACE_REPO.into())
}

fn collect_json_files(root: &Path, current: &Path, output: &mut Vec<String>) -> io::Result<()> {
    for entry in fs::read_dir(current)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_json_files(root, &path, output)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("json")
            && path.file_name().and_then(|name| name.to_str()) != Some("marketplace-index.json")
        {
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

fn is_exact_file_match(file: &MarketplaceFile, target: &str) -> bool {
    let target = target.trim_matches('/');
    file.path == target || file.path.trim_end_matches(".json") == target
}

fn select_manifest_entries(manifest: &MarketplaceManifest, target: &str) -> Vec<String> {
    let target = target.trim_matches('/');
    let exact = if target.ends_with(".json") {
        target.to_string()
    } else {
        format!("{target}.json")
    };
    manifest
        .entries
        .iter()
        .filter(|(filename, entry)| {
            entry.source_path == exact
                || entry.source_path.starts_with(&format!("{target}/"))
                || filename.as_str() == exact
                || filename.trim_end_matches(".json") == target
        })
        .map(|(filename, _)| filename.clone())
        .collect()
}

fn manifest_entry_for_source<'a>(
    manifest: &'a MarketplaceManifest,
    source_path: &str,
) -> Option<(&'a str, &'a InstalledEntry)> {
    manifest
        .entries
        .iter()
        .find(|(_, entry)| entry.source_path == source_path)
        .map(|(filename, entry)| (filename.as_str(), entry))
}

fn local_status(filename: &str, entry: &InstalledEntry) -> LocalStatus {
    match fs::read(default_config_dir().join("configs").join(filename)) {
        Ok(content) if checksum(&content) == entry.checksum => LocalStatus::Current,
        Ok(_) => LocalStatus::Modified,
        Err(err) if err.kind() == io::ErrorKind::NotFound => LocalStatus::Missing,
        Err(_) => LocalStatus::Modified,
    }
}

fn plan_update_action(
    local_checksum: Option<&str>,
    installed_checksum: &str,
    remote_checksum: &str,
    force: bool,
) -> UpdateAction {
    if local_checksum == Some(remote_checksum) {
        return UpdateAction::Unchanged;
    }
    if local_checksum.is_some() && local_checksum != Some(installed_checksum) && !force {
        return UpdateAction::Conflict;
    }
    UpdateAction::Update
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

fn print_installed_table(manifest: &MarketplaceManifest) {
    let rows = manifest
        .entries
        .iter()
        .map(|(filename, entry)| {
            (
                trim_json_suffix(&entry.source_path).to_string(),
                local_status(filename, entry).as_str(),
            )
        })
        .collect::<Vec<_>>();
    let path_width = rows
        .iter()
        .map(|(path, _)| path.len())
        .max()
        .unwrap_or("Config".len())
        .max("Config".len());
    let status_width = "Status".len().max(8);
    let table_width = 2 + path_width + 2 + status_width;
    println!("{}", "─".repeat(table_width));
    println!("  {:<path_width$}  {:<status_width$}", "Config", "Status");
    println!("{}", "─".repeat(table_width));
    for (path, status) in rows {
        println!("  {path:<path_width$}  {status:<status_width$}");
    }
    println!("{}", "─".repeat(table_width));
}

fn trim_json_suffix(path: &str) -> &str {
    path.trim_end_matches(".json")
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

struct MarketplaceOptions {
    positionals: Vec<String>,
    force: bool,
    dry_run: bool,
}

fn parse_options(args: Vec<String>) -> MarketplaceOptions {
    let mut options = MarketplaceOptions {
        positionals: Vec::new(),
        force: false,
        dry_run: false,
    };
    for arg in args {
        match arg.as_str() {
            "--force" => options.force = true,
            "--dry-run" => options.dry_run = true,
            _ => options.positionals.push(arg),
        }
    }
    options
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
    eprintln!(
        "usage: dtk marketplace <refresh|list|search|installed|info|install|uninstall|update> ..."
    );
    eprintln!("  dtk marketplace refresh");
    eprintln!("  dtk marketplace list [category] [--offline]");
    eprintln!("  dtk marketplace search <query> [--offline]");
    eprintln!("  dtk marketplace installed");
    eprintln!("  dtk marketplace info <category|config> [--offline]");
    eprintln!("  dtk marketplace install <category|config> [--force] [--offline]");
    eprintln!("  dtk marketplace uninstall <category|config> [--force]");
    eprintln!("  dtk marketplace update [--dry-run] [--force] [--offline]");
}

#[cfg(test)]
mod tests {
    use super::{
        checksum, group_files_by_category, is_exact_file_match, marketplace_files_from_tree,
        plan_update_action, select_files, select_manifest_entries, short_revision,
        validate_marketplace_path, GitTreeEntry, InstalledEntry, MarketplaceFile,
        MarketplaceManifest, UpdateAction,
    };
    use std::collections::BTreeMap;

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
    fn distinguishes_exact_config_from_single_config_category() {
        let file = MarketplaceFile {
            path: "notion/pat/notion_search_pat.json".to_string(),
        };
        assert!(is_exact_file_match(&file, "notion/pat/notion_search_pat"));
        assert!(!is_exact_file_match(&file, "notion/pat"));
    }

    #[test]
    fn checksum_is_stable() {
        assert_eq!(
            checksum(b"dtk"),
            "48f11ae0b92d42d5bfe89702270e475fa1bec9b7845c800064514a1ce7d06179"
        );
    }

    #[test]
    fn rejects_unsafe_marketplace_paths() {
        assert!(validate_marketplace_path("notion/search.json").is_ok());
        assert!(validate_marketplace_path("../outside.json").is_err());
        assert!(validate_marketplace_path("/absolute.json").is_err());
    }

    #[test]
    fn builds_catalog_from_config_files_only() {
        let files = marketplace_files_from_tree(vec![
            GitTreeEntry {
                path: "notion/search.json".to_string(),
                kind: "blob".to_string(),
            },
            GitTreeEntry {
                path: "marketplace-index.json".to_string(),
                kind: "blob".to_string(),
            },
            GitTreeEntry {
                path: "README.md".to_string(),
                kind: "blob".to_string(),
            },
            GitTreeEntry {
                path: "notion".to_string(),
                kind: "tree".to_string(),
            },
        ]);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "notion/search.json");
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

    #[test]
    fn selects_installed_entries_by_category_or_config() {
        let manifest = MarketplaceManifest {
            entries: BTreeMap::from([
                (
                    "notion_search_pat.json".to_string(),
                    InstalledEntry {
                        source_path: "notion/pat/notion_search_pat.json".to_string(),
                        checksum: "a".to_string(),
                    },
                ),
                (
                    "n8n_workflows_list.json".to_string(),
                    InstalledEntry {
                        source_path: "n8n/n8n_workflows_list.json".to_string(),
                        checksum: "b".to_string(),
                    },
                ),
            ]),
            ..MarketplaceManifest::default()
        };
        assert_eq!(select_manifest_entries(&manifest, "notion").len(), 1);
        assert_eq!(
            select_manifest_entries(&manifest, "n8n/n8n_workflows_list").len(),
            1
        );
    }

    #[test]
    fn plans_updates_without_overwriting_local_changes() {
        assert_eq!(
            plan_update_action(Some("old"), "old", "new", false),
            UpdateAction::Update
        );
        assert_eq!(
            plan_update_action(Some("local"), "old", "new", false),
            UpdateAction::Conflict
        );
        assert_eq!(
            plan_update_action(Some("local"), "old", "new", true),
            UpdateAction::Update
        );
        assert_eq!(
            plan_update_action(Some("new"), "old", "new", false),
            UpdateAction::Unchanged
        );
    }
}
