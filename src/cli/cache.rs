use super::*;

pub(super) fn run_cache_command(args: Vec<String>) -> ExitCode {
    let mut iter = args.into_iter();
    let Some(subcommand) = iter.next() else {
        print_cache_usage();
        return ExitCode::from(2);
    };

    match subcommand.as_str() {
        "list" => run_cache_list(),
        "show" => {
            let Some(ref_id) = iter.next() else {
                eprintln!("missing ref_id");
                print_cache_usage();
                return ExitCode::from(2);
            };
            run_cache_show(&ref_id)
        }
        "help" | "-h" | "--help" => {
            print_cache_usage();
            ExitCode::from(0)
        }
        other => {
            eprintln!("unknown cache subcommand: {other}");
            print_cache_usage();
            ExitCode::from(2)
        }
    }
}

fn print_cache_usage() {
    eprintln!("Usage: dtk cache <list|show> [ref_id]");
    eprintln!("  dtk cache list");
    eprintln!("  dtk cache show <ref_id>");
}

fn run_cache_list() -> ExitCode {
    let store_dir = runtime_store_dir();
    let index = match read_store_index(&store_dir) {
        Ok(index) => index,
        Err(err) => {
            eprintln!("failed to read DTK cache index: {err}");
            return ExitCode::from(1);
        }
    };

    if index.is_empty() {
        println!("no cache entries");
        return ExitCode::from(0);
    }

    let mut entries: Vec<_> = index.into_iter().collect();
    entries.sort_by(|left, right| {
        left.1
            .created_at_unix_ms
            .cmp(&right.1.created_at_unix_ms)
            .then(left.0.cmp(&right.0))
    });

    let mut rows: Vec<Vec<String>> = Vec::new();
    for (ref_id, entry) in entries {
        let filtered_path = filtered_payload_path(&store_dir, &ref_id);
        let original_tokens = token_count_for_path(Path::new(&entry.path))
            .map(|value| value.to_string())
            .unwrap_or_else(|_| "-".to_string());
        let filtered_tokens = token_count_for_path(filtered_path.as_path())
            .map(|value| value.to_string())
            .unwrap_or_else(|_| "-".to_string());
        rows.push(vec![
            ref_id,
            age_from_unix_ms(entry.created_at_unix_ms),
            original_tokens.clone(),
            filtered_tokens.clone(),
            token_delta_for_tokens(&original_tokens, &filtered_tokens),
        ]);
    }

    print_cache_table(
        &["ref_id", "age", "orig_tokens", "filtered_tokens", "delta"],
        &rows,
    );

    ExitCode::from(0)
}

fn run_cache_show(ref_id: &str) -> ExitCode {
    let store_dir = runtime_store_dir();
    let index = match read_store_index(&store_dir) {
        Ok(index) => index,
        Err(err) => {
            eprintln!("failed to read DTK cache index: {err}");
            return ExitCode::from(1);
        }
    };

    let Some(entry) = index.get(ref_id) else {
        eprintln!("unknown ref_id: {ref_id}");
        return ExitCode::from(1);
    };

    let filtered_path = filtered_payload_path(&store_dir, ref_id);
    println!("ref_id: {ref_id}");
    println!("created_at_unix_ms: {}", entry.created_at_unix_ms);
    println!(
        "retention_days: {}",
        entry
            .retention_days
            .map(|days| days.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "expires_at_unix_ms: {}",
        entry
            .expires_at_unix_ms
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string())
    );
    println!("original_path: {}", entry.path);
    println!("filtered_path: {}", filtered_path.display());
    println!();
    println!("--- original ---");
    match fs::read_to_string(&entry.path) {
        Ok(content) => print_json_or_raw(&content),
        Err(err) => {
            eprintln!("failed to read original payload: {err}");
            return ExitCode::from(1);
        }
    }
    println!();
    println!("--- filtered ---");
    match fs::read_to_string(&filtered_path) {
        Ok(content) => print_json_or_raw(&content),
        Err(err) if err.kind() == io::ErrorKind::NotFound => println!("[missing]"),
        Err(err) => {
            eprintln!("failed to read filtered payload: {err}");
            return ExitCode::from(1);
        }
    }

    ExitCode::from(0)
}

fn print_json_or_raw(content: &str) {
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(content) {
        match serde_json::to_string_pretty(&value) {
            Ok(text) => println!("{text}"),
            Err(_) => println!("{content}"),
        }
    } else {
        println!("{content}");
    }
}
