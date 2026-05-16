use dtk::DEFAULT_SAMPLE_CONFIG_NAME;

#[derive(Debug, Clone)]
pub(super) struct ExecOptions {
    pub(super) config_path: String,
    pub(super) retention_days: Option<u64>,
    pub(super) command_args: Vec<String>,
}

pub(super) fn parse_exec_args() -> Result<ExecOptions, i32> {
    let mut args = std::env::args().skip(1);
    let mut config_path = DEFAULT_SAMPLE_CONFIG_NAME.to_string();
    let mut retention_days: Option<u64> = None;
    let mut command_args: Vec<String> = Vec::new();
    let mut seen_separator = false;

    while let Some(arg) = args.next() {
        if seen_separator {
            command_args.push(arg);
            continue;
        }

        match arg.as_str() {
            "--config" => {
                let Some(path) = args.next() else {
                    eprintln!("missing value for --config");
                    return Err(2);
                };
                config_path = path;
            }
            "--retention-days" => {
                let Some(days) = args.next() else {
                    eprintln!("missing value for --retention-days");
                    return Err(2);
                };
                let Ok(parsed) = days.parse::<u64>() else {
                    eprintln!("invalid retention days: {days}");
                    return Err(2);
                };
                retention_days = Some(parsed);
            }
            "--" => {
                seen_separator = true;
            }
            other => {
                command_args.push(other.to_string());
                seen_separator = true;
            }
        }
    }

    if command_args.is_empty() {
        eprintln!("usage: dtk_exec [--config PATH] [--retention-days N] -- <command> [args...]");
        return Err(2);
    }

    Ok(ExecOptions {
        config_path,
        retention_days,
        command_args,
    })
}
