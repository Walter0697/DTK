#[derive(Debug, Clone)]
pub(super) struct ExecOptions {
    pub(super) config_path: Option<String>,
    pub(super) retention_days: Option<u64>,
    pub(super) use_rtk: bool,
    pub(super) command_args: Vec<String>,
}

pub(super) fn parse_exec_args() -> Result<ExecOptions, i32> {
    parse_exec_args_from(std::env::args().skip(1))
}

fn parse_exec_args_from<I>(args: I) -> Result<ExecOptions, i32>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut config_path: Option<String> = None;
    let mut retention_days: Option<u64> = None;
    let mut use_rtk = false;
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
                config_path = Some(path);
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
            "--use-rtk" => {
                use_rtk = true;
            }
            "--help" | "-h" => {
                print_exec_usage();
                return Err(0);
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
        print_exec_usage();
        return Err(2);
    }

    Ok(ExecOptions {
        config_path,
        retention_days,
        use_rtk,
        command_args,
    })
}

fn print_exec_usage() {
    eprintln!(
        "usage: dtk_exec [--config PATH] [--retention-days N] [--use-rtk] -- <command> [args...]"
    );
}

#[cfg(test)]
mod tests {
    use super::parse_exec_args_from;

    #[test]
    fn prints_usage_for_help_flag() {
        let args = vec!["--help".to_string()];
        let err = parse_exec_args_from(args).expect_err("help should exit early");
        assert_eq!(err, 0);
    }

    #[test]
    fn parses_use_rtk_flag_before_separator() {
        let args = vec![
            "--use-rtk".to_string(),
            "--config".to_string(),
            "notion.json".to_string(),
            "--".to_string(),
            "curl".to_string(),
            "-sS".to_string(),
            "https://example.com".to_string(),
        ];

        let parsed = parse_exec_args_from(args).expect("parsed");
        assert!(parsed.use_rtk);
        assert_eq!(parsed.config_path.as_deref(), Some("notion.json"));
        assert_eq!(
            parsed.command_args,
            vec!["curl", "-sS", "https://example.com"]
        );
    }
}
