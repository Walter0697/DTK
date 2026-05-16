pub(super) fn normalize_command_args_for_metrics(command_args: &[String]) -> Vec<String> {
    shell_wrapper_command_args(command_args).unwrap_or_else(|| command_args.to_vec())
}

fn shell_wrapper_command_args(command_args: &[String]) -> Option<Vec<String>> {
    let script = shell_wrapper_script(command_args)?;
    shell_script_command_args(&script)
}

fn shell_wrapper_script(command_args: &[String]) -> Option<String> {
    let first = command_args.first()?;
    let first_name = binary_name(first);
    let mut index = if first_name == "env" { 1 } else { 0 };

    if first_name == "env" {
        while let Some(arg) = command_args.get(index) {
            if arg == "--" {
                index += 1;
                continue;
            }
            if arg.starts_with('-') || arg.contains('=') {
                index += 1;
                continue;
            }
            break;
        }
    }

    let shell = command_args.get(index)?;
    let shell_name = binary_name(shell);
    if !matches!(shell_name, "bash" | "sh" | "zsh" | "dash" | "fish") {
        return None;
    }

    let mut flag_index = index + 1;
    while let Some(arg) = command_args.get(flag_index) {
        if arg == "--" {
            flag_index += 1;
            continue;
        }
        if arg.starts_with('-') {
            if arg.contains('c') {
                return command_args.get(flag_index + 1).cloned();
            }
            flag_index += 1;
            continue;
        }
        break;
    }

    None
}

fn shell_script_command_args(script: &str) -> Option<Vec<String>> {
    for statement in split_shell_statements(script) {
        let tokens = tokenize_shell_words(&statement);
        if tokens.is_empty() {
            continue;
        }

        if is_shell_builtin(&tokens[0]) {
            continue;
        }

        if let Some(payload) = command_args_after_dtk_exec(&tokens) {
            return Some(payload);
        }

        return Some(tokens);
    }

    None
}

fn command_args_after_dtk_exec(tokens: &[String]) -> Option<Vec<String>> {
    if tokens.len() < 2 {
        return None;
    }

    if binary_name(&tokens[0]) != "dtk" || tokens.get(1).map(String::as_str) != Some("exec") {
        return None;
    }

    let payload_start = tokens.iter().position(|token| token == "--")? + 1;
    let payload = tokens[payload_start..].to_vec();
    if payload.is_empty() {
        None
    } else {
        Some(payload)
    }
}

fn split_shell_statements(script: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut chars = script.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;
    let mut escaped = false;

    while let Some(ch) = chars.next() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }

        match ch {
            '\\' if !in_single => {
                escaped = true;
            }
            '\'' if !in_double => {
                in_single = !in_single;
            }
            '"' if !in_single => {
                in_double = !in_double;
            }
            ';' | '\n' if !in_single && !in_double => {
                if !current.trim().is_empty() {
                    statements.push(current.trim().to_string());
                }
                current.clear();
            }
            '&' if !in_single && !in_double && matches!(chars.peek(), Some('&')) => {
                chars.next();
                if !current.trim().is_empty() {
                    statements.push(current.trim().to_string());
                }
                current.clear();
            }
            '|' if !in_single && !in_double && matches!(chars.peek(), Some('|')) => {
                chars.next();
                if !current.trim().is_empty() {
                    statements.push(current.trim().to_string());
                }
                current.clear();
            }
            '|' if !in_single && !in_double => {
                if !current.trim().is_empty() {
                    statements.push(current.trim().to_string());
                }
                current.clear();
            }
            _ => current.push(ch),
        }
    }

    if !current.trim().is_empty() {
        statements.push(current.trim().to_string());
    }

    statements
}

fn tokenize_shell_words(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut chars = input.chars().peekable();
    let mut in_single = false;
    let mut in_double = false;
    let mut escaped = false;

    while let Some(ch) = chars.next() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }

        match ch {
            '\\' if !in_single => {
                escaped = true;
            }
            '\'' if !in_double => {
                in_single = !in_single;
            }
            '"' if !in_single => {
                in_double = !in_double;
            }
            c if c.is_whitespace() && !in_single && !in_double => {
                if !current.is_empty() {
                    tokens.push(current.clone());
                    current.clear();
                }
            }
            _ => current.push(ch),
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    tokens
}

fn is_shell_builtin(command: &str) -> bool {
    matches!(
        command,
        "set"
            | "export"
            | "cd"
            | "source"
            | "."
            | "exec"
            | "command"
            | "builtin"
            | "eval"
            | "alias"
            | "unalias"
            | "umask"
            | "trap"
            | "readonly"
            | "shift"
            | "test"
            | "["
            | ":"
            | "true"
            | "false"
            | "pwd"
            | "printf"
            | "echo"
    )
}

fn binary_name(value: &str) -> &str {
    value.rsplit('/').next().unwrap_or(value)
}
