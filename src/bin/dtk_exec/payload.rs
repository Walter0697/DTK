use std::io::ErrorKind;
use std::process::{Command, Output, Stdio};

pub(super) fn run_payload_command(
    command_args: &[String],
    use_rtk: bool,
) -> Result<Output, std::io::Error> {
    if use_rtk && is_curl_command(command_args) {
        match run_payload_command_through_rtk_proxy(command_args) {
            Ok(output) => Ok(output),
            Err(err) if err.kind() == ErrorKind::NotFound => {
                run_payload_command_direct(command_args)
            }
            Err(err) => Err(err),
        }
    } else {
        run_payload_command_direct(command_args)
    }
}

fn run_payload_command_through_rtk_proxy(
    command_args: &[String],
) -> Result<Output, std::io::Error> {
    let mut command = Command::new("rtk");
    command.arg("proxy");
    command.args(command_args);
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    command.output()
}

fn run_payload_command_direct(command_args: &[String]) -> Result<Output, std::io::Error> {
    let mut command = Command::new(&command_args[0]);
    if command_args.len() > 1 {
        command.args(&command_args[1..]);
    }
    command.stdout(Stdio::piped()).stderr(Stdio::piped());
    command.output()
}

fn is_curl_command(command_args: &[String]) -> bool {
    command_args
        .first()
        .map(|value| value == "curl")
        .unwrap_or(false)
}

pub(super) fn should_return_original_output(
    original_tokens: usize,
    filtered_tokens: usize,
) -> bool {
    filtered_tokens > original_tokens
}
