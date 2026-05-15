use std::process::ExitCode;

use super::{args, process};

pub(super) fn run_exec_command() -> ExitCode {
    match args::parse_exec_args() {
        Ok(options) => process::run_exec_flow(options),
        Err(code) => ExitCode::from(code as u8),
    }
}
