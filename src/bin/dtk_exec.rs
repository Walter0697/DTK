#[path = "dtk_exec/args.rs"]
mod args;
#[path = "dtk_exec/payload.rs"]
mod payload;
#[path = "dtk_exec/process.rs"]
mod process;
#[path = "dtk_exec/runner.rs"]
mod runner;
#[path = "dtk_exec/shell.rs"]
mod shell;

use std::process::ExitCode;

fn main() -> ExitCode {
    runner::run_exec_command()
}
