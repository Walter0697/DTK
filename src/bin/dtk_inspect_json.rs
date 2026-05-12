use std::io::{self, IsTerminal, Read};
use std::process::ExitCode;

use dtk::{collect_field_paths, parse_json_payload};

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let input = if let Some(text) = args.next() {
        text
    } else {
        if io::stdin().is_terminal() {
            eprintln!("provide JSON as an argument or pipe data on stdin");
            return ExitCode::from(2);
        }

        let mut buffer = String::new();
        if let Err(err) = io::stdin().read_to_string(&mut buffer) {
            eprintln!("failed to read stdin: {err}");
            return ExitCode::from(1);
        }
        buffer
    };

    let Some(value) = parse_json_payload(&input) else {
        eprintln!("input is not a structured JSON object or array");
        return ExitCode::from(1);
    };

    for path in collect_field_paths(&value) {
        println!("{path}");
    }

    ExitCode::from(0)
}
