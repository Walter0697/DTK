use std::io::{self, IsTerminal, Read};
use std::process::ExitCode;

use dtk::is_json_payload;

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let input = if let Some(text) = args.next() {
        text
    } else {
        if io::stdin().is_terminal() {
            eprintln!("provide text as an argument or pipe data on stdin");
            return ExitCode::from(2);
        }

        let mut buffer = String::new();
        if let Err(err) = io::stdin().read_to_string(&mut buffer) {
            eprintln!("failed to read stdin: {err}");
            return ExitCode::from(1);
        }
        buffer
    };

    if is_json_payload(&input) {
        println!("json");
        ExitCode::from(0)
    } else {
        println!("not-json");
        ExitCode::from(1)
    }
}
