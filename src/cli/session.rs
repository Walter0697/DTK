use super::*;

pub(super) fn run_session_command(args: Vec<String>) -> ExitCode {
    let mut iter = args.into_iter();
    let Some(subcommand) = iter.next() else {
        print_session_usage();
        return ExitCode::from(2);
    };

    let mut ticket_id: Option<String> = None;
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--ticket-id" | "--ticketId" => {
                let Some(value) = iter.next() else {
                    eprintln!("missing value for --ticket-id");
                    return ExitCode::from(2);
                };
                if value.trim().is_empty() {
                    eprintln!("invalid ticketId: {value}");
                    return ExitCode::from(2);
                }
                ticket_id = Some(value);
            }
            "--help" | "-h" => {
                print_session_usage();
                return ExitCode::from(0);
            }
            other => {
                eprintln!("unknown session argument: {other}");
                print_session_usage();
                return ExitCode::from(2);
            }
        }
    }

    let store_dir = runtime_store_dir();
    match subcommand.as_str() {
        "start" => match start_session(&store_dir, ticket_id) {
            Ok(session) => {
                println!(
                    "session started: ticketId={} session={}",
                    session.ticket_id, session.id
                );
                ExitCode::from(0)
            }
            Err(err) => {
                eprintln!("failed to start session: {err}");
                ExitCode::from(1)
            }
        },
        "end" => match end_session(&store_dir) {
            Ok(session) => {
                println!(
                    "session ended: ticketId={} session={}",
                    session.ticket_id, session.id
                );
                ExitCode::from(0)
            }
            Err(err) => {
                eprintln!("failed to end session: {err}");
                ExitCode::from(1)
            }
        },
        "--help" | "-h" | "help" => {
            print_session_usage();
            ExitCode::from(0)
        }
        other => {
            eprintln!("unknown session subcommand: {other}");
            print_session_usage();
            ExitCode::from(2)
        }
    }
}

fn print_session_usage() {
    eprintln!("Usage: dtk session <start|end> [--ticket-id ID|--ticketId ID]");
    eprintln!("Session commands:");
    eprintln!("  dtk session start");
    eprintln!("  dtk session start --ticket-id abc123");
    eprintln!("  dtk session start --ticketId abc123");
    eprintln!("  dtk session end");
    eprintln!("  Records the active ticketId on metrics while the session is open.");
}
