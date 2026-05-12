use std::process::ExitCode;

use dtk::{cleanup_expired_payloads, preview_expired_payloads, runtime_store_dir};

fn main() -> ExitCode {
    let dry_run = std::env::args().any(|arg| arg == "--dry-run");
    let store_dir = runtime_store_dir();
    if dry_run {
        match preview_expired_payloads(&store_dir) {
            Ok(report) => {
                println!("expired={}", report.expired_ref_ids.len());
                for ref_id in report.expired_ref_ids {
                    println!("{ref_id}");
                }
                ExitCode::from(0)
            }
            Err(err) => {
                eprintln!("failed to preview DTK cleanup: {err}");
                ExitCode::from(1)
            }
        }
    } else {
        match cleanup_expired_payloads(&store_dir) {
            Ok(report) => {
                println!(
                    "removed={} remaining={}",
                    report.removed_count, report.remaining_count
                );
                ExitCode::from(0)
            }
            Err(err) => {
                eprintln!("failed to clean DTK store: {err}");
                ExitCode::from(1)
            }
        }
    }
}
