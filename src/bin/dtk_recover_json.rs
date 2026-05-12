use std::process::ExitCode;

use dtk::{recover_original_payload, runtime_store_dir};

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(ref_id) = args.next() else {
        eprintln!("usage: dtk_recover_json <ref_id>");
        return ExitCode::from(2);
    };

    let store_dir = runtime_store_dir();
    match recover_original_payload(&ref_id, &store_dir) {
        Ok(payload) => {
            println!("{payload}");
            ExitCode::from(0)
        }
        Err(err) => {
            eprintln!("failed to recover payload for {ref_id}: {err}");
            ExitCode::from(1)
        }
    }
}
