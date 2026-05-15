mod doctor;
mod interactive;

use std::io::{self, IsTerminal, Write};
use std::process::ExitCode;

use dtk::{
    install_agent_guidance, install_agent_guidance_with_dummy_samples, runtime_store_dir,
    runtime_usage_dir, uninstall_agent_guidance, AgentTarget,
};

use doctor::doctor_checks;
use interactive::{
    maybe_install_skill_interactive, select_target_interactive, suggested_uninstall_target,
};

pub fn maybe_select_target(
    command: &str,
    explicit_target_supplied: bool,
    detected: AgentTarget,
) -> Option<AgentTarget> {
    if explicit_target_supplied || !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return None;
    }

    match command {
        "install" | "install-dummy" => select_target_interactive(detected, "install"),
        "uninstall" => select_target_interactive(suggested_uninstall_target(), "uninstall"),
        _ => None,
    }
}

pub fn run_command(command: &str, target: AgentTarget) -> Option<ExitCode> {
    let (label, op, fail_prefix, should_prompt_skill) = match command {
        "install" => (
            "Installing DTK integration",
            install_agent_guidance as fn(AgentTarget) -> io::Result<dtk::AgentInstallReport>,
            "install failed",
            true,
        ),
        "install-dummy" => (
            "Installing DTK integration and dummy samples",
            install_agent_guidance_with_dummy_samples
                as fn(AgentTarget) -> io::Result<dtk::AgentInstallReport>,
            "install failed",
            true,
        ),
        "uninstall" => (
            "Removing DTK integration",
            uninstall_agent_guidance as fn(AgentTarget) -> io::Result<dtk::AgentInstallReport>,
            "uninstall failed",
            false,
        ),
        _ => return None,
    };

    let (exit, changed) = run_agent_steps(label, target, op, fail_prefix);
    if should_prompt_skill && exit == ExitCode::from(0) {
        maybe_install_skill_interactive(target);
    }

    match command {
        "uninstall" => {
            if changed {
                eprintln!("Uninstall complete for {}", target.as_str());
            } else {
                eprintln!("Nothing to remove for {}", target.as_str());
            }
        }
        _ => {
            if changed {
                eprintln!("Install complete for {}", target.as_str());
            } else {
                eprintln!("Already up to date for {}", target.as_str());
            }
        }
    }

    Some(exit)
}

pub fn run_doctor(target: AgentTarget) -> ExitCode {
    let store_dir = runtime_store_dir();
    let usage_dir = runtime_usage_dir();

    println!("DTK doctor");
    println!("  version: v{}", env!("CARGO_PKG_VERSION"));
    println!("  detected agent: {}", target.as_str());
    println!("  store dir: {}", store_dir.display());
    println!("  usage dir: {}", usage_dir.display());

    let checks = doctor_checks(target, &store_dir, &usage_dir);
    let mut failed = false;

    for check in checks {
        let status = if check.ok {
            "ok"
        } else if check.required {
            "missing"
        } else {
            "warn"
        };
        let note = if check.required { "" } else { " (optional)" };
        println!("  [{status}] {}{}{}", check.label, note, check.detail);
        if check.required && !check.ok {
            failed = true;
        }
    }

    if failed {
        ExitCode::from(1)
    } else {
        ExitCode::from(0)
    }
}

fn run_agent_steps<F>(
    label: &str,
    target: AgentTarget,
    op: F,
    fail_prefix: &str,
) -> (ExitCode, bool)
where
    F: Fn(AgentTarget) -> Result<dtk::AgentInstallReport, std::io::Error>,
{
    let agents = expand_target(target);
    let total = agents.len();
    let mut any_changed = false;
    for (idx, agent) in agents.iter().enumerate() {
        let width = 24usize;
        let progress = ((idx * 100) / total).min(99);
        let filled = (progress * width) / 100;
        eprint!(
            "\r\x1b[2K[{}{}] {:>3}% {} ({}/{})",
            "=".repeat(filled),
            " ".repeat(width.saturating_sub(filled)),
            progress,
            label,
            idx + 1,
            total
        );
        let _ = io::stderr().flush();

        match op(*agent) {
            Ok(report) => {
                any_changed |= report.changed;
            }
            Err(err) => {
                eprint!("\r\x1b[2K");
                eprintln!("{fail_prefix}: {err}");
                return (ExitCode::from(1), any_changed);
            }
        }
    }
    eprint!("\r\x1b[2K[{}] 100% {}\n", "=".repeat(24), label);
    let _ = io::stderr().flush();
    (ExitCode::from(0), any_changed)
}

fn expand_target(target: AgentTarget) -> Vec<AgentTarget> {
    match target {
        AgentTarget::All => vec![AgentTarget::Codex, AgentTarget::Claude, AgentTarget::Cursor],
        other => vec![other],
    }
}
