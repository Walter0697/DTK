use std::io::{self, IsTerminal, Write};
use std::process::ExitCode;

use dialoguer::{theme::ColorfulTheme, MultiSelect, Select};
use dtk::{install_config_skill, AgentTarget};

use super::doctor::detect_installed_selection;

pub(super) fn maybe_install_skill_interactive(target: AgentTarget) {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return;
    }

    if !prompt_skill_install(target) {
        eprintln!("Skipped skill installation.");
        return;
    }

    let exit = run_skill_steps(target);
    if exit == ExitCode::from(0) {
        eprintln!("Skill install complete for {}", target.as_str());
    } else {
        eprintln!("DTK integration installed, but skill install did not complete.");
    }
}

pub(super) fn select_target_interactive(
    detected: AgentTarget,
    action: &str,
) -> Option<AgentTarget> {
    let mut selected = [false, false, false];
    apply_detected_selection(detected, &mut selected);

    let labels = vec![
        format!("Auto-detect (apply: {})", detected.as_str()),
        "codex".to_string(),
        "claude".to_string(),
        "cursor".to_string(),
    ];
    let defaults = vec![false, selected[0], selected[1], selected[2]];
    let prompt = format!(
        "Select coding agents for DTK {} (space=toggle, enter=confirm)",
        action
    );

    let choices = MultiSelect::with_theme(&ColorfulTheme::default())
        .with_prompt(prompt)
        .items(&labels)
        .defaults(&defaults)
        .report(false)
        .interact_opt();

    match choices {
        Ok(Some(indices)) => {
            let mut resolved = [false, false, false];
            let mut apply_auto = false;
            for idx in indices {
                if idx == 0 {
                    apply_auto = true;
                } else if idx <= 3 {
                    resolved[idx - 1] = true;
                }
            }
            if apply_auto {
                apply_detected_selection(detected, &mut resolved);
            }
            Some(selection_to_target(&resolved, detected))
        }
        Ok(None) | Err(_) => Some(detected),
    }
}

pub(super) fn selection_to_target(selected: &[bool; 3], detected: AgentTarget) -> AgentTarget {
    let count = selected.iter().filter(|value| **value).count();
    if count == 0 {
        return detected;
    }
    if count > 1 {
        return AgentTarget::All;
    }
    if selected[0] {
        AgentTarget::Codex
    } else if selected[1] {
        AgentTarget::Claude
    } else {
        AgentTarget::Cursor
    }
}

fn prompt_skill_install(target: AgentTarget) -> bool {
    loop {
        eprintln!();
        let selection = Select::with_theme(&ColorfulTheme::default())
            .with_prompt(format!(
                "Install DTK configuration skills for {}",
                target.as_str()
            ))
            .items(["Yes", "No", "What is it?"])
            .default(0)
            .interact();

        let Ok(selection) = selection else {
            eprintln!("Skipping skill install (failed to read input).");
            return false;
        };

        match selection {
            0 => return true,
            1 => return false,
            _ => explain_skill(target),
        }
    }
}

fn explain_skill(target: AgentTarget) {
    eprintln!();
    eprintln!(
        "DTK installs one optional skill to create configs from live payloads; after that, use native DTK config commands to tune the allowlist or delete the config."
    );
    eprintln!(
        "It runs the source, inspects the output, helps decide what fields matter, and drafts the reusable config."
    );
    eprintln!(
        "For {}, it is optional guidance for interactive payload filtering setup.",
        target.as_str()
    );
}

fn run_skill_steps(target: AgentTarget) -> ExitCode {
    let agents = expand_target(target);
    let total = agents.len();
    for (idx, agent) in agents.iter().enumerate() {
        let width = 24usize;
        let progress = ((idx * 100) / total).min(99);
        let filled = (progress * width) / 100;
        eprint!(
            "\r\x1b[2K[{}{}] {:>3}% Installing DTK skills ({}/{})",
            "=".repeat(filled),
            " ".repeat(width.saturating_sub(filled)),
            progress,
            idx + 1,
            total
        );
        let _ = io::stderr().flush();
        if let Err(err) = install_config_skill(*agent) {
            eprint!("\r\x1b[2K");
            eprintln!("skill install failed: {err}");
            return ExitCode::from(1);
        }
    }
    eprint!("\r\x1b[2K[{}] 100% Installing DTK skills\n", "=".repeat(24));
    let _ = io::stderr().flush();
    ExitCode::from(0)
}

fn expand_target(target: AgentTarget) -> Vec<AgentTarget> {
    match target {
        AgentTarget::All => vec![AgentTarget::Codex, AgentTarget::Claude, AgentTarget::Cursor],
        other => vec![other],
    }
}

fn apply_detected_selection(detected: AgentTarget, selected: &mut [bool; 3]) {
    *selected = [false, false, false];
    match detected {
        AgentTarget::All => *selected = [true, true, true],
        AgentTarget::Codex => selected[0] = true,
        AgentTarget::Claude => selected[1] = true,
        AgentTarget::Cursor => selected[2] = true,
    }
}

pub(super) fn suggested_uninstall_target() -> AgentTarget {
    selection_to_target(&detect_installed_selection(), AgentTarget::Codex)
}
