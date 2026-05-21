use std::io::{self, IsTerminal, Write};
use std::process::ExitCode;

use dialoguer::{theme::ColorfulTheme, MultiSelect, Select};
use dtk::{install_config_skill, AgentTarget};

use super::doctor::detect_installed_selection;

const INTERACTIVE_TARGETS: [AgentTarget; 11] = [
    AgentTarget::Codex,
    AgentTarget::Claude,
    AgentTarget::Cursor,
    AgentTarget::Copilot,
    AgentTarget::Gemini,
    AgentTarget::Windsurf,
    AgentTarget::Cline,
    AgentTarget::KiloCode,
    AgentTarget::Antigravity,
    AgentTarget::OpenCode,
    AgentTarget::Hermes,
];

pub(super) fn maybe_install_skill_interactive(target: AgentTarget) {
    if !matches!(
        target,
        AgentTarget::All
            | AgentTarget::Codex
            | AgentTarget::Claude
            | AgentTarget::Cursor
            | AgentTarget::Gemini
    ) {
        return;
    }

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
    let mut selected = [false; INTERACTIVE_TARGETS.len()];
    apply_detected_selection(detected, &mut selected);

    let mut labels = vec![format!("Auto-detect (apply: {})", detected.as_str())];
    labels.extend(
        INTERACTIVE_TARGETS
            .iter()
            .map(|target| target.as_str().to_string()),
    );
    let mut defaults = vec![false];
    defaults.extend(selected.iter().copied());
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
            let mut resolved = [false; INTERACTIVE_TARGETS.len()];
            let mut apply_auto = false;
            for idx in indices {
                if idx == 0 {
                    apply_auto = true;
                } else if idx - 1 < resolved.len() {
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

pub(super) fn selection_to_target(selected: &[bool], detected: AgentTarget) -> AgentTarget {
    let mut chosen: Option<AgentTarget> = None;
    for (idx, is_selected) in selected.iter().enumerate() {
        if !is_selected {
            continue;
        }
        let target = INTERACTIVE_TARGETS[idx];
        if chosen.replace(target).is_some() {
            return AgentTarget::All;
        }
    }

    chosen.unwrap_or(detected)
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
        AgentTarget::All => vec![
            AgentTarget::Codex,
            AgentTarget::Claude,
            AgentTarget::Cursor,
            AgentTarget::Gemini,
        ],
        other => vec![other],
    }
}

fn apply_detected_selection(detected: AgentTarget, selected: &mut [bool]) {
    selected.fill(false);
    if detected == AgentTarget::All {
        selected.fill(true);
        return;
    }

    if let Some(index) = INTERACTIVE_TARGETS
        .iter()
        .position(|candidate| *candidate == detected)
    {
        selected[index] = true;
    }
}

pub(super) fn suggested_uninstall_target() -> AgentTarget {
    selection_to_target(&detect_installed_selection(), AgentTarget::Codex)
}
