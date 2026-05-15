use std::fs;
use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use dialoguer::{theme::ColorfulTheme, MultiSelect, Select};
use dtk::{
    claude_dir, codex_dir, cursor_dir, install_agent_guidance,
    install_agent_guidance_with_dummy_samples, install_config_skill, runtime_store_dir,
    runtime_usage_dir, uninstall_agent_guidance, AgentTarget,
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
        "uninstall" => select_target_interactive(
            selection_to_target(&detect_installed_selection(), AgentTarget::Codex),
            "uninstall",
        ),
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

#[derive(Debug, Clone)]
struct DoctorCheck {
    label: String,
    ok: bool,
    detail: String,
    required: bool,
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

fn doctor_checks(
    target: AgentTarget,
    store_dir: &PathBuf,
    usage_dir: &PathBuf,
) -> Vec<DoctorCheck> {
    let mut checks = Vec::new();
    match target {
        AgentTarget::All => {
            checks.extend(agent_doctor_checks(AgentTarget::Codex));
            checks.extend(agent_doctor_checks(AgentTarget::Claude));
            checks.extend(agent_doctor_checks(AgentTarget::Cursor));
        }
        AgentTarget::Codex | AgentTarget::Claude | AgentTarget::Cursor => {
            checks.extend(agent_doctor_checks(target));
        }
    }

    checks.push(check_store_writable(store_dir));
    checks.push(check_usage_writable(usage_dir));
    checks
}

fn agent_doctor_checks(target: AgentTarget) -> Vec<DoctorCheck> {
    let mut checks = Vec::new();
    match target {
        AgentTarget::Codex => {
            let guide = codex_dir().join("DTK.md");
            let skill = codex_dir().join("skills").join("dtk").join("SKILL.md");
            checks.push(file_check(&guide, true));
            checks.push(text_contains_check(&guide, "DTK Config Assistant", true));
            checks.push(file_check(&skill, false));
        }
        AgentTarget::Claude => {
            let guide = claude_dir().join("DTK.md");
            let skill = claude_dir().join("skills").join("dtk").join("SKILL.md");
            let claude_md = claude_dir().join("CLAUDE.md");
            checks.push(file_check(&guide, true));
            checks.push(text_contains_check(&guide, "DTK Config Assistant", true));
            checks.push(file_check(&skill, false));
            checks.push(file_check(&claude_md, true));
            checks.push(text_contains_check(&claude_md, "@DTK.md", true));
        }
        AgentTarget::Cursor => {
            let guide = cursor_dir().join("DTK.md");
            let skill = cursor_dir().join("skills").join("dtk").join("SKILL.md");
            checks.push(file_check(&guide, true));
            checks.push(text_contains_check(&guide, "DTK Config Assistant", true));
            checks.push(file_check(&skill, false));
        }
        AgentTarget::All => unreachable!(),
    }

    checks
}

fn file_check(path: &PathBuf, required: bool) -> DoctorCheck {
    DoctorCheck {
        label: path.display().to_string(),
        ok: path.exists(),
        detail: String::new(),
        required,
    }
}

fn text_contains_check(path: &PathBuf, needle: &str, required: bool) -> DoctorCheck {
    match fs::read_to_string(path) {
        Ok(content) => DoctorCheck {
            label: format!("{} contains {:?}", path.display(), needle),
            ok: content.contains(needle),
            detail: String::new(),
            required,
        },
        Err(err) => DoctorCheck {
            label: format!("{} contains {:?}", path.display(), needle),
            ok: false,
            detail: format!(" ({err})"),
            required,
        },
    }
}

fn check_store_writable(store_dir: &PathBuf) -> DoctorCheck {
    let test_dir = store_dir.join(".doctor");
    let test_file = test_dir.join("write-test");
    let result = (|| -> io::Result<()> {
        fs::create_dir_all(&test_dir)?;
        fs::write(&test_file, b"ok")?;
        fs::remove_file(&test_file)?;
        let _ = fs::remove_dir(&test_dir);
        Ok(())
    })();

    match result {
        Ok(()) => DoctorCheck {
            label: "store writable".to_string(),
            ok: true,
            detail: format!(" ({})", store_dir.display()),
            required: true,
        },
        Err(err) => DoctorCheck {
            label: "store writable".to_string(),
            ok: false,
            detail: format!(" ({}: {err})", store_dir.display()),
            required: true,
        },
    }
}

fn check_usage_writable(usage_dir: &PathBuf) -> DoctorCheck {
    let test_dir = usage_dir.join(".doctor");
    let test_file = test_dir.join("write-test");
    let result = (|| -> io::Result<()> {
        fs::create_dir_all(&test_dir)?;
        fs::write(&test_file, b"ok")?;
        fs::remove_file(&test_file)?;
        let _ = fs::remove_dir(&test_dir);
        Ok(())
    })();

    match result {
        Ok(()) => DoctorCheck {
            label: "usage db writable".to_string(),
            ok: true,
            detail: format!(" ({})", usage_dir.display()),
            required: true,
        },
        Err(err) => DoctorCheck {
            label: "usage db writable".to_string(),
            ok: false,
            detail: format!(" ({}: {err})", usage_dir.display()),
            required: true,
        },
    }
}

fn maybe_install_skill_interactive(target: AgentTarget) {
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

fn select_target_interactive(detected: AgentTarget, action: &str) -> Option<AgentTarget> {
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

fn apply_detected_selection(detected: AgentTarget, selected: &mut [bool; 3]) {
    *selected = [false, false, false];
    match detected {
        AgentTarget::All => *selected = [true, true, true],
        AgentTarget::Codex => selected[0] = true,
        AgentTarget::Claude => selected[1] = true,
        AgentTarget::Cursor => selected[2] = true,
    }
}

fn selection_to_target(selected: &[bool; 3], detected: AgentTarget) -> AgentTarget {
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

fn detect_installed_selection() -> [bool; 3] {
    [
        codex_artifacts_present(),
        claude_artifacts_present(),
        cursor_artifacts_present(),
    ]
}

fn codex_artifacts_present() -> bool {
    codex_dir().join("DTK.md").exists()
        || codex_dir()
            .join("skills")
            .join("dtk")
            .join("SKILL.md")
            .exists()
}

fn claude_artifacts_present() -> bool {
    let base = claude_dir();
    if base.join("DTK.md").exists()
        || base.join("hooks").join("dtk-rewrite.sh").exists()
        || base.join("skills").join("dtk").join("SKILL.md").exists()
    {
        return true;
    }
    let claude_md = base.join("CLAUDE.md");
    if let Ok(content) = fs::read_to_string(claude_md) {
        if content.lines().any(|line| line.trim() == "@DTK.md") {
            return true;
        }
    }
    let settings = base.join("settings.json");
    if let Ok(content) = fs::read_to_string(settings) {
        return content.contains("dtk-rewrite.sh");
    }
    false
}

fn cursor_artifacts_present() -> bool {
    let base = cursor_dir();
    if base.join("DTK.md").exists()
        || base.join("hooks").join("dtk-rewrite.sh").exists()
        || base.join("skills").join("dtk").join("SKILL.md").exists()
    {
        return true;
    }
    let hooks = base.join("hooks.json");
    if let Ok(content) = fs::read_to_string(hooks) {
        return content.contains("dtk-rewrite.sh");
    }
    false
}
