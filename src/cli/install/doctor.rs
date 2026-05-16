use std::fs;
use std::io;
use std::path::PathBuf;

use dtk::{claude_dir, codex_dir, cursor_dir, AgentTarget};

#[derive(Debug, Clone)]
pub(super) struct DoctorCheck {
    pub(super) label: String,
    pub(super) ok: bool,
    pub(super) detail: String,
    pub(super) required: bool,
}

pub(super) fn doctor_checks(
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

pub(super) fn detect_installed_selection() -> [bool; 3] {
    [
        codex_artifacts_present(),
        claude_artifacts_present(),
        cursor_artifacts_present(),
    ]
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
