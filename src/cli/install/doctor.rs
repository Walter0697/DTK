use std::fs;
use std::io;
use std::path::PathBuf;

use dtk::{claude_dir, codex_dir, cursor_dir, gemini_dir, AgentTarget};

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
            checks.extend(agent_doctor_checks(AgentTarget::Copilot));
            checks.extend(agent_doctor_checks(AgentTarget::Gemini));
            checks.extend(agent_doctor_checks(AgentTarget::Windsurf));
            checks.extend(agent_doctor_checks(AgentTarget::Cline));
            checks.extend(agent_doctor_checks(AgentTarget::KiloCode));
            checks.extend(agent_doctor_checks(AgentTarget::Antigravity));
            checks.extend(agent_doctor_checks(AgentTarget::OpenCode));
            checks.extend(agent_doctor_checks(AgentTarget::Hermes));
        }
        AgentTarget::Codex
        | AgentTarget::Claude
        | AgentTarget::Cursor
        | AgentTarget::Copilot
        | AgentTarget::Gemini
        | AgentTarget::Windsurf
        | AgentTarget::Cline
        | AgentTarget::KiloCode
        | AgentTarget::Antigravity
        | AgentTarget::OpenCode
        | AgentTarget::Hermes => {
            checks.extend(agent_doctor_checks(target));
        }
    }

    checks.push(check_store_writable(store_dir));
    checks.push(check_usage_writable(usage_dir));
    checks
}

pub(super) fn detect_installed_selection() -> [bool; 11] {
    [
        codex_artifacts_present(),
        claude_artifacts_present(),
        cursor_artifacts_present(),
        copilot_artifacts_present(),
        gemini_artifacts_present(),
        windsurf_artifacts_present(),
        cline_artifacts_present(),
        kilocode_artifacts_present(),
        antigravity_artifacts_present(),
        opencode_artifacts_present(),
        hermes_artifacts_present(),
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
            let settings = claude_dir().join("settings.json");
            checks.push(file_check(&guide, true));
            checks.push(text_contains_check(&guide, "DTK Config Assistant", true));
            checks.push(file_check(&skill, false));
            checks.push(file_check(&claude_md, true));
            checks.push(text_contains_check(&claude_md, "@DTK.md", true));
            checks.push(file_check(&settings, true));
            checks.push(text_contains_check(
                &settings,
                "dtk_hook_route --provider claude",
                true,
            ));
        }
        AgentTarget::Cursor => {
            let guide = cursor_dir().join("DTK.md");
            let skill = cursor_dir().join("skills").join("dtk").join("SKILL.md");
            let hooks = cursor_dir().join("hooks.json");
            checks.push(file_check(&guide, true));
            checks.push(text_contains_check(&guide, "DTK Config Assistant", true));
            checks.push(file_check(&skill, false));
            checks.push(file_check(&hooks, true));
            checks.push(text_contains_check(
                &hooks,
                "dtk_hook_route --provider cursor",
                true,
            ));
        }
        AgentTarget::Copilot => {
            let hooks = std::path::PathBuf::from(".github")
                .join("hooks")
                .join("dtk-rewrite.json");
            let instructions = std::path::PathBuf::from(".github").join("copilot-instructions.md");
            checks.push(file_check(&hooks, true));
            checks.push(text_contains_check(
                &hooks,
                "dtk_hook_route --provider copilot",
                true,
            ));
            checks.push(file_check(&instructions, true));
            checks.push(text_contains_check(&instructions, "dtk exec", true));
        }
        AgentTarget::Gemini => {
            let base = gemini_dir();
            let guide = base.join("DTK.md");
            let skill = base.join("skills").join("dtk").join("SKILL.md");
            let settings = base.join("settings.json");
            checks.push(file_check(&guide, true));
            checks.push(text_contains_check(&guide, "DTK Config Assistant", true));
            checks.push(file_check(&skill, false));
            checks.push(file_check(&settings, true));
            checks.push(text_contains_check(
                &settings,
                "dtk_hook_route --provider gemini",
                true,
            ));
        }
        AgentTarget::Windsurf => {
            let rules = PathBuf::from(".windsurfrules");
            checks.push(file_check(&rules, true));
            checks.push(text_contains_check(&rules, "dtk exec", true));
        }
        AgentTarget::Cline => {
            let rules = PathBuf::from(".clinerules");
            checks.push(file_check(&rules, true));
            checks.push(text_contains_check(&rules, "dtk exec", true));
        }
        AgentTarget::KiloCode => {
            let rules = PathBuf::from(".kilocode")
                .join("rules")
                .join("rtk-rules.md");
            checks.push(file_check(&rules, true));
            checks.push(text_contains_check(&rules, "dtk exec", true));
        }
        AgentTarget::Antigravity => {
            let rules = PathBuf::from(".agents")
                .join("rules")
                .join("antigravity-rtk-rules.md");
            checks.push(file_check(&rules, true));
            checks.push(text_contains_check(&rules, "dtk exec", true));
        }
        AgentTarget::OpenCode => {
            let plugin = PathBuf::from(".config")
                .join("opencode")
                .join("plugins")
                .join("dtk.ts");
            checks.push(file_check(&plugin, true));
            checks.push(text_contains_check(&plugin, "dtk exec --", true));
        }
        AgentTarget::Hermes => {
            let base = hermes_home();
            let plugin = base.join("plugins").join("dtk-rewrite");
            let init = plugin.join("__init__.py");
            let manifest = plugin.join("plugin.yaml");
            let config = base.join("config.yaml");
            checks.push(file_check(&init, true));
            checks.push(file_check(&manifest, true));
            checks.push(text_contains_check(&init, "dtk exec --", true));
            checks.push(file_check(&config, true));
            checks.push(text_contains_check(&config, "dtk-rewrite", true));
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
        return content.contains("dtk_hook_route --provider claude")
            || content.contains("dtk-rewrite.sh");
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
        return content.contains("dtk_hook_route --provider cursor")
            || content.contains("dtk-rewrite.sh");
    }
    false
}

fn gemini_artifacts_present() -> bool {
    let base = gemini_dir();
    if base.join("DTK.md").exists() || base.join("skills").join("dtk").join("SKILL.md").exists() {
        return true;
    }
    let settings = base.join("settings.json");
    if let Ok(content) = fs::read_to_string(settings) {
        return content.contains("dtk_hook_route --provider gemini")
            || content.contains("dtk-rewrite.sh");
    }
    false
}

fn copilot_artifacts_present() -> bool {
    let hooks = std::path::PathBuf::from(".github")
        .join("hooks")
        .join("dtk-rewrite.json");
    let instructions = std::path::PathBuf::from(".github").join("copilot-instructions.md");
    if hooks.exists() || instructions.exists() {
        return true;
    }
    if let Ok(content) = fs::read_to_string(&hooks) {
        if content.contains("dtk_hook_route --provider copilot") {
            return true;
        }
    }
    if let Ok(content) = fs::read_to_string(&instructions) {
        if content.contains("dtk exec") {
            return true;
        }
    }
    false
}

fn windsurf_artifacts_present() -> bool {
    PathBuf::from(".windsurfrules").exists()
}

fn cline_artifacts_present() -> bool {
    PathBuf::from(".clinerules").exists()
}

fn kilocode_artifacts_present() -> bool {
    PathBuf::from(".kilocode")
        .join("rules")
        .join("rtk-rules.md")
        .exists()
}

fn antigravity_artifacts_present() -> bool {
    PathBuf::from(".agents")
        .join("rules")
        .join("antigravity-rtk-rules.md")
        .exists()
}

fn opencode_artifacts_present() -> bool {
    PathBuf::from(".config")
        .join("opencode")
        .join("plugins")
        .join("dtk.ts")
        .exists()
}

fn hermes_artifacts_present() -> bool {
    let base = hermes_home();
    let plugin = base.join("plugins").join("dtk-rewrite");
    plugin.join("__init__.py").exists()
        || plugin.join("plugin.yaml").exists()
        || base.join("config.yaml").exists()
}

fn hermes_home() -> PathBuf {
    std::env::var("DTK_HERMES_DIR")
        .map(PathBuf::from)
        .or_else(|_| std::env::var("HERMES_HOME").map(PathBuf::from))
        .unwrap_or_else(|_| {
            std::env::var("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(".hermes")
        })
}
