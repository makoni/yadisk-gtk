use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use anyhow::{Context, Result};

const USER_SERVICE_NAME: &str = "yadiskd.service";
const USER_SERVICE_DIR: &str = "systemd/user";
const SERVICE_TEMPLATE_REL: &str = "packaging/systemd/yadiskd.service";
const LOCAL_BIN_NAME: &str = "yadiskd";
const DAEMON_BIN_ENV: &str = "YADISK_DAEMON_BIN";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceAction {
    Start,
    Stop,
    Restart,
    EnableAutostart,
    DisableAutostart,
}

impl ServiceAction {
    fn as_systemctl_action(self) -> &'static str {
        match self {
            ServiceAction::Start => "start",
            ServiceAction::Stop => "stop",
            ServiceAction::Restart => "restart",
            ServiceAction::EnableAutostart => "enable",
            ServiceAction::DisableAutostart => "disable",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceStatus {
    pub state: String,
}

impl ServiceStatus {
    pub fn normalized(&self) -> &str {
        self.state.as_str()
    }
}

pub fn query_daemon_service_status() -> Result<ServiceStatus> {
    let output = Command::new("systemctl")
        .args(["--user", "is-active", USER_SERVICE_NAME])
        .output()
        .context("failed to run systemctl --user is-active")?;
    let state = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if state.is_empty() {
        return Ok(ServiceStatus {
            state: "unknown".to_string(),
        });
    }
    Ok(ServiceStatus { state })
}

pub fn run_service_action(action: ServiceAction) -> Result<()> {
    sync_daemon_binary_for_action(action)?;
    let output = run_systemctl_action(action)?;
    if output.status.success() {
        return Ok(());
    }

    let details = command_output(&output);
    if is_unit_not_found(&details) {
        bootstrap_user_service()?;
        let retry = run_systemctl_action(action)?;
        if retry.status.success() {
            return Ok(());
        }
        anyhow::bail!(
            "systemctl --user {} {} failed after bootstrap: {}",
            action.as_systemctl_action(),
            USER_SERVICE_NAME,
            command_output(&retry)
        );
    }

    anyhow::bail!(
        "systemctl --user {} {} failed: {}",
        action.as_systemctl_action(),
        USER_SERVICE_NAME,
        details
    );
}

fn sync_daemon_binary_for_action(action: ServiceAction) -> Result<()> {
    if !matches!(
        action,
        ServiceAction::Start | ServiceAction::Restart | ServiceAction::EnableAutostart
    ) {
        return Ok(());
    }
    if let Ok(path) = std::env::var(DAEMON_BIN_ENV) {
        let candidate = PathBuf::from(path);
        if candidate.is_file() {
            return Ok(());
        }
        anyhow::bail!("{DAEMON_BIN_ENV} points to a missing file");
    }
    let local_bin = home_local_bin_path()?;
    if let Some(candidate) = find_repo_daemon_binary() {
        install_binary(&candidate, &local_bin)?;
        return Ok(());
    }
    if local_bin.is_file() {
        return Ok(());
    }

    let build_status = Command::new("cargo")
        .current_dir(repo_root())
        .args(["build", "-p", "yadiskd"])
        .status()
        .context("failed to run cargo build -p yadiskd")?;
    if !build_status.success() {
        anyhow::bail!("cargo build -p yadiskd failed with status {build_status}");
    }
    if let Some(candidate) = find_repo_daemon_binary() {
        install_binary(&candidate, &local_bin)?;
        return Ok(());
    }
    anyhow::bail!("failed to locate built yadiskd binary")
}

fn run_systemctl_action(action: ServiceAction) -> Result<Output> {
    Command::new("systemctl")
        .args(["--user", action.as_systemctl_action(), USER_SERVICE_NAME])
        .output()
        .with_context(|| {
            format!(
                "failed to run systemctl --user {} {}",
                action.as_systemctl_action(),
                USER_SERVICE_NAME
            )
        })
}

fn bootstrap_user_service() -> Result<()> {
    let binary = ensure_daemon_binary()?;
    let unit_dir = dirs::config_dir()
        .map(|base| base.join(USER_SERVICE_DIR))
        .ok_or_else(|| anyhow::anyhow!("failed to resolve user config directory"))?;
    std::fs::create_dir_all(&unit_dir)
        .with_context(|| format!("failed to create {}", unit_dir.display()))?;
    let unit_path = unit_dir.join(USER_SERVICE_NAME);

    let template_path = repo_root().join(SERVICE_TEMPLATE_REL);
    let unit_template = std::fs::read_to_string(&template_path)
        .with_context(|| format!("failed to read {}", template_path.display()))?;
    let unit_content = rewrite_exec_start(&unit_template, &binary);
    std::fs::write(&unit_path, unit_content)
        .with_context(|| format!("failed to write {}", unit_path.display()))?;

    let reload = Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .output()
        .context("failed to run systemctl --user daemon-reload")?;
    if !reload.status.success() {
        anyhow::bail!(
            "systemctl --user daemon-reload failed: {}",
            command_output(&reload)
        );
    }
    Ok(())
}

fn ensure_daemon_binary() -> Result<PathBuf> {
    if let Ok(path) = std::env::var(DAEMON_BIN_ENV) {
        let candidate = PathBuf::from(path);
        if candidate.is_file() {
            return Ok(candidate);
        }
        anyhow::bail!("{DAEMON_BIN_ENV} points to a missing file");
    }

    let local_bin = home_local_bin_path()?;
    if local_bin.is_file() {
        return Ok(local_bin);
    }

    if let Some(candidate) = find_repo_daemon_binary() {
        install_binary(&candidate, &local_bin)?;
        return Ok(local_bin);
    }

    let build_status = Command::new("cargo")
        .current_dir(repo_root())
        .args(["build", "-p", "yadiskd"])
        .status()
        .context("failed to run cargo build -p yadiskd")?;
    if !build_status.success() {
        anyhow::bail!("cargo build -p yadiskd failed with status {build_status}");
    }

    if let Some(candidate) = find_repo_daemon_binary() {
        install_binary(&candidate, &local_bin)?;
        return Ok(local_bin);
    }

    anyhow::bail!("failed to locate built yadiskd binary")
}

fn find_repo_daemon_binary() -> Option<PathBuf> {
    let root = repo_root();
    [
        root.join("target/debug/yadiskd"),
        root.join("target/release/yadiskd"),
    ]
    .into_iter()
    .find(|path| path.is_file())
}

fn install_binary(source: &Path, destination: &Path) -> Result<()> {
    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::copy(source, destination).with_context(|| {
        format!(
            "failed to copy daemon binary from {} to {}",
            source.display(),
            destination.display()
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(destination, std::fs::Permissions::from_mode(0o755))
            .with_context(|| format!("failed to chmod {}", destination.display()))?;
    }
    Ok(())
}

fn rewrite_exec_start(template: &str, binary: &Path) -> String {
    let mut replaced = false;
    let mut lines = Vec::new();
    for line in template.lines() {
        if line.trim_start().starts_with("ExecStart=") {
            lines.push(format!("ExecStart={}", binary.display()));
            replaced = true;
        } else {
            lines.push(line.to_string());
        }
    }
    if !replaced {
        lines.push(format!("ExecStart={}", binary.display()));
    }
    lines.join("\n") + "\n"
}

fn home_local_bin_path() -> Result<PathBuf> {
    let home =
        dirs::home_dir().ok_or_else(|| anyhow::anyhow!("failed to resolve home directory"))?;
    Ok(home.join(".local/bin").join(LOCAL_BIN_NAME))
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root exists")
        .to_path_buf()
}

fn is_unit_not_found(output: &str) -> bool {
    output.contains("Unit yadiskd.service not found")
        || output.contains("unit yadiskd.service not found")
}

fn command_output(output: &Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    match (stdout.is_empty(), stderr.is_empty()) {
        (true, true) => format!("exit status {}", output.status),
        (false, true) => stdout,
        (true, false) => stderr,
        (false, false) => format!("{stderr}; {stdout}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_to_systemctl_name_matches_expected_values() {
        assert_eq!(ServiceAction::Start.as_systemctl_action(), "start");
        assert_eq!(ServiceAction::Stop.as_systemctl_action(), "stop");
        assert_eq!(ServiceAction::Restart.as_systemctl_action(), "restart");
        assert_eq!(
            ServiceAction::EnableAutostart.as_systemctl_action(),
            "enable"
        );
        assert_eq!(
            ServiceAction::DisableAutostart.as_systemctl_action(),
            "disable"
        );
    }

    #[test]
    fn rewrite_exec_start_replaces_existing_line() {
        let template = "[Service]\nExecStart=%h/.local/bin/yadiskd\nRestart=on-failure\n";
        let rewritten = rewrite_exec_start(template, Path::new("/tmp/yadiskd"));
        assert!(rewritten.contains("ExecStart=/tmp/yadiskd"));
        assert!(!rewritten.contains("%h/.local/bin/yadiskd"));
    }

    #[test]
    fn detects_missing_unit_message() {
        assert!(is_unit_not_found(
            "Failed to start yadiskd.service: Unit yadiskd.service not found."
        ));
    }
}
