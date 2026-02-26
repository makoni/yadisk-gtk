use std::process::Command;

use anyhow::{Context, Result};

const USER_SERVICE_NAME: &str = "yadiskd.service";

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
    let status = Command::new("systemctl")
        .args(["--user", action.as_systemctl_action(), USER_SERVICE_NAME])
        .status()
        .with_context(|| {
            format!(
                "failed to run systemctl --user {} {}",
                action.as_systemctl_action(),
                USER_SERVICE_NAME
            )
        })?;
    if status.success() {
        return Ok(());
    }
    anyhow::bail!(
        "systemctl --user {} {} failed with status {status}",
        action.as_systemctl_action(),
        USER_SERVICE_NAME
    );
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
}
