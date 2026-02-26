use crate::control_client::{ControlClient, ControlSnapshot};
use crate::integration_control::{IntegrationStatus, detect_integration_status};
use crate::service_control::{ServiceStatus, query_daemon_service_status};
use crate::settings::{SettingsSnapshot, read_settings_snapshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UiStatus {
    Unknown,
    Ready,
    NeedsSetup,
    Error,
}

#[derive(Debug, Clone)]
pub struct UiModel {
    pub auth_status: UiStatus,
    pub daemon_status: UiStatus,
    pub integration_status: UiStatus,
    pub auth_summary: String,
    pub daemon_summary: String,
    pub integration_summary: String,
    pub control: Option<ControlSnapshot>,
    pub service: Option<ServiceStatus>,
    pub integrations: IntegrationStatus,
    pub settings: SettingsSnapshot,
}

impl UiModel {
    pub fn collect() -> Self {
        let control = ControlClient::connect()
            .ok()
            .and_then(|dbus| dbus.get_statuses().ok());
        let service = query_daemon_service_status().ok();
        let integrations = detect_integration_status();
        let settings = read_settings_snapshot();

        let auth_status = control
            .as_ref()
            .map(|snapshot| map_control_status(snapshot.auth_state.as_str()))
            .unwrap_or(UiStatus::Unknown);
        let daemon_status = control
            .as_ref()
            .map(|snapshot| map_control_status(snapshot.daemon_state.as_str()))
            .or_else(|| service.as_ref().map(map_service_status))
            .unwrap_or(UiStatus::Unknown);
        let integration_status = map_integration_status(&integrations);

        let auth_summary = control
            .as_ref()
            .map(|snapshot| format!("{}: {}", snapshot.auth_state, snapshot.auth_message))
            .unwrap_or_else(|| "unknown: control API is unavailable".to_string());
        let daemon_summary = if let Some(snapshot) = &control {
            format!("{}: {}", snapshot.daemon_state, snapshot.daemon_message)
        } else if let Some(service) = &service {
            format!("{}: user service state", service.state)
        } else {
            "unknown: service status is unavailable".to_string()
        };
        let integration_summary = integrations.summary_message();

        Self {
            auth_status,
            daemon_status,
            integration_status,
            auth_summary,
            daemon_summary,
            integration_summary,
            control,
            service,
            integrations,
            settings,
        }
    }
}

fn map_control_status(status: &str) -> UiStatus {
    match status {
        "authorized" | "running" | "ok" => UiStatus::Ready,
        "unauthorized" | "pending" | "needs_setup" => UiStatus::NeedsSetup,
        "error" => UiStatus::Error,
        _ => UiStatus::Unknown,
    }
}

fn map_service_status(service_status: &ServiceStatus) -> UiStatus {
    match service_status.normalized() {
        "active" => UiStatus::Ready,
        "activating" | "reloading" => UiStatus::NeedsSetup,
        "failed" => UiStatus::Error,
        _ => UiStatus::Unknown,
    }
}

fn map_integration_status(integration_status: &IntegrationStatus) -> UiStatus {
    match integration_status.summary_state() {
        "ok" => UiStatus::Ready,
        "needs_setup" => UiStatus::NeedsSetup,
        _ => UiStatus::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_control_status_values() {
        assert_eq!(map_control_status("authorized"), UiStatus::Ready);
        assert_eq!(map_control_status("pending"), UiStatus::NeedsSetup);
        assert_eq!(map_control_status("error"), UiStatus::Error);
        assert_eq!(map_control_status("anything-else"), UiStatus::Unknown);
    }

    #[test]
    fn maps_service_status_values() {
        assert_eq!(
            map_service_status(&ServiceStatus {
                state: "active".to_string()
            }),
            UiStatus::Ready
        );
        assert_eq!(
            map_service_status(&ServiceStatus {
                state: "failed".to_string()
            }),
            UiStatus::Error
        );
    }

    #[test]
    fn maps_integration_status_values() {
        assert_eq!(
            map_integration_status(&IntegrationStatus {
                nautilus_extension_installed: true,
                fuse_helper_installed: true,
                emblems_installed: true,
            }),
            UiStatus::Ready
        );
        assert_eq!(
            map_integration_status(&IntegrationStatus {
                nautilus_extension_installed: false,
                fuse_helper_installed: true,
                emblems_installed: true,
            }),
            UiStatus::NeedsSetup
        );
    }
}
