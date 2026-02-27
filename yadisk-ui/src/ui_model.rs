use crate::control_client::{ControlClient, ControlSnapshot};
use crate::integration_control::{IntegrationStatus, detect_integration_status};
use crate::service_control::{ServiceStatus, query_daemon_service_status};
use crate::settings::{SettingsSnapshot, read_settings_snapshot};
use keyring::Entry;
use yadisk_integrations::ids::KEYRING_SERVICE;

const TOKEN_KEY: &str = "yadisk_token";
const TOKEN_STORAGE_DIR: &str = "yadisk-gtk";
const TOKEN_PORTAL_DIR: &str = "secret-portal";
const TOKEN_FILENAME: &str = "yadisk_token.portal";
const ENABLE_SECRET_PORTAL_ENV: &str = "YADISK_ENABLE_SECRET_PORTAL";
const DISABLE_SECRET_PORTAL_ENV: &str = "YADISK_DISABLE_SECRET_PORTAL";

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

        let (auth_status, auth_summary) =
            control
                .as_ref()
                .map_or_else(detect_local_auth_state, |snapshot| {
                    (
                        map_auth_control_status(snapshot.auth_state.as_str()),
                        format!("{}: {}", snapshot.auth_state, snapshot.auth_message),
                    )
                });
        let daemon_status = control
            .as_ref()
            .map(|snapshot| map_daemon_control_status(snapshot.daemon_state.as_str()))
            .or_else(|| service.as_ref().map(map_service_status))
            .unwrap_or(UiStatus::Unknown);
        let integration_status = map_integration_status(&integrations);

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

fn map_auth_control_status(status: &str) -> UiStatus {
    match status {
        "authorized" => UiStatus::Ready,
        "unauthorized" | "pending" | "cancelled" | "needs_setup" => UiStatus::NeedsSetup,
        "error" => UiStatus::Error,
        _ => UiStatus::Unknown,
    }
}

fn map_daemon_control_status(status: &str) -> UiStatus {
    match status {
        "running" | "busy" => UiStatus::Ready,
        "starting" | "stopping" | "pending" | "inactive" | "stopped" | "needs_setup" => {
            UiStatus::NeedsSetup
        }
        "error" | "failed" => UiStatus::Error,
        _ => UiStatus::Unknown,
    }
}

fn map_service_status(service_status: &ServiceStatus) -> UiStatus {
    match service_status.normalized() {
        "active" => UiStatus::Ready,
        "activating" | "reloading" | "inactive" | "deactivating" => UiStatus::NeedsSetup,
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

fn detect_local_auth_state() -> (UiStatus, String) {
    if std::env::var("YADISK_TOKEN")
        .ok()
        .is_some_and(|token| !token.trim().is_empty())
    {
        return (
            UiStatus::Ready,
            "authorized: YADISK_TOKEN environment variable is set".to_string(),
        );
    }

    if uses_secret_portal() {
        let has_portal_token = dirs::config_dir()
            .map(|base| {
                base.join(TOKEN_STORAGE_DIR)
                    .join(TOKEN_PORTAL_DIR)
                    .join(TOKEN_FILENAME)
            })
            .is_some_and(|path| path.is_file());
        return if has_portal_token {
            (
                UiStatus::Ready,
                "authorized: saved token found in portal storage".to_string(),
            )
        } else {
            (
                UiStatus::NeedsSetup,
                "unauthorized: token is missing in portal storage".to_string(),
            )
        };
    }

    let entry = match Entry::new(KEYRING_SERVICE, TOKEN_KEY) {
        Ok(entry) => entry,
        Err(err) => {
            return (
                UiStatus::Error,
                format!("error: failed to access keyring entry ({err})"),
            );
        }
    };

    match entry.get_password() {
        Ok(_) => (
            UiStatus::Ready,
            "authorized: saved token found in keyring".to_string(),
        ),
        Err(keyring::Error::NoEntry) => (
            UiStatus::NeedsSetup,
            "unauthorized: token is missing in keyring".to_string(),
        ),
        Err(err) => (
            UiStatus::Error,
            format!("error: failed to read keyring token ({err})"),
        ),
    }
}

fn uses_secret_portal() -> bool {
    if env_flag(DISABLE_SECRET_PORTAL_ENV) {
        return false;
    }
    if env_flag(ENABLE_SECRET_PORTAL_ENV) {
        return true;
    }
    std::env::var("FLATPAK_ID").is_ok()
        || std::env::var("CONTAINER")
            .ok()
            .is_some_and(|value| value.eq_ignore_ascii_case("flatpak"))
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.as_str(),
                "1" | "true" | "TRUE" | "True" | "yes" | "YES" | "Yes" | "on" | "ON" | "On"
            )
        })
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_auth_control_status_values() {
        assert_eq!(map_auth_control_status("authorized"), UiStatus::Ready);
        assert_eq!(map_auth_control_status("pending"), UiStatus::NeedsSetup);
        assert_eq!(map_auth_control_status("cancelled"), UiStatus::NeedsSetup);
        assert_eq!(map_auth_control_status("error"), UiStatus::Error);
        assert_eq!(map_auth_control_status("anything-else"), UiStatus::Unknown);
    }

    #[test]
    fn maps_daemon_control_status_values() {
        assert_eq!(map_daemon_control_status("running"), UiStatus::Ready);
        assert_eq!(map_daemon_control_status("busy"), UiStatus::Ready);
        assert_eq!(map_daemon_control_status("inactive"), UiStatus::NeedsSetup);
        assert_eq!(map_daemon_control_status("failed"), UiStatus::Error);
        assert_eq!(
            map_daemon_control_status("anything-else"),
            UiStatus::Unknown
        );
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
