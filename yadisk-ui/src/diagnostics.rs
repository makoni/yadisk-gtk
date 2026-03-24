use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use serde::Serialize;

use crate::control_client::ControlSnapshot;
use crate::integration_control::IntegrationStatus;
use crate::service_control::ServiceStatus;
use crate::settings::SettingsSnapshot;

const SUPPORT_BUNDLE_PREFIX: &str = "yadisk-support";

#[derive(Debug, Serialize)]
pub struct DiagnosticsReport {
    pub service_state: Option<String>,
    pub dbus: Option<DbusSnapshot>,
    pub integrations: IntegrationSnapshot,
    pub settings: SettingsSnapshot,
}

#[derive(Debug, Serialize)]
pub struct DbusSnapshot {
    pub daemon_state: String,
    pub daemon_message: String,
    pub auth_state: String,
    pub auth_message: String,
    pub integration_state: String,
    pub integration_message: String,
}

#[derive(Debug, Serialize)]
pub struct IntegrationSnapshot {
    pub state: String,
    pub details: String,
    pub nautilus_extension_installed: bool,
    pub fuse_helper_installed: bool,
    pub emblems_installed: bool,
}

pub fn print_diagnostics_report(
    control: Option<&ControlSnapshot>,
    service: Option<&ServiceStatus>,
    integrations: &IntegrationStatus,
    settings: SettingsSnapshot,
) -> Result<()> {
    println!(
        "{}",
        diagnostics_report_json(control, service, integrations, settings)?
    );
    Ok(())
}

pub fn diagnostics_report_json(
    control: Option<&ControlSnapshot>,
    service: Option<&ServiceStatus>,
    integrations: &IntegrationStatus,
    settings: SettingsSnapshot,
) -> Result<String> {
    let report = DiagnosticsReport {
        service_state: service.map(|s| s.state.clone()),
        dbus: control.map(|snapshot| DbusSnapshot {
            daemon_state: snapshot.daemon_state.clone(),
            daemon_message: snapshot.daemon_message.clone(),
            auth_state: snapshot.auth_state.clone(),
            auth_message: snapshot.auth_message.clone(),
            integration_state: snapshot.integration_state.clone(),
            integration_message: snapshot.integration_message.clone(),
        }),
        integrations: IntegrationSnapshot {
            state: integrations.summary_state().to_string(),
            details: integrations.summary_message(),
            nautilus_extension_installed: integrations.nautilus_extension_installed,
            fuse_helper_installed: integrations.fuse_helper_installed,
            emblems_installed: integrations.emblems_installed,
        },
        settings,
    };
    Ok(serde_json::to_string_pretty(&report)?)
}

pub fn export_support_bundle(
    control: Option<&ControlSnapshot>,
    service: Option<&ServiceStatus>,
    integrations: &IntegrationStatus,
    settings: SettingsSnapshot,
    daemon_logs: &str,
) -> Result<PathBuf> {
    let timestamp = current_unix_timestamp();
    let path = support_bundle_export_path(dirs::download_dir(), dirs::home_dir(), timestamp);
    write_support_bundle(
        &path,
        support_bundle_text(
            control,
            service,
            integrations,
            settings,
            daemon_logs,
            timestamp,
        )?,
    )?;
    Ok(path)
}

fn support_bundle_text(
    control: Option<&ControlSnapshot>,
    service: Option<&ServiceStatus>,
    integrations: &IntegrationStatus,
    settings: SettingsSnapshot,
    daemon_logs: &str,
    generated_at_unix: u64,
) -> Result<String> {
    let diagnostics = diagnostics_report_json(control, service, integrations, settings)?;
    Ok(format!(
        "Yadisk GTK support bundle\ngenerated_at_unix={generated_at_unix}\n\n=== diagnostics ===\n{diagnostics}\n\n=== yadiskd journal ===\n{daemon_logs}\n"
    ))
}

fn write_support_bundle(path: &Path, content: String) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, content)?;
    Ok(())
}

fn support_bundle_export_path(
    download_dir: Option<PathBuf>,
    home_dir: Option<PathBuf>,
    timestamp: u64,
) -> PathBuf {
    let base_dir = download_dir.or(home_dir).unwrap_or_else(std::env::temp_dir);
    base_dir.join(format!("{SUPPORT_BUNDLE_PREFIX}-{timestamp}.txt"))
}

fn current_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn support_bundle_contains_diagnostics_and_journal_sections() {
        let integrations = IntegrationStatus {
            nautilus_extension_installed: true,
            fuse_helper_installed: false,
            emblems_installed: true,
        };
        let settings = SettingsSnapshot {
            sync_root: "/tmp/Yandex Disk".to_string(),
            cache_root: "/tmp/cache".to_string(),
            remote_root: "disk:/".to_string(),
            cloud_poll_secs: 15,
            worker_loop_ms: 500,
            local_watcher_enabled: true,
            autostart: "enabled".to_string(),
        };
        let content = support_bundle_text(
            None,
            Some(&ServiceStatus {
                state: "active".to_string(),
            }),
            &integrations,
            settings,
            "2026-03-24T00:00:00+00:00 host yadiskd[1]: started",
            42,
        )
        .unwrap();

        assert!(content.contains("generated_at_unix=42"));
        assert!(content.contains("=== diagnostics ==="));
        assert!(content.contains("\"service_state\": \"active\""));
        assert!(content.contains("=== yadiskd journal ==="));
        assert!(content.contains("host yadiskd[1]: started"));
    }

    #[test]
    fn support_bundle_path_prefers_downloads_dir() {
        let path = support_bundle_export_path(
            Some(PathBuf::from("/tmp/downloads")),
            Some(PathBuf::from("/tmp/home")),
            123,
        );
        assert_eq!(path, PathBuf::from("/tmp/downloads/yadisk-support-123.txt"));
    }

    #[test]
    fn support_bundle_path_falls_back_to_home_dir() {
        let path = support_bundle_export_path(None, Some(PathBuf::from("/tmp/home")), 123);
        assert_eq!(path, PathBuf::from("/tmp/home/yadisk-support-123.txt"));
    }
}
