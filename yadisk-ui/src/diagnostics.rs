use anyhow::Result;
use serde::Serialize;

use crate::control_client::ControlSnapshot;
use crate::integration_control::IntegrationStatus;
use crate::service_control::ServiceStatus;
use crate::settings::SettingsSnapshot;

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
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}
