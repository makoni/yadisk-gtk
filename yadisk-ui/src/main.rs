mod control_client;
mod diagnostics;
#[cfg(feature = "gtk-ui")]
mod gtk_app;
mod integration_control;
mod service_control;
mod settings;
mod state;
mod ui_model;

use control_client::ControlClient;
use diagnostics::print_diagnostics_report;
use integration_control::{
    detect_integration_status, guided_install_instructions, run_auto_install,
};
use service_control::{ServiceAction, run_service_action};
use settings::read_settings_snapshot;
use ui_model::UiModel;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CliMode {
    Status,
    StartAuth,
    CancelAuth,
    Logout,
    StartDaemon,
    StopDaemon,
    RestartDaemon,
    CheckIntegrations,
    InstallIntegrationsGuided,
    InstallIntegrationsAuto,
    ShowSettings,
    Diagnostics,
    EnableAutostart,
    DisableAutostart,
    Gtk,
    GtkWelcome,
    GtkSync,
    GtkIntegrations,
    GtkSettings,
    GtkDiagnostics,
    Help,
}

fn parse_cli_mode<I>(args: I) -> anyhow::Result<CliMode>
where
    I: IntoIterator<Item = String>,
{
    let mut mode = default_cli_mode();
    for arg in args.into_iter().skip(1) {
        mode = match arg.as_str() {
            "--status" => CliMode::Status,
            "--start-auth" => CliMode::StartAuth,
            "--cancel-auth" => CliMode::CancelAuth,
            "--logout" => CliMode::Logout,
            "--start-daemon" => CliMode::StartDaemon,
            "--stop-daemon" => CliMode::StopDaemon,
            "--restart-daemon" => CliMode::RestartDaemon,
            "--check-integrations" => CliMode::CheckIntegrations,
            "--install-integrations-guided" => CliMode::InstallIntegrationsGuided,
            "--install-integrations-auto" => CliMode::InstallIntegrationsAuto,
            "--show-settings" => CliMode::ShowSettings,
            "--diagnostics" => CliMode::Diagnostics,
            "--enable-autostart" => CliMode::EnableAutostart,
            "--disable-autostart" => CliMode::DisableAutostart,
            "--gtk" => CliMode::Gtk,
            "--tab-welcome" | "--tab-account" => CliMode::GtkWelcome,
            "--tab-sync" => CliMode::GtkSync,
            "--tab-integrations" => CliMode::GtkIntegrations,
            "--tab-settings" => CliMode::GtkSettings,
            "--tab-diagnostics" => CliMode::GtkDiagnostics,
            "--help" | "-h" => {
                print_help();
                return Ok(CliMode::Help);
            }
            other => anyhow::bail!("unknown argument: {other}"),
        };
    }
    Ok(mode)
}

#[cfg(feature = "gtk-ui")]
fn default_cli_mode() -> CliMode {
    CliMode::Gtk
}

#[cfg(not(feature = "gtk-ui"))]
fn default_cli_mode() -> CliMode {
    CliMode::Status
}

fn main() -> anyhow::Result<()> {
    let mode = parse_cli_mode(std::env::args())?;
    if mode == CliMode::Help {
        return Ok(());
    }
    if matches!(
        mode,
        CliMode::Gtk
            | CliMode::GtkWelcome
            | CliMode::GtkSync
            | CliMode::GtkIntegrations
            | CliMode::GtkSettings
            | CliMode::GtkDiagnostics
    ) {
        let tab = match mode {
            CliMode::GtkWelcome => Some("welcome".to_string()),
            CliMode::GtkSync => Some("sync".to_string()),
            CliMode::GtkIntegrations => Some("integrations".to_string()),
            CliMode::GtkSettings => Some("settings".to_string()),
            CliMode::GtkDiagnostics => Some("diagnostics".to_string()),
            _ => None,
        };
        return launch_gtk(tab);
    }

    match mode {
        CliMode::StartDaemon => run_service_action(ServiceAction::Start)?,
        CliMode::StopDaemon => run_service_action(ServiceAction::Stop)?,
        CliMode::RestartDaemon => run_service_action(ServiceAction::Restart)?,
        CliMode::EnableAutostart => run_service_action(ServiceAction::EnableAutostart)?,
        CliMode::DisableAutostart => run_service_action(ServiceAction::DisableAutostart)?,
        CliMode::InstallIntegrationsGuided => {
            for line in guided_install_instructions() {
                println!("{line}");
            }
        }
        CliMode::InstallIntegrationsAuto => run_auto_install()?,
        _ => {}
    }

    let client = ControlClient::connect().ok();
    match mode {
        CliMode::StartAuth => client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("yadiskd D-Bus service is not available"))?
            .start_auth()?,
        CliMode::CancelAuth => client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("yadiskd D-Bus service is not available"))?
            .cancel_auth()?,
        CliMode::Logout => client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("yadiskd D-Bus service is not available"))?
            .logout()?,
        CliMode::Status
        | CliMode::StartDaemon
        | CliMode::StopDaemon
        | CliMode::RestartDaemon
        | CliMode::CheckIntegrations
        | CliMode::InstallIntegrationsGuided
        | CliMode::InstallIntegrationsAuto
        | CliMode::ShowSettings
        | CliMode::Diagnostics
        | CliMode::EnableAutostart
        | CliMode::DisableAutostart => {}
        CliMode::Gtk
        | CliMode::GtkWelcome
        | CliMode::GtkSync
        | CliMode::GtkIntegrations
        | CliMode::GtkSettings
        | CliMode::GtkDiagnostics => unreachable!("gtk mode returns early"),
        CliMode::Help => unreachable!("help mode returns early"),
    }
    let integration_status = detect_integration_status();
    let settings_snapshot = read_settings_snapshot();
    let model = UiModel::collect();
    if mode == CliMode::CheckIntegrations {
        println!(
            "Integrations: state={}, details={}",
            integration_status.summary_state(),
            integration_status.summary_message()
        );
    }
    if mode == CliMode::ShowSettings {
        println!("{}", serde_json::to_string_pretty(&settings_snapshot)?);
    }
    if mode == CliMode::Diagnostics {
        print_diagnostics_report(
            model.control.as_ref(),
            model.service.as_ref(),
            &model.integrations,
            model.settings.clone(),
        )?;
    }
    if !matches!(mode, CliMode::ShowSettings | CliMode::Diagnostics) {
        state::run(&model);
    }
    Ok(())
}

fn print_help() {
    println!(
        "Usage: yadisk-ui [--status | --start-auth | --cancel-auth | --logout | --start-daemon | --stop-daemon | --restart-daemon | --enable-autostart | --disable-autostart | --check-integrations | --install-integrations-guided | --install-integrations-auto | --show-settings | --diagnostics | --gtk | --tab-welcome | --tab-sync | --tab-integrations | --tab-settings | --tab-diagnostics]\n(note: in default build with no flags, GTK window starts)"
    );
}

#[cfg(feature = "gtk-ui")]
fn launch_gtk(start_tab: Option<String>) -> anyhow::Result<()> {
    gtk_app::run(start_tab)
}

#[cfg(not(feature = "gtk-ui"))]
fn launch_gtk(_start_tab: Option<String>) -> anyhow::Result<()> {
    anyhow::bail!(
        "GTK UI is not enabled in this build. Rebuild without --no-default-features or enable --features gtk-ui."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(feature = "gtk-ui"))]
    #[test]
    fn parses_default_mode() {
        let mode = parse_cli_mode(vec!["yadisk-ui".to_string()]).unwrap();
        assert_eq!(mode, CliMode::Status);
    }

    #[cfg(feature = "gtk-ui")]
    #[test]
    fn parses_default_mode_as_gtk_with_feature() {
        let mode = parse_cli_mode(vec!["yadisk-ui".to_string()]).unwrap();
        assert_eq!(mode, CliMode::Gtk);
    }

    #[test]
    fn parses_start_auth_mode() {
        let mode =
            parse_cli_mode(vec!["yadisk-ui".to_string(), "--start-auth".to_string()]).unwrap();
        assert_eq!(mode, CliMode::StartAuth);
    }

    #[test]
    fn parses_status_mode() {
        let mode = parse_cli_mode(vec!["yadisk-ui".to_string(), "--status".to_string()]).unwrap();
        assert_eq!(mode, CliMode::Status);
    }

    #[test]
    fn parses_logout_mode() {
        let mode = parse_cli_mode(vec!["yadisk-ui".to_string(), "--logout".to_string()]).unwrap();
        assert_eq!(mode, CliMode::Logout);
    }

    #[test]
    fn parses_help_mode() {
        let mode = parse_cli_mode(vec!["yadisk-ui".to_string(), "--help".to_string()]).unwrap();
        assert_eq!(mode, CliMode::Help);
    }

    #[test]
    fn parses_start_daemon_mode() {
        let mode =
            parse_cli_mode(vec!["yadisk-ui".to_string(), "--start-daemon".to_string()]).unwrap();
        assert_eq!(mode, CliMode::StartDaemon);
    }

    #[test]
    fn parses_check_integrations_mode() {
        let mode = parse_cli_mode(vec![
            "yadisk-ui".to_string(),
            "--check-integrations".to_string(),
        ])
        .unwrap();
        assert_eq!(mode, CliMode::CheckIntegrations);
    }

    #[test]
    fn parses_diagnostics_mode() {
        let mode =
            parse_cli_mode(vec!["yadisk-ui".to_string(), "--diagnostics".to_string()]).unwrap();
        assert_eq!(mode, CliMode::Diagnostics);
    }

    #[test]
    fn parses_gtk_mode() {
        let mode = parse_cli_mode(vec!["yadisk-ui".to_string(), "--gtk".to_string()]).unwrap();
        assert_eq!(mode, CliMode::Gtk);
    }

    #[test]
    fn parses_tab_sync_mode() {
        let mode = parse_cli_mode(vec!["yadisk-ui".to_string(), "--tab-sync".to_string()]).unwrap();
        assert_eq!(mode, CliMode::GtkSync);
    }
}
