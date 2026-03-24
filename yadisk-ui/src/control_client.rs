use std::time::Duration;

use anyhow::{Context, Result};
use yadisk_integrations::ids::{
    DBUS_INTERFACE_CONTROL, DBUS_NAME_CONTROL, DBUS_OBJECT_PATH_CONTROL,
};
use zbus::blocking::{Connection, Proxy, connection::Builder as ConnectionBuilder};

const CONTROL_STATUS_TIMEOUT: Duration = Duration::from_secs(5);
const CONTROL_ACTION_TIMEOUT: Duration = Duration::from_secs(15);
const CONTROL_SUBMIT_AUTH_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlSnapshot {
    pub daemon_state: String,
    pub daemon_message: String,
    pub auth_state: String,
    pub auth_message: String,
    pub integration_state: String,
    pub integration_message: String,
}

pub struct ControlClient {}

impl ControlClient {
    pub fn connect() -> Result<Self> {
        let _ = connect_control(CONTROL_STATUS_TIMEOUT)?;
        Ok(Self {})
    }

    pub fn get_statuses(&self) -> Result<ControlSnapshot> {
        self.with_proxy(CONTROL_STATUS_TIMEOUT, |proxy| {
            snapshot_from_status_calls(
                status_call(proxy, "GetDaemonStatus"),
                status_call(proxy, "GetAuthState"),
                status_call(proxy, "GetIntegrationStatus"),
            )
        })
    }

    pub fn start_auth(&self) -> Result<String> {
        self.with_proxy(CONTROL_ACTION_TIMEOUT, |proxy| {
            proxy.call("StartAuth", &()).context("StartAuth failed")
        })
    }

    pub fn submit_auth_code(&self, code: &str) -> Result<()> {
        self.with_proxy(CONTROL_SUBMIT_AUTH_TIMEOUT, |proxy| {
            proxy
                .call::<_, _, ()>("SubmitAuthCode", &(code,))
                .context("SubmitAuthCode failed")
        })
    }

    pub fn cancel_auth(&self) -> Result<()> {
        self.with_proxy(CONTROL_ACTION_TIMEOUT, |proxy| {
            proxy
                .call::<_, _, ()>("CancelAuth", &())
                .context("CancelAuth failed")
        })
    }

    pub fn logout(&self) -> Result<()> {
        self.with_proxy(CONTROL_ACTION_TIMEOUT, |proxy| {
            proxy
                .call::<_, _, ()>("Logout", &())
                .context("Logout failed")
        })
    }

    fn with_proxy<T>(
        &self,
        timeout: Duration,
        f: impl FnOnce(&Proxy<'_>) -> Result<T>,
    ) -> Result<T> {
        let connection = connect_control(timeout)?;
        let proxy = self.proxy(&connection)?;
        f(&proxy)
    }

    fn proxy<'a>(&self, connection: &'a Connection) -> Result<Proxy<'a>> {
        Proxy::new(
            connection,
            DBUS_NAME_CONTROL,
            DBUS_OBJECT_PATH_CONTROL,
            DBUS_INTERFACE_CONTROL,
        )
        .context("failed to create Control1 proxy")
    }
}

fn connect_control(timeout: Duration) -> Result<Connection> {
    ConnectionBuilder::session()
        .context("failed to open session D-Bus builder for Control1")?
        .method_timeout(timeout)
        .build()
        .context("failed to connect to session D-Bus for Control1")
}

fn status_call(proxy: &Proxy<'_>, method: &str) -> Result<(String, String)> {
    proxy
        .call(method, &())
        .with_context(|| format!("{method} failed"))
}

fn snapshot_from_status_calls(
    daemon: Result<(String, String)>,
    auth: Result<(String, String)>,
    integration: Result<(String, String)>,
) -> Result<ControlSnapshot> {
    let mut any_success = false;
    let mut first_err = None;
    let (daemon_state, daemon_message) = partial_status_field(
        "GetDaemonStatus",
        "daemon state unavailable",
        daemon,
        &mut any_success,
        &mut first_err,
    );
    let (auth_state, auth_message) = partial_status_field(
        "GetAuthState",
        "auth state unavailable",
        auth,
        &mut any_success,
        &mut first_err,
    );
    let (integration_state, integration_message) = partial_status_field(
        "GetIntegrationStatus",
        "integration state unavailable",
        integration,
        &mut any_success,
        &mut first_err,
    );
    if !any_success {
        return Err(first_err.expect("status calls without success must keep first error"));
    }
    Ok(ControlSnapshot {
        daemon_state,
        daemon_message,
        auth_state,
        auth_message,
        integration_state,
        integration_message,
    })
}

fn partial_status_field(
    method: &str,
    fallback_message: &str,
    result: Result<(String, String)>,
    any_success: &mut bool,
    first_err: &mut Option<anyhow::Error>,
) -> (String, String) {
    match result {
        Ok(value) => {
            *any_success = true;
            value
        }
        Err(err) => {
            eprintln!("[yadisk-ui] Control1 {method} failed: {err}");
            if first_err.is_none() {
                *first_err = Some(err);
            }
            ("unknown".to_string(), fallback_message.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_status_snapshot_keeps_successful_fields() {
        let snapshot = snapshot_from_status_calls(
            Ok(("running".to_string(), "idle".to_string())),
            Err(anyhow::anyhow!("auth timeout")),
            Ok((
                "ok".to_string(),
                "all integration components are present".to_string(),
            )),
        )
        .unwrap();

        assert_eq!(snapshot.daemon_state, "running");
        assert_eq!(snapshot.auth_state, "unknown");
        assert_eq!(snapshot.auth_message, "auth state unavailable");
        assert_eq!(snapshot.integration_state, "ok");
    }

    #[test]
    fn status_snapshot_returns_error_when_every_call_fails() {
        let err = snapshot_from_status_calls(
            Err(anyhow::anyhow!("daemon timeout")),
            Err(anyhow::anyhow!("auth timeout")),
            Err(anyhow::anyhow!("integration timeout")),
        )
        .unwrap_err();

        assert!(err.to_string().contains("daemon timeout"));
    }
}
