use anyhow::{Context, Result};
use yadisk_integrations::ids::{
    DBUS_INTERFACE_CONTROL, DBUS_NAME_CONTROL, DBUS_OBJECT_PATH_CONTROL,
};
use zbus::blocking::{Connection, Proxy};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlSnapshot {
    pub daemon_state: String,
    pub daemon_message: String,
    pub auth_state: String,
    pub auth_message: String,
    pub integration_state: String,
    pub integration_message: String,
}

pub struct ControlClient {
    connection: Connection,
}

impl ControlClient {
    pub fn connect() -> Result<Self> {
        let connection =
            Connection::session().context("failed to connect to session D-Bus for Control1")?;
        Ok(Self { connection })
    }

    pub fn get_statuses(&self) -> Result<ControlSnapshot> {
        let proxy = self.proxy()?;
        let (daemon_state, daemon_message): (String, String) = proxy
            .call("GetDaemonStatus", &())
            .context("GetDaemonStatus failed")?;
        let (auth_state, auth_message): (String, String) = proxy
            .call("GetAuthState", &())
            .context("GetAuthState failed")?;
        let (integration_state, integration_message): (String, String) = proxy
            .call("GetIntegrationStatus", &())
            .context("GetIntegrationStatus failed")?;
        Ok(ControlSnapshot {
            daemon_state,
            daemon_message,
            auth_state,
            auth_message,
            integration_state,
            integration_message,
        })
    }

    pub fn start_auth(&self) -> Result<()> {
        self.proxy()?
            .call::<_, _, ()>("StartAuth", &())
            .context("StartAuth failed")
    }

    pub fn cancel_auth(&self) -> Result<()> {
        self.proxy()?
            .call::<_, _, ()>("CancelAuth", &())
            .context("CancelAuth failed")
    }

    pub fn logout(&self) -> Result<()> {
        self.proxy()?
            .call::<_, _, ()>("Logout", &())
            .context("Logout failed")
    }

    fn proxy(&self) -> Result<Proxy<'_>> {
        Proxy::new(
            &self.connection,
            DBUS_NAME_CONTROL,
            DBUS_OBJECT_PATH_CONTROL,
            DBUS_INTERFACE_CONTROL,
        )
        .context("failed to create Control1 proxy")
    }
}
