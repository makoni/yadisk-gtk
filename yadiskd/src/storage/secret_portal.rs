use std::{env, io::Read, os::unix::net::UnixStream};

use ashpd::desktop::secret::Secret as PortalClient;
use thiserror::Error;

const ENABLE_ENV: &str = "YADISK_ENABLE_SECRET_PORTAL";
const DISABLE_ENV: &str = "YADISK_DISABLE_SECRET_PORTAL";

#[derive(Debug, Error)]
pub enum PortalSecretError {
    #[error("failed to create UNIX stream: {0}")]
    Io(#[from] std::io::Error),
    #[error("secret portal request failed: {0}")]
    Portal(ashpd::Error),
    #[error("secret portal response missing expected data")]
    InvalidResponse,
}

#[derive(Debug, Clone)]
pub struct PortalSecret {
    pub secret: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub enum PortalPreference {
    ForcedEnabled,
    ForcedDisabled,
    Sandboxed,
    Disabled,
}

impl PortalPreference {
    pub fn use_portal(self) -> bool {
        matches!(
            self,
            PortalPreference::ForcedEnabled | PortalPreference::Sandboxed
        )
    }

    pub fn allow_fallback(self) -> bool {
        matches!(
            self,
            PortalPreference::Disabled | PortalPreference::ForcedDisabled
        )
    }
}

pub fn portal_preference() -> PortalPreference {
    if env_flag(DISABLE_ENV) {
        return PortalPreference::ForcedDisabled;
    }

    if env_flag(ENABLE_ENV) {
        return PortalPreference::ForcedEnabled;
    }

    if sandbox_reason().is_some() {
        return PortalPreference::Sandboxed;
    }

    PortalPreference::Disabled
}

pub async fn retrieve_secret() -> Result<PortalSecret, PortalSecretError> {
    let (reader, writer) = UnixStream::pair()?;

    let portal = PortalClient::new()
        .await
        .map_err(PortalSecretError::Portal)?;
    let request = portal
        .retrieve(&writer)
        .await
        .map_err(PortalSecretError::Portal)?;
    request.response().map_err(PortalSecretError::Portal)?;
    drop(writer);

    let secret_bytes = read_secret(reader)?;
    if secret_bytes.is_empty() {
        return Err(PortalSecretError::InvalidResponse);
    }

    Ok(PortalSecret {
        secret: secret_bytes,
    })
}

fn sandbox_reason() -> Option<&'static str> {
    const CANDIDATES: [(&str, &str); 2] =
        [("FLATPAK_ID", "FLATPAK_ID"), ("CONTAINER", "CONTAINER")];

    for (var, label) in CANDIDATES {
        if let Ok(value) = env::var(var)
            && (var != "CONTAINER"
                || value.eq_ignore_ascii_case("flatpak")
                || value.eq_ignore_ascii_case("snap"))
        {
            return Some(label);
        }
    }

    None
}

fn env_flag(name: &str) -> bool {
    env::var(name)
        .ok()
        .map(|value| parse_flag(&value))
        .unwrap_or(false)
}

fn read_secret(mut reader: UnixStream) -> Result<Vec<u8>, PortalSecretError> {
    let mut bytes = Vec::with_capacity(64);
    reader.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn parse_flag(value: &str) -> bool {
    matches!(
        value,
        "1" | "true" | "TRUE" | "True" | "yes" | "YES" | "Yes" | "on" | "ON" | "On"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_truthy_flags() {
        for value in [
            "1", "true", "TRUE", "True", "yes", "YES", "Yes", "on", "ON", "On",
        ] {
            assert!(
                parse_flag(value),
                "Expected '{value}' to be recognized as true"
            );
        }
        assert!(!parse_flag("0"));
        assert!(!parse_flag("false"));
        assert!(!parse_flag(""));
    }
}
