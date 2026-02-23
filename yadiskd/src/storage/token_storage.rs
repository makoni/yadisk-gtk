use keyring::Entry;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::{
    portal_token_store::{PortalStoreError, PortalTokenStore},
    secret_portal,
};

const SERVICE_NAME: &str = "com.yadisk.gtk";
const TOKEN_KEY: &str = "yadisk_token";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OAuthState {
    pub access_token: String,
    #[serde(default)]
    pub refresh_token: Option<String>,
    #[serde(default)]
    pub expires_at: Option<i64>,
    #[serde(default)]
    pub scope: Option<String>,
    #[serde(default)]
    pub token_type: Option<String>,
}

impl OAuthState {
    pub fn from_access_token(access_token: impl Into<String>) -> Self {
        Self {
            access_token: access_token.into(),
            refresh_token: None,
            expires_at: None,
            scope: None,
            token_type: None,
        }
    }

    pub fn from_oauth_token(token: &yadisk_core::OAuthToken) -> Self {
        Self {
            access_token: token.access_token.clone(),
            refresh_token: token.refresh_token.clone(),
            expires_at: token
                .expires_in
                .map(|ttl| now_unix().saturating_add(ttl as i64)),
            scope: token.scope.clone(),
            token_type: Some(token.token_type.clone()),
        }
    }
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("keyring error: {0}")]
    KeyringError(#[from] keyring::Error),
    #[error("token not found")]
    TokenNotFound,
    #[error("secret portal required but unavailable: {0}")]
    PortalUnavailable(PortalStoreError),
    #[error("portal storage error: {0}")]
    Portal(PortalStoreError),
    #[error("oauth state serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub struct TokenStorage {
    entry: Entry,
    backend: Backend,
}

enum Backend {
    Portal(PortalTokenStore),
    Keyring,
}

impl TokenStorage {
    pub async fn new() -> Result<Self, StorageError> {
        let entry = Entry::new(SERVICE_NAME, TOKEN_KEY)?;
        let preference = secret_portal::portal_preference();

        if preference.use_portal() {
            match PortalTokenStore::new().await {
                Ok(store) => {
                    migrate_keyring_token(&entry, &store);
                    return Ok(Self {
                        entry,
                        backend: Backend::Portal(store),
                    });
                }
                Err(err) => {
                    if !preference.allow_fallback() {
                        return Err(StorageError::PortalUnavailable(err));
                    }
                }
            }
        }

        Ok(Self {
            entry,
            backend: Backend::Keyring,
        })
    }

    #[allow(dead_code)]
    pub fn save_token(&self, token: &str) -> Result<(), StorageError> {
        self.save_oauth_state(&OAuthState::from_access_token(token))
    }

    pub fn save_oauth_state(&self, state: &OAuthState) -> Result<(), StorageError> {
        let payload = serde_json::to_string(state)?;
        self.write_raw_token(&payload)
    }

    fn write_raw_token(&self, raw: &str) -> Result<(), StorageError> {
        match &self.backend {
            Backend::Portal(store) => store.save_token(raw).map_err(StorageError::Portal),
            Backend::Keyring => {
                self.entry.set_password(raw)?;
                Ok(())
            }
        }
    }

    pub fn get_token(&self) -> Result<String, StorageError> {
        Ok(self.get_oauth_state()?.access_token)
    }

    pub fn get_oauth_state(&self) -> Result<OAuthState, StorageError> {
        let raw = self.read_raw_token()?;
        if let Some(state) = parse_oauth_state(&raw) {
            return Ok(state);
        }

        let migrated = OAuthState::from_access_token(raw);
        self.save_oauth_state(&migrated)?;
        Ok(migrated)
    }

    fn read_raw_token(&self) -> Result<String, StorageError> {
        match &self.backend {
            Backend::Portal(store) => store.get_token().map_err(StorageError::Portal),
            Backend::Keyring => match self.entry.get_password() {
                Ok(token) => Ok(token),
                Err(keyring::Error::NoEntry) => Err(StorageError::TokenNotFound),
                Err(err) => Err(StorageError::KeyringError(err)),
            },
        }
    }

    #[allow(dead_code)]
    pub fn delete_token(&self) -> Result<(), StorageError> {
        match &self.backend {
            Backend::Portal(store) => store.delete_token().map_err(StorageError::Portal),
            Backend::Keyring => match self.entry.delete_credential() {
                Ok(()) | Err(keyring::Error::NoEntry) => Ok(()),
                Err(err) => Err(StorageError::KeyringError(err)),
            },
        }
    }

    #[allow(dead_code)]
    pub fn has_token(&self) -> bool {
        match &self.backend {
            Backend::Portal(store) => store.has_token(),
            Backend::Keyring => self.entry.get_password().is_ok(),
        }
    }
}

fn migrate_keyring_token(entry: &Entry, store: &PortalTokenStore) {
    if store.has_token() {
        return;
    }

    if let Ok(token) = entry.get_password()
        && store.save_token(&token).is_ok()
    {
        let _ = entry.delete_credential();
    }
}

fn now_unix() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn parse_oauth_state(raw: &str) -> Option<OAuthState> {
    serde_json::from_str::<OAuthState>(raw).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_json_oauth_state() {
        let raw = r#"{"access_token":"a","refresh_token":"r","expires_at":10,"scope":"disk:read","token_type":"bearer"}"#;
        let state = parse_oauth_state(raw).expect("state should parse");
        assert_eq!(state.access_token, "a");
        assert_eq!(state.refresh_token.as_deref(), Some("r"));
        assert_eq!(state.expires_at, Some(10));
    }

    #[test]
    fn legacy_token_falls_back_to_access_only_state() {
        let state = OAuthState::from_access_token("legacy");
        assert_eq!(state.access_token, "legacy");
        assert!(state.refresh_token.is_none());
        assert!(state.expires_at.is_none());
    }
}
