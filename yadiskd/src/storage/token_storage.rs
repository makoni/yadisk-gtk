use keyring::Entry;
use thiserror::Error;

use super::{
    portal_token_store::{PortalStoreError, PortalTokenStore},
    secret_portal,
};

const SERVICE_NAME: &str = "com.yadisk.gtk";
const TOKEN_KEY: &str = "yadisk_token";

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
        match &self.backend {
            Backend::Portal(store) => store.save_token(token).map_err(StorageError::Portal),
            Backend::Keyring => {
                self.entry.set_password(token)?;
                Ok(())
            }
        }
    }

    pub fn get_token(&self) -> Result<String, StorageError> {
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
