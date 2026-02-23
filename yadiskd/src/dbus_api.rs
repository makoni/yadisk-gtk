#![allow(dead_code)]

use std::collections::HashMap;

use thiserror::Error;
use tokio::sync::RwLock;
use zbus::{interface, object_server::SignalEmitter};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathState {
    CloudOnly,
    Cached,
    Syncing,
    Error,
}

impl PathState {
    fn as_str(self) -> &'static str {
        match self {
            PathState::CloudOnly => "cloud_only",
            PathState::Cached => "cached",
            PathState::Syncing => "syncing",
            PathState::Error => "error",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictInfo {
    pub id: u64,
    pub path: String,
    pub renamed_local: String,
}

#[derive(Debug, Error)]
pub enum DbusServiceError {
    #[error("path does not exist")]
    NotFound,
    #[error("operation in progress")]
    Busy,
    #[error("invalid path")]
    InvalidPath,
    #[error("operation failed")]
    Failed,
}

pub fn dbus_error_name(err: &DbusServiceError) -> &'static str {
    match err {
        DbusServiceError::NotFound => "com.yadisk.Sync1.Error.NotFound",
        DbusServiceError::Busy => "com.yadisk.Sync1.Error.Busy",
        DbusServiceError::InvalidPath => "com.yadisk.Sync1.Error.InvalidPath",
        DbusServiceError::Failed => "com.yadisk.Sync1.Error.Failed",
    }
}

fn map_to_fdo(err: DbusServiceError) -> zbus::fdo::Error {
    zbus::fdo::Error::Failed(format!("{}: {}", dbus_error_name(&err), err))
}

#[derive(Default)]
pub struct SyncDbusService {
    states: RwLock<HashMap<String, PathState>>,
    pinned: RwLock<HashMap<String, bool>>,
    conflicts: RwLock<Vec<ConflictInfo>>,
}

impl SyncDbusService {
    fn validate_path(path: &str) -> Result<(), DbusServiceError> {
        if path.is_empty() || !path.starts_with('/') {
            return Err(DbusServiceError::InvalidPath);
        }
        Ok(())
    }
}

#[interface(name = "com.yadisk.Sync1")]
impl SyncDbusService {
    async fn download(&self, path: &str) -> zbus::fdo::Result<()> {
        Self::validate_path(path).map_err(map_to_fdo)?;
        self.states
            .write()
            .await
            .insert(path.to_string(), PathState::Syncing);
        Ok(())
    }

    async fn pin(&self, path: &str, pin: bool) -> zbus::fdo::Result<()> {
        Self::validate_path(path).map_err(map_to_fdo)?;
        self.pinned.write().await.insert(path.to_string(), pin);
        Ok(())
    }

    async fn evict(&self, path: &str) -> zbus::fdo::Result<()> {
        Self::validate_path(path).map_err(map_to_fdo)?;
        self.states
            .write()
            .await
            .insert(path.to_string(), PathState::CloudOnly);
        Ok(())
    }

    async fn retry(&self, path: &str) -> zbus::fdo::Result<()> {
        Self::validate_path(path).map_err(map_to_fdo)?;
        self.states
            .write()
            .await
            .insert(path.to_string(), PathState::Syncing);
        Ok(())
    }

    async fn get_state(&self, path: &str) -> zbus::fdo::Result<String> {
        Self::validate_path(path).map_err(map_to_fdo)?;
        let states = self.states.read().await;
        let state = states
            .get(path)
            .ok_or(DbusServiceError::NotFound)
            .map_err(map_to_fdo)?;
        Ok(state.as_str().to_string())
    }

    async fn list_conflicts(&self) -> zbus::fdo::Result<Vec<(u64, String, String)>> {
        let conflicts = self.conflicts.read().await;
        Ok(conflicts
            .iter()
            .map(|c| (c.id, c.path.clone(), c.renamed_local.clone()))
            .collect())
    }

    #[zbus(signal)]
    async fn state_changed(ctxt: &SignalEmitter<'_>, path: &str, state: &str) -> zbus::Result<()>;

    #[zbus(signal)]
    async fn conflict_added(
        ctxt: &SignalEmitter<'_>,
        id: u64,
        path: &str,
        renamed_local: &str,
    ) -> zbus::Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_errors_to_stable_dbus_names() {
        assert_eq!(
            dbus_error_name(&DbusServiceError::NotFound),
            "com.yadisk.Sync1.Error.NotFound"
        );
        assert_eq!(
            dbus_error_name(&DbusServiceError::Busy),
            "com.yadisk.Sync1.Error.Busy"
        );
        assert_eq!(
            dbus_error_name(&DbusServiceError::InvalidPath),
            "com.yadisk.Sync1.Error.InvalidPath"
        );
        assert_eq!(
            dbus_error_name(&DbusServiceError::Failed),
            "com.yadisk.Sync1.Error.Failed"
        );
    }

    #[tokio::test]
    async fn download_sets_syncing_state() {
        let service = SyncDbusService::default();
        service.download("/Docs/A.txt").await.unwrap();
        let state = service.get_state("/Docs/A.txt").await.unwrap();
        assert_eq!(state, "syncing");
    }

    #[tokio::test]
    async fn invalid_path_returns_error() {
        let service = SyncDbusService::default();
        let err = service
            .download("bad")
            .await
            .expect_err("expected invalid path error");
        match err {
            zbus::fdo::Error::Failed(msg) => {
                assert!(msg.contains("com.yadisk.Sync1.Error.InvalidPath"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
