#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::RwLock;
use yadisk_integrations::ids::{
    DBUS_ERROR_BUSY, DBUS_ERROR_FAILED, DBUS_ERROR_INVALID_PATH, DBUS_ERROR_NOT_FOUND,
};
use zbus::{interface, object_server::SignalEmitter};

use crate::sync::engine::EngineError;
use crate::sync::engine::PathDisplayState;
use crate::sync::engine::SyncEngine;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathState {
    CloudOnly,
    Cached,
    Syncing,
    Error,
    Partial,
}

impl PathState {
    fn as_str(self) -> &'static str {
        match self {
            PathState::CloudOnly => "cloud_only",
            PathState::Cached => "cached",
            PathState::Syncing => "syncing",
            PathState::Error => "error",
            PathState::Partial => "partial",
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
        DbusServiceError::NotFound => DBUS_ERROR_NOT_FOUND,
        DbusServiceError::Busy => DBUS_ERROR_BUSY,
        DbusServiceError::InvalidPath => DBUS_ERROR_INVALID_PATH,
        DbusServiceError::Failed => DBUS_ERROR_FAILED,
    }
}

fn map_to_fdo(err: DbusServiceError) -> zbus::fdo::Error {
    zbus::fdo::Error::Failed(format!("{}: {}", dbus_error_name(&err), err))
}

#[derive(Default)]
pub struct SyncDbusService {
    backend: Option<Arc<SyncEngine>>,
    states: RwLock<HashMap<String, PathState>>,
    pinned: RwLock<HashMap<String, bool>>,
    conflicts: RwLock<Vec<ConflictInfo>>,
}

impl SyncDbusService {
    pub fn with_engine(engine: Arc<SyncEngine>) -> Self {
        Self {
            backend: Some(engine),
            states: RwLock::new(HashMap::new()),
            pinned: RwLock::new(HashMap::new()),
            conflicts: RwLock::new(Vec::new()),
        }
    }

    fn canonical_slash_path(path: &str) -> Result<String, DbusServiceError> {
        if path.is_empty() {
            return Err(DbusServiceError::InvalidPath);
        }
        if let Some(rest) = path.strip_prefix("disk:/") {
            let suffix = rest.trim_start_matches('/');
            return Ok(if suffix.is_empty() {
                "/".to_string()
            } else {
                format!("/{suffix}")
            });
        }
        if path.starts_with('/') {
            return Ok(path.to_string());
        }
        Err(DbusServiceError::InvalidPath)
    }

    fn canonical_disk_path(path: &str) -> Result<String, DbusServiceError> {
        let slash = Self::canonical_slash_path(path)?;
        let suffix = slash.trim_start_matches('/');
        Ok(if suffix.is_empty() {
            "disk:/".to_string()
        } else {
            format!("disk:/{suffix}")
        })
    }

    fn path_candidates(path: &str) -> Result<[String; 2], DbusServiceError> {
        let slash = Self::canonical_slash_path(path)?;
        let disk = Self::canonical_disk_path(&slash)?;
        Ok([slash, disk])
    }

    fn from_path_display_state(state: PathDisplayState) -> PathState {
        match state {
            PathDisplayState::CloudOnly => PathState::CloudOnly,
            PathDisplayState::Cached => PathState::Cached,
            PathDisplayState::Syncing => PathState::Syncing,
            PathDisplayState::Error => PathState::Error,
            PathDisplayState::Partial => PathState::Partial,
        }
    }
}

fn map_engine_error(err: EngineError) -> zbus::fdo::Error {
    match err {
        EngineError::MissingItem(_) => map_to_fdo(DbusServiceError::NotFound),
        _ => map_to_fdo(DbusServiceError::Failed),
    }
}

#[interface(name = "me.spaceinbox.yadisk.Sync1")]
impl SyncDbusService {
    async fn download(&self, path: &str) -> zbus::fdo::Result<()> {
        let [slash, disk] = Self::path_candidates(path).map_err(map_to_fdo)?;
        eprintln!("[yadiskd] dbus Download path={path}");
        if let Some(engine) = &self.backend {
            for candidate in [&slash, &disk] {
                match engine.enqueue_download(candidate).await {
                    Ok(_) => {
                        eprintln!("[yadiskd] dbus Download queued path={candidate}");
                        return Ok(());
                    }
                    Err(EngineError::MissingItem(_)) => continue,
                    Err(err) => return Err(map_engine_error(err)),
                }
            }
            return Err(map_to_fdo(DbusServiceError::NotFound));
        }
        self.states.write().await.insert(slash, PathState::Syncing);
        Ok(())
    }

    async fn pin(&self, path: &str, pin: bool) -> zbus::fdo::Result<()> {
        let [slash, disk] = Self::path_candidates(path).map_err(map_to_fdo)?;
        eprintln!("[yadiskd] dbus Pin path={path} pin={pin}");
        if let Some(engine) = &self.backend {
            for candidate in [&slash, &disk] {
                match engine.pin_path(candidate, pin).await {
                    Ok(_) => {
                        eprintln!("[yadiskd] dbus Pin updated path={candidate} pin={pin}");
                        return Ok(());
                    }
                    Err(EngineError::MissingItem(_)) => continue,
                    Err(err) => return Err(map_engine_error(err)),
                }
            }
            return Err(map_to_fdo(DbusServiceError::NotFound));
        }
        self.pinned.write().await.insert(slash, pin);
        Ok(())
    }

    async fn evict(&self, path: &str) -> zbus::fdo::Result<()> {
        let [slash, disk] = Self::path_candidates(path).map_err(map_to_fdo)?;
        eprintln!("[yadiskd] dbus Evict path={path}");
        if let Some(engine) = &self.backend {
            for candidate in [&slash, &disk] {
                match engine.evict_path(candidate).await {
                    Ok(_) => {
                        eprintln!("[yadiskd] dbus Evict done path={candidate}");
                        return Ok(());
                    }
                    Err(EngineError::MissingItem(_)) => continue,
                    Err(err) => return Err(map_engine_error(err)),
                }
            }
            return Err(map_to_fdo(DbusServiceError::NotFound));
        }
        self.states
            .write()
            .await
            .insert(slash, PathState::CloudOnly);
        Ok(())
    }

    async fn retry(&self, path: &str) -> zbus::fdo::Result<()> {
        let [slash, disk] = Self::path_candidates(path).map_err(map_to_fdo)?;
        eprintln!("[yadiskd] dbus Retry path={path}");
        if let Some(engine) = &self.backend {
            for candidate in [&slash, &disk] {
                match engine.retry_path(candidate).await {
                    Ok(_) => {
                        eprintln!("[yadiskd] dbus Retry queued path={candidate}");
                        return Ok(());
                    }
                    Err(EngineError::MissingItem(_)) => continue,
                    Err(err) => return Err(map_engine_error(err)),
                }
            }
            return Err(map_to_fdo(DbusServiceError::NotFound));
        }
        self.states.write().await.insert(slash, PathState::Syncing);
        Ok(())
    }

    async fn get_state(&self, path: &str) -> zbus::fdo::Result<String> {
        let [slash, disk] = Self::path_candidates(path).map_err(map_to_fdo)?;
        if let Some(engine) = &self.backend {
            for candidate in [&slash, &disk] {
                if let Some(state) = engine
                    .state_for_path(candidate)
                    .await
                    .map_err(map_engine_error)?
                {
                    return Ok(Self::from_path_display_state(state).as_str().to_string());
                }
            }
            return Err(map_to_fdo(DbusServiceError::NotFound));
        }
        let states = self.states.read().await;
        let state = states
            .get(slash.as_str())
            .ok_or(DbusServiceError::NotFound)
            .map_err(map_to_fdo)?;
        Ok(state.as_str().to_string())
    }

    async fn list_conflicts(&self) -> zbus::fdo::Result<Vec<(u64, String, String)>> {
        if let Some(engine) = &self.backend {
            let conflicts = engine.list_conflicts().await.map_err(map_engine_error)?;
            return Ok(conflicts
                .into_iter()
                .map(|c| (c.id as u64, c.path, c.renamed_local))
                .collect());
        }
        let conflicts = self.conflicts.read().await;
        Ok(conflicts
            .iter()
            .map(|c| (c.id, c.path.clone(), c.renamed_local.clone()))
            .collect())
    }

    #[zbus(signal)]
    pub async fn state_changed(
        ctxt: &SignalEmitter<'_>,
        path: &str,
        state: &str,
    ) -> zbus::Result<()>;

    #[zbus(signal)]
    pub async fn conflict_added(
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
            DBUS_ERROR_NOT_FOUND
        );
        assert_eq!(dbus_error_name(&DbusServiceError::Busy), DBUS_ERROR_BUSY);
        assert_eq!(
            dbus_error_name(&DbusServiceError::InvalidPath),
            DBUS_ERROR_INVALID_PATH
        );
        assert_eq!(
            dbus_error_name(&DbusServiceError::Failed),
            DBUS_ERROR_FAILED
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
                assert!(msg.contains(DBUS_ERROR_INVALID_PATH));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn accepts_disk_prefixed_paths() {
        let service = SyncDbusService::default();
        service.download("disk:/Docs/A.txt").await.unwrap();
        let state = service.get_state("disk:/Docs/A.txt").await.unwrap();
        assert_eq!(state, "syncing");
    }

    #[tokio::test]
    async fn get_state_supports_partial_value() {
        let service = SyncDbusService::default();
        service
            .states
            .write()
            .await
            .insert("/Docs".to_string(), PathState::Partial);
        let state = service.get_state("/Docs").await.unwrap();
        assert_eq!(state, "partial");
    }
}
