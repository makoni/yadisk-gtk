#![allow(dead_code)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::RwLock;
use yadisk_integrations::ids::{
    DBUS_ERROR_BUSY, DBUS_ERROR_FAILED, DBUS_ERROR_INVALID_PATH, DBUS_ERROR_NOT_FOUND,
};
use zbus::{interface, object_server::SignalEmitter};

use crate::oauth_flow::{OAuthFlow, OAuthFlowError};
use crate::storage::{OAuthState, TokenStorage};
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

#[derive(Default)]
pub struct ControlDbusService {
    backend: Option<Arc<SyncEngine>>,
    auth_override: RwLock<Option<(String, String)>>,
    integration_override: RwLock<Option<(String, String)>>,
}

impl ControlDbusService {
    pub fn with_engine(engine: Arc<SyncEngine>) -> Self {
        Self {
            backend: Some(engine),
            auth_override: RwLock::new(None),
            integration_override: RwLock::new(None),
        }
    }

    async fn daemon_status_tuple(&self) -> (String, String) {
        let has_work = if let Some(engine) = &self.backend {
            engine.has_active_or_queued_work().await.unwrap_or(false)
        } else {
            false
        };
        if has_work {
            (
                "busy".to_string(),
                "queued or active operations".to_string(),
            )
        } else {
            ("running".to_string(), "idle".to_string())
        }
    }

    async fn set_auth_override(&self, state: &str, message: &str) {
        let mut auth_override = self.auth_override.write().await;
        *auth_override = Some((state.to_string(), message.to_string()));
    }

    fn oauth_flow_from_env() -> Result<OAuthFlow, zbus::fdo::Error> {
        let client_id = std::env::var("YADISK_CLIENT_ID")
            .map_err(|_| zbus::fdo::Error::Failed("YADISK_CLIENT_ID is missing".to_string()))?;
        let client_secret = std::env::var("YADISK_CLIENT_SECRET")
            .map_err(|_| zbus::fdo::Error::Failed("YADISK_CLIENT_SECRET is missing".to_string()))?;
        Ok(OAuthFlow::new(client_id, client_secret))
    }

    fn detect_integration_status() -> (String, String) {
        let nautilus_installed = nautilus_candidate_paths()
            .into_iter()
            .map(|base| base.join("libyadisk_nautilus.so"))
            .any(|path| path.is_file());
        let fuse_installed = std::env::var_os("HOME")
            .map(PathBuf::from)
            .map(|home| home.join(".local/bin/yadisk-fuse"))
            .is_some_and(|path| path.is_file());
        let emblems_installed = std::env::var_os("HOME")
            .map(PathBuf::from)
            .map(|home| {
                home.join(".local/share/icons/hicolor/scalable/emblems")
                    .join("cloud-outline-thin-symbolic.svg")
            })
            .is_some_and(|path| path.is_file());
        let state = if nautilus_installed && fuse_installed && emblems_installed {
            "ok".to_string()
        } else {
            "needs_setup".to_string()
        };
        let mut missing = Vec::new();
        if !nautilus_installed {
            missing.push("nautilus_extension");
        }
        if !fuse_installed {
            missing.push("fuse_helper");
        }
        if !emblems_installed {
            missing.push("emblems");
        }
        let message = if missing.is_empty() {
            "all integration components are present".to_string()
        } else {
            format!("missing components: {}", missing.join(", "))
        };
        (state, message)
    }
}

fn nautilus_candidate_paths() -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(path) = std::env::var_os("YADISK_NAUTILUS_EXT_DIR") {
        paths.push(PathBuf::from(path));
    }
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        paths.push(home.join(".local/lib/nautilus/extensions-4"));
    }
    paths.push(PathBuf::from("/usr/lib/nautilus/extensions-4"));
    paths.push(PathBuf::from(
        "/usr/lib/x86_64-linux-gnu/nautilus/extensions-4",
    ));
    paths
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

#[interface(name = "me.spaceinbox.yadisk.Control1")]
impl ControlDbusService {
    async fn get_daemon_status(&self) -> zbus::fdo::Result<(String, String)> {
        Ok(self.daemon_status_tuple().await)
    }

    async fn get_auth_state(&self) -> zbus::fdo::Result<(String, String)> {
        if let Some(override_state) = self.auth_override.read().await.as_ref() {
            return Ok(override_state.clone());
        }
        let storage = TokenStorage::new()
            .await
            .map_err(|err| zbus::fdo::Error::Failed(format!("token storage error: {err}")))?;
        if storage.has_token() {
            Ok(("authorized".to_string(), "saved token found".to_string()))
        } else {
            Ok(("unauthorized".to_string(), "token is missing".to_string()))
        }
    }

    async fn get_integration_status(&self) -> zbus::fdo::Result<(String, String)> {
        if let Some(override_state) = self.integration_override.read().await.as_ref() {
            return Ok(override_state.clone());
        }
        Ok(Self::detect_integration_status())
    }

    async fn start_auth(&self) -> zbus::fdo::Result<()> {
        self.set_auth_override("pending", "auth flow start requested")
            .await;
        let flow = Self::oauth_flow_from_env()?;
        let token = match flow.authenticate().await {
            Ok(token) => token,
            Err(OAuthFlowError::Cancelled) => {
                self.set_auth_override("unauthorized", "authorization cancelled")
                    .await;
                return Ok(());
            }
            Err(err) => {
                self.set_auth_override("error", &err.to_string()).await;
                return Err(zbus::fdo::Error::Failed(format!(
                    "authorization failed: {err}"
                )));
            }
        };

        let storage = TokenStorage::new()
            .await
            .map_err(|err| zbus::fdo::Error::Failed(format!("token storage error: {err}")))?;
        storage
            .save_oauth_state(&OAuthState::from_oauth_token(&token))
            .map_err(|err| zbus::fdo::Error::Failed(format!("failed to save token: {err}")))?;
        self.set_auth_override("authorized", "token saved").await;
        Ok(())
    }

    async fn cancel_auth(&self) -> zbus::fdo::Result<()> {
        self.set_auth_override("cancelled", "auth request cancelled")
            .await;
        Ok(())
    }

    async fn logout(&self) -> zbus::fdo::Result<()> {
        let storage = TokenStorage::new()
            .await
            .map_err(|err| zbus::fdo::Error::Failed(format!("token storage error: {err}")))?;
        storage
            .delete_token()
            .map_err(|err| zbus::fdo::Error::Failed(format!("logout failed: {err}")))?;
        self.set_auth_override("unauthorized", "token removed")
            .await;
        Ok(())
    }

    async fn run_integration_check(&self) -> zbus::fdo::Result<(String, String)> {
        let status = Self::detect_integration_status();
        {
            let mut integration_override = self.integration_override.write().await;
            *integration_override = Some(status.clone());
        }
        Ok(status)
    }

    #[zbus(signal)]
    pub async fn daemon_status_changed(
        ctxt: &SignalEmitter<'_>,
        state: &str,
        message: &str,
    ) -> zbus::Result<()>;

    #[zbus(signal)]
    pub async fn auth_state_changed(
        ctxt: &SignalEmitter<'_>,
        state: &str,
        message: &str,
    ) -> zbus::Result<()>;

    #[zbus(signal)]
    pub async fn integration_status_changed(
        ctxt: &SignalEmitter<'_>,
        state: &str,
        details: &str,
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

    #[tokio::test]
    async fn control_service_defaults_to_running_idle() {
        let service = ControlDbusService::default();
        let (state, message) = service.get_daemon_status().await.unwrap();
        assert_eq!(state, "running");
        assert_eq!(message, "idle");
    }

    #[tokio::test]
    async fn control_service_supports_integration_check_override() {
        let service = ControlDbusService::default();
        let (state, message) = service.run_integration_check().await.unwrap();
        assert!(matches!(state.as_str(), "ok" | "needs_setup"));
        assert!(!message.is_empty());
    }
}
