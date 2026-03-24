#![allow(dead_code)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use thiserror::Error;
use tokio::sync::RwLock;
use url::Url;
use yadisk_core::OAuthClient;
use yadisk_integrations::ids::{
    DBUS_ERROR_BUSY, DBUS_ERROR_FAILED, DBUS_ERROR_INVALID_PATH, DBUS_ERROR_NOT_FOUND,
};
use zbus::{interface, object_server::SignalEmitter};

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

type FullStateSnapshot = (Vec<(String, String)>, Vec<(u64, String, String)>);

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

    async fn full_state_snapshot(&self) -> zbus::fdo::Result<FullStateSnapshot> {
        if let Some(engine) = &self.backend {
            let items = engine
                .list_items_by_prefix("/")
                .await
                .map_err(map_engine_error)?;
            let mut states = HashMap::with_capacity(items.len());
            for item in items {
                let Some(state) = engine
                    .state_for_path(&item.path)
                    .await
                    .map_err(map_engine_error)?
                else {
                    continue;
                };
                let slash = Self::canonical_slash_path(&item.path).map_err(map_to_fdo)?;
                states.insert(
                    slash,
                    Self::from_path_display_state(state).as_str().to_string(),
                );
            }
            let conflicts = engine.list_conflicts().await.map_err(map_engine_error)?;
            let mut states: Vec<_> = states.into_iter().collect();
            states.sort_by(|a, b| a.0.cmp(&b.0));
            let mut conflicts: Vec<_> = conflicts
                .into_iter()
                .map(|c| (u64::try_from(c.id).unwrap_or(0), c.path, c.renamed_local))
                .collect();
            conflicts.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
            return Ok((states, conflicts));
        }

        let states = self.states.read().await;
        let mut snapshot_states: Vec<_> = states
            .iter()
            .map(|(path, state)| (path.clone(), state.as_str().to_string()))
            .collect();
        snapshot_states.sort_by(|a, b| a.0.cmp(&b.0));
        drop(states);

        let conflicts = self.conflicts.read().await;
        let mut snapshot_conflicts: Vec<_> = conflicts
            .iter()
            .map(|c| (c.id, c.path.clone(), c.renamed_local.clone()))
            .collect();
        snapshot_conflicts.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        Ok((snapshot_states, snapshot_conflicts))
    }
}

fn map_engine_error(err: EngineError) -> zbus::fdo::Error {
    match err {
        EngineError::MissingItem(_) => map_to_fdo(DbusServiceError::NotFound),
        _ => map_to_fdo(DbusServiceError::Failed),
    }
}

pub struct ControlDbusService {
    backend: Option<Arc<SyncEngine>>,
    daemon_status: Arc<RwLock<(String, String)>>,
    auth_override: RwLock<Option<(String, String)>>,
    auth_session: RwLock<Option<AuthSession>>,
    integration_override: RwLock<Option<(String, String)>>,
}

#[derive(Clone)]
struct AuthSession {
    oauth_client: OAuthClient,
    authorize_url: String,
    redirect_uri: Option<String>,
}

impl ControlDbusService {
    pub fn daemon_status_handle() -> Arc<RwLock<(String, String)>> {
        Arc::new(RwLock::new(("running".to_string(), "idle".to_string())))
    }

    pub fn with_engine(engine: Arc<SyncEngine>) -> Self {
        Self::with_engine_and_status(engine, Self::daemon_status_handle())
    }

    pub fn with_engine_and_status(
        engine: Arc<SyncEngine>,
        daemon_status: Arc<RwLock<(String, String)>>,
    ) -> Self {
        Self {
            backend: Some(engine),
            daemon_status,
            auth_override: RwLock::new(None),
            auth_session: RwLock::new(None),
            integration_override: RwLock::new(None),
        }
    }

    async fn daemon_status_tuple(&self) -> (String, String) {
        if self.backend.is_some() {
            return self.daemon_status.read().await.clone();
        }
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

    fn oauth_client_from_env() -> Result<(OAuthClient, String), zbus::fdo::Error> {
        let client_id = std::env::var("YADISK_CLIENT_ID")
            .map_err(|_| zbus::fdo::Error::Failed("YADISK_CLIENT_ID is missing".to_string()))?;
        let client_secret = std::env::var("YADISK_CLIENT_SECRET")
            .map_err(|_| zbus::fdo::Error::Failed("YADISK_CLIENT_SECRET is missing".to_string()))?;
        let oauth_client = OAuthClient::new(client_id.clone(), client_secret)
            .map_err(|err| zbus::fdo::Error::Failed(format!("oauth client init failed: {err}")))?;
        Ok((oauth_client, client_id))
    }

    fn oauth_redirect_uri_from_env() -> Option<String> {
        let value = std::env::var("YADISK_REDIRECT_URI").ok()?;
        let value = value.trim().to_string();
        if value.is_empty() {
            return None;
        }
        Some(value)
    }

    fn manual_authorize_url(
        client_id: &str,
        redirect_uri: Option<&str>,
    ) -> Result<String, zbus::fdo::Error> {
        let mut url = Url::parse("https://oauth.yandex.ru/authorize")
            .map_err(|err| zbus::fdo::Error::Failed(format!("oauth URL build failed: {err}")))?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("response_type", "code");
            query.append_pair("client_id", client_id);
            if let Some(redirect_uri) = redirect_uri {
                query.append_pair("redirect_uri", redirect_uri);
            }
        }
        Ok(url.to_string())
    }

    async fn start_auth_session(
        &self,
        oauth_client: OAuthClient,
        client_id: &str,
    ) -> zbus::fdo::Result<String> {
        let redirect_uri = Self::oauth_redirect_uri_from_env();
        let authorize_url = Self::manual_authorize_url(client_id, redirect_uri.as_deref())?;
        {
            let mut auth_session = self.auth_session.write().await;
            *auth_session = Some(AuthSession {
                oauth_client,
                authorize_url: authorize_url.clone(),
                redirect_uri,
            });
        }
        self.set_auth_override("pending", "waiting for verification code")
            .await;
        Ok(authorize_url)
    }

    fn detect_integration_status() -> (String, String) {
        let nautilus_installed = active_nautilus_extension_dir()
            .join("libyadisk_nautilus.so")
            .is_file();
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

impl Default for ControlDbusService {
    fn default() -> Self {
        Self {
            backend: None,
            daemon_status: Self::daemon_status_handle(),
            auth_override: RwLock::new(None),
            auth_session: RwLock::new(None),
            integration_override: RwLock::new(None),
        }
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

fn active_nautilus_extension_dir() -> PathBuf {
    if let Some(path) = std::env::var_os("YADISK_NAUTILUS_EXT_DIR") {
        return PathBuf::from(path);
    }
    let output = Command::new("pkg-config")
        .arg("--variable=extensiondir")
        .arg("libnautilus-extension-4")
        .output();
    if let Ok(output) = output
        && output.status.success()
    {
        let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
        if !value.is_empty() {
            return PathBuf::from(value);
        }
    }
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        return home.join(".local/lib/nautilus/extensions-4");
    }
    PathBuf::from(".local/lib/nautilus/extensions-4")
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
                .map(|c| (u64::try_from(c.id).unwrap_or(0), c.path, c.renamed_local))
                .collect());
        }
        let conflicts = self.conflicts.read().await;
        Ok(conflicts
            .iter()
            .map(|c| (c.id, c.path.clone(), c.renamed_local.clone()))
            .collect())
    }

    async fn get_full_state(&self) -> zbus::fdo::Result<FullStateSnapshot> {
        self.full_state_snapshot().await
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

    async fn start_auth(&self) -> zbus::fdo::Result<String> {
        let (oauth_client, client_id) = Self::oauth_client_from_env()?;
        self.start_auth_session(oauth_client, &client_id).await
    }

    async fn submit_auth_code(&self, code: &str) -> zbus::fdo::Result<()> {
        let code = code.trim();
        if code.is_empty() {
            return Err(zbus::fdo::Error::Failed(
                "verification code must not be empty".to_string(),
            ));
        }
        let auth_session = self.auth_session.read().await.clone().ok_or_else(|| {
            zbus::fdo::Error::Failed(
                "auth session is not started; call StartAuth first".to_string(),
            )
        })?;
        let token = match auth_session
            .oauth_client
            .exchange_code(code, auth_session.redirect_uri.as_deref())
            .await
        {
            Ok(token) => token,
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
        {
            let mut auth_session = self.auth_session.write().await;
            *auth_session = None;
        }
        self.set_auth_override("authorized", "token saved").await;
        Ok(())
    }

    async fn cancel_auth(&self) -> zbus::fdo::Result<()> {
        {
            let mut auth_session = self.auth_session.write().await;
            *auth_session = None;
        }
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
        {
            let mut auth_session = self.auth_session.write().await;
            *auth_session = None;
        }
        self.set_auth_override("unauthorized", "token removed")
            .await;
        Ok(())
    }

    async fn get_auth_start_url(&self) -> zbus::fdo::Result<String> {
        if let Some(session) = self.auth_session.read().await.as_ref() {
            return Ok(session.authorize_url.clone());
        }
        Err(zbus::fdo::Error::Failed(
            "auth session is not started".to_string(),
        ))
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
    async fn get_full_state_returns_sorted_states_and_conflicts() {
        let service = SyncDbusService::default();
        {
            let mut states = service.states.write().await;
            states.insert("/Docs/B.txt".to_string(), PathState::Cached);
            states.insert("/Docs/A.txt".to_string(), PathState::Partial);
        }
        {
            let mut conflicts = service.conflicts.write().await;
            conflicts.push(ConflictInfo {
                id: 2,
                path: "/Docs/B.txt".to_string(),
                renamed_local: "/Docs/B (conflict).txt".to_string(),
            });
            conflicts.push(ConflictInfo {
                id: 1,
                path: "/Docs/A.txt".to_string(),
                renamed_local: "/Docs/A (conflict).txt".to_string(),
            });
        }

        let (states, conflicts) = service.get_full_state().await.unwrap();
        assert_eq!(
            states,
            vec![
                ("/Docs/A.txt".to_string(), "partial".to_string()),
                ("/Docs/B.txt".to_string(), "cached".to_string()),
            ]
        );
        assert_eq!(
            conflicts,
            vec![
                (
                    1,
                    "/Docs/A.txt".to_string(),
                    "/Docs/A (conflict).txt".to_string(),
                ),
                (
                    2,
                    "/Docs/B.txt".to_string(),
                    "/Docs/B (conflict).txt".to_string(),
                ),
            ]
        );
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

    #[test]
    fn manual_auth_url_contains_client_id() {
        let url =
            ControlDbusService::manual_authorize_url("client-1", Some("http://localhost/callback"))
                .unwrap();
        assert!(url.contains("oauth.yandex.ru/authorize"));
        assert!(url.contains("client_id=client-1"));
        assert!(url.contains("response_type=code"));
        assert!(url.contains("redirect_uri="));
    }

    #[test]
    fn manual_auth_url_without_redirect_omits_redirect_param() {
        let url = ControlDbusService::manual_authorize_url("client-1", None).unwrap();
        assert!(url.contains("oauth.yandex.ru/authorize"));
        assert!(url.contains("client_id=client-1"));
        assert!(!url.contains("redirect_uri="));
    }

    #[tokio::test]
    async fn start_auth_returns_url_and_sets_pending_state() {
        let service = ControlDbusService::default();
        let oauth_client = OAuthClient::new("test-client-id", "test-client-secret").unwrap();
        let url = service
            .start_auth_session(oauth_client, "test-client-id")
            .await
            .unwrap();
        assert!(url.contains("oauth.yandex.ru/authorize"));
        assert!(url.contains("client_id=test-client-id"));
        let (state, message) = service.get_auth_state().await.unwrap();
        assert_eq!(state, "pending");
        assert!(message.contains("verification code"));
        let session_url = service.get_auth_start_url().await.unwrap();
        assert_eq!(session_url, url);
    }

    #[tokio::test]
    async fn submit_auth_code_without_started_session_returns_error() {
        let service = ControlDbusService::default();
        let err = service
            .submit_auth_code("abc")
            .await
            .expect_err("expected missing session error");
        match err {
            zbus::fdo::Error::Failed(msg) => {
                assert!(msg.contains("StartAuth"));
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn cancel_auth_clears_started_session() {
        let service = ControlDbusService::default();
        let oauth_client = OAuthClient::new("test-client-id", "test-client-secret").unwrap();
        let _ = service
            .start_auth_session(oauth_client, "test-client-id")
            .await
            .unwrap();
        service.cancel_auth().await.unwrap();
        let err = service
            .get_auth_start_url()
            .await
            .expect_err("expected cleared auth session");
        match err {
            zbus::fdo::Error::Failed(msg) => assert!(msg.contains("not started")),
            other => panic!("unexpected error: {other:?}"),
        }
        let (state, message) = service.get_auth_state().await.unwrap();
        assert_eq!(state, "cancelled");
        assert!(message.contains("cancelled"));
    }
}
