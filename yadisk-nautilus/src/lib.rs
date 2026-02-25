use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use thiserror::Error;
use yadisk_integrations::ids::{DBUS_INTERFACE_SYNC, DBUS_NAME_SYNC, DBUS_OBJECT_PATH_SYNC};
use zbus::Message;
use zbus::blocking::{Connection, Proxy, proxy::SignalIterator};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncUiState {
    CloudOnly,
    Cached,
    Partial,
    Syncing,
    Error,
}

impl SyncUiState {
    pub fn from_dbus(value: &str) -> Self {
        match value {
            "cached" => Self::Cached,
            "partial" => Self::Partial,
            "syncing" => Self::Syncing,
            "error" => Self::Error,
            _ => Self::CloudOnly,
        }
    }

    pub fn as_dbus(self) -> &'static str {
        match self {
            Self::CloudOnly => "cloud_only",
            Self::Cached => "cached",
            Self::Partial => "partial",
            Self::Syncing => "syncing",
            Self::Error => "error",
        }
    }

    pub fn badge_label(self) -> &'static str {
        match self {
            Self::CloudOnly => "Only in cloud",
            Self::Cached => "Available offline",
            Self::Partial => "Partially available offline",
            Self::Syncing => "Syncing",
            Self::Error => "Sync error",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NautilusAction {
    SaveOffline,
    RemoveOfflineCopy,
    DownloadNow,
    RetrySync,
}

impl NautilusAction {
    pub fn id(self) -> &'static str {
        match self {
            Self::SaveOffline => "save_offline",
            Self::RemoveOfflineCopy => "remove_offline_copy",
            Self::DownloadNow => "download_now",
            Self::RetrySync => "retry_sync",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::SaveOffline => "Save Offline",
            Self::RemoveOfflineCopy => "Remove Offline Copy",
            Self::DownloadNow => "Download",
            Self::RetrySync => "Retry Sync",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MenuItemSpec {
    pub id: &'static str,
    pub label: &'static str,
    pub action: NautilusAction,
    pub is_primary: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileUiInfo {
    pub state: SyncUiState,
    pub emblem: &'static str,
    pub badge_label: &'static str,
    pub menu: Vec<MenuItemSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncSignalEvent {
    StateChanged {
        path: String,
        state: SyncUiState,
    },
    ConflictAdded {
        id: u64,
        path: String,
        renamed_local: String,
    },
}

pub fn emblem_for_state(state: SyncUiState) -> &'static str {
    match state {
        SyncUiState::CloudOnly => "cloud-outline-thin-symbolic",
        SyncUiState::Cached => "check-round-outline-symbolic",
        SyncUiState::Partial => "cloud-outline-thin-symbolic",
        SyncUiState::Syncing => "update-symbolic",
        SyncUiState::Error => "dialog-error-symbolic",
    }
}

pub fn visible_actions_for_state(state: SyncUiState) -> Vec<NautilusAction> {
    match state {
        SyncUiState::CloudOnly => vec![NautilusAction::DownloadNow],
        SyncUiState::Partial => {
            vec![
                NautilusAction::DownloadNow,
                NautilusAction::RemoveOfflineCopy,
            ]
        }
        SyncUiState::Cached => vec![NautilusAction::RemoveOfflineCopy, NautilusAction::RetrySync],
        SyncUiState::Syncing => vec![NautilusAction::RetrySync],
        SyncUiState::Error => vec![NautilusAction::RetrySync, NautilusAction::DownloadNow],
    }
}

pub fn menu_for_state(state: SyncUiState) -> Vec<MenuItemSpec> {
    visible_actions_for_state(state)
        .into_iter()
        .enumerate()
        .map(|(idx, action)| MenuItemSpec {
            id: action.id(),
            label: action.label(),
            action,
            is_primary: idx == 0,
        })
        .collect()
}

#[derive(Debug, Error)]
pub enum ExtensionError {
    #[error("dbus error: {0}")]
    Dbus(#[from] zbus::Error),
    #[error("fdo error: {0}")]
    Fdo(#[from] zbus::fdo::Error),
    #[error("path is outside sync root")]
    OutsideSyncRoot,
    #[error("unsupported signal payload: {0}")]
    UnsupportedSignal(String),
    #[error("empty remote candidate list")]
    EmptyCandidates,
}

pub struct SyncDbusClient {
    connection: Connection,
}

impl SyncDbusClient {
    pub fn connect_session() -> Result<Self, ExtensionError> {
        Ok(Self {
            connection: Connection::session()?,
        })
    }

    fn proxy(&self) -> Result<Proxy<'_>, ExtensionError> {
        Ok(Proxy::new(
            &self.connection,
            DBUS_NAME_SYNC,
            DBUS_OBJECT_PATH_SYNC,
            DBUS_INTERFACE_SYNC,
        )?)
    }

    pub fn get_state(&self, remote_path: &str) -> Result<SyncUiState, ExtensionError> {
        let proxy = self.proxy()?;
        let state: String = proxy.call("GetState", &(remote_path))?;
        Ok(SyncUiState::from_dbus(&state))
    }

    pub fn save_offline(&self, remote_path: &str) -> Result<(), ExtensionError> {
        let proxy = self.proxy()?;
        proxy.call_method("Pin", &(remote_path, true))?;
        proxy.call_method("Download", &(remote_path))?;
        Ok(())
    }

    pub fn download(&self, remote_path: &str) -> Result<(), ExtensionError> {
        let proxy = self.proxy()?;
        proxy.call_method("Download", &(remote_path))?;
        Ok(())
    }

    pub fn pin(&self, remote_path: &str) -> Result<(), ExtensionError> {
        let proxy = self.proxy()?;
        proxy.call_method("Pin", &(remote_path, true))?;
        Ok(())
    }

    pub fn remove_offline_copy(&self, remote_path: &str) -> Result<(), ExtensionError> {
        let proxy = self.proxy()?;
        proxy.call_method("Evict", &(remote_path))?;
        Ok(())
    }

    pub fn retry(&self, remote_path: &str) -> Result<(), ExtensionError> {
        let proxy = self.proxy()?;
        proxy.call_method("Retry", &(remote_path))?;
        Ok(())
    }

    pub fn perform_action(
        &self,
        remote_path: &str,
        action: NautilusAction,
    ) -> Result<(), ExtensionError> {
        match action {
            NautilusAction::SaveOffline => self.save_offline(remote_path),
            NautilusAction::RemoveOfflineCopy => self.remove_offline_copy(remote_path),
            NautilusAction::DownloadNow => self.download(remote_path),
            NautilusAction::RetrySync => self.retry(remote_path),
        }
    }

    pub fn perform_action_with_fallback(
        &self,
        remote_candidates: &[String],
        action: NautilusAction,
    ) -> Result<(), ExtensionError> {
        if remote_candidates.is_empty() {
            return Err(ExtensionError::EmptyCandidates);
        }
        let mut last_err: Option<ExtensionError> = None;
        for candidate in remote_candidates {
            match self.perform_action(candidate, action) {
                Ok(_) => return Ok(()),
                Err(err) => last_err = Some(err),
            }
        }
        Err(last_err.unwrap_or(ExtensionError::EmptyCandidates))
    }

    pub fn get_state_with_fallback(
        &self,
        remote_candidates: &[String],
    ) -> Result<SyncUiState, ExtensionError> {
        if remote_candidates.is_empty() {
            return Err(ExtensionError::EmptyCandidates);
        }
        let mut last_err: Option<ExtensionError> = None;
        for candidate in remote_candidates {
            match self.get_state(candidate) {
                Ok(state) => return Ok(state),
                Err(err) => last_err = Some(err),
            }
        }
        Err(last_err.unwrap_or(ExtensionError::EmptyCandidates))
    }

    pub fn subscribe_signals(&self) -> Result<SignalListener, ExtensionError> {
        let proxy = self.proxy()?;
        let iter = proxy.receive_all_signals()?;
        Ok(SignalListener { iter })
    }
}

pub fn map_local_to_remote_candidates(
    local_path: &Path,
    sync_root: &Path,
) -> Result<[String; 2], ExtensionError> {
    let relative = local_path
        .strip_prefix(sync_root)
        .map_err(|_| ExtensionError::OutsideSyncRoot)?;
    let suffix = relative.to_string_lossy().replace('\\', "/");
    let normalized = suffix.trim_start_matches('/');
    Ok([format!("disk:/{}", normalized), format!("/{}", normalized)])
}

pub fn map_remote_to_local_path(remote_path: &str, sync_root: &Path) -> PathBuf {
    let normalized = if let Some(rest) = remote_path.strip_prefix("disk:/") {
        format!("/{}", rest.trim_start_matches('/'))
    } else {
        remote_path.to_string()
    };
    let mut local = PathBuf::from(sync_root);
    for part in normalized.split('/').filter(|part| !part.is_empty()) {
        local.push(part);
    }
    local
}

pub struct NautilusInfoProvider {
    sync_root: PathBuf,
    client: Arc<SyncDbusClient>,
    cache: Mutex<HashMap<PathBuf, SyncUiState>>,
}

impl NautilusInfoProvider {
    pub fn new(sync_root: PathBuf, client: Arc<SyncDbusClient>) -> Self {
        Self {
            sync_root,
            client,
            cache: Mutex::new(HashMap::new()),
        }
    }

    pub fn info_for_path(&self, local_path: &Path) -> Result<FileUiInfo, ExtensionError> {
        let candidates = map_local_to_remote_candidates(local_path, &self.sync_root)?;
        let state = self.client.get_state_with_fallback(&candidates)?;
        self.cache
            .lock()
            .expect("cache lock poisoned")
            .insert(local_path.to_path_buf(), state);
        Ok(FileUiInfo {
            state,
            emblem: emblem_for_state(state),
            badge_label: state.badge_label(),
            menu: menu_for_state(state),
        })
    }

    pub fn apply_signal(&self, event: &SyncSignalEvent) {
        if let SyncSignalEvent::StateChanged { path, state } = event {
            let local = map_remote_to_local_path(path, &self.sync_root);
            self.cache
                .lock()
                .expect("cache lock poisoned")
                .insert(local, *state);
        }
    }
}

pub struct NautilusMenuProvider {
    sync_root: PathBuf,
    client: Arc<SyncDbusClient>,
}

impl NautilusMenuProvider {
    pub fn new(sync_root: PathBuf, client: Arc<SyncDbusClient>) -> Self {
        Self { sync_root, client }
    }

    pub fn menu_for_path(&self, local_path: &Path) -> Result<Vec<MenuItemSpec>, ExtensionError> {
        let candidates = map_local_to_remote_candidates(local_path, &self.sync_root)?;
        let state = self.client.get_state_with_fallback(&candidates)?;
        Ok(menu_for_state(state))
    }

    pub fn activate_action(
        &self,
        local_path: &Path,
        action: NautilusAction,
    ) -> Result<(), ExtensionError> {
        let candidates = map_local_to_remote_candidates(local_path, &self.sync_root)?;
        self.client
            .perform_action_with_fallback(&candidates, action)
    }
}

pub struct SignalListener {
    iter: SignalIterator<'static>,
}

impl SignalListener {
    pub fn next_event(&mut self) -> Result<Option<SyncSignalEvent>, ExtensionError> {
        let Some(message) = self.iter.next() else {
            return Ok(None);
        };
        parse_signal_event(&message).map(Some)
    }
}

fn parse_signal_event(message: &Message) -> Result<SyncSignalEvent, ExtensionError> {
    let member = message
        .header()
        .member()
        .map(|member| member.as_str().to_string())
        .unwrap_or_default();

    match member.as_str() {
        "StateChanged" => {
            let (path, state): (String, String) = message.body().deserialize()?;
            Ok(SyncSignalEvent::StateChanged {
                path,
                state: SyncUiState::from_dbus(&state),
            })
        }
        "ConflictAdded" => {
            let (id, path, renamed_local): (u64, String, String) = message.body().deserialize()?;
            Ok(SyncSignalEvent::ConflictAdded {
                id,
                path,
                renamed_local,
            })
        }
        other => Err(ExtensionError::UnsupportedSignal(other.to_string())),
    }
}

#[cfg(feature = "nautilus-plugin")]
mod nautilus_plugin;

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;
