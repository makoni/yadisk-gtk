use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use tokio::sync::mpsc;
use yadisk_core::{ApiErrorClass, DiskInfo, OAuthClient, YadiskClient};
use yadisk_integrations::ids::{DBUS_NAME_SYNC, DBUS_OBJECT_PATH_SYNC};
use zbus::connection::Builder as ConnectionBuilder;
use zbus::object_server::SignalEmitter;

use crate::dbus_api::SyncDbusService;
use crate::oauth_flow::OAuthFlow;
use crate::storage::{OAuthState, TokenStorage};
use crate::sync::engine::SyncEngine;
use crate::sync::index::{FileState, IndexStore};
use crate::sync::local_watcher::{LocalEvent, start_notify_watcher};
use crate::token_provider::TokenProvider;
use crate::tray::{TraySyncState, start_status_tray};

const DEFAULT_SYNC_DIR_NAME: &str = "Yandex Disk";
const DEFAULT_REMOTE_ROOT: &str = "disk:/";
const DEFAULT_CLOUD_POLL_SECS: u64 = 15;
const DEFAULT_WORKER_LOOP_MS: u64 = 500;
const DEFAULT_EVICTION_SECS: u64 = 60;
const DEFAULT_CACHE_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct DaemonConfig {
    pub sync_root: PathBuf,
    pub cache_root: PathBuf,
    pub remote_root: String,
    pub cloud_poll_interval: Duration,
    pub worker_interval: Duration,
    pub eviction_interval: Duration,
    pub cache_max_bytes: u64,
    pub enable_local_watcher: bool,
}

impl DaemonConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let home = dirs::home_dir().context("home directory is unavailable")?;
        let default_sync = home.join(DEFAULT_SYNC_DIR_NAME);
        let sync_root = std::env::var("YADISK_SYNC_DIR")
            .ok()
            .map(|value| expand_with_home(&value, &home))
            .unwrap_or(default_sync);
        let cache_root = std::env::var("YADISK_CACHE_DIR")
            .ok()
            .map(|value| expand_with_home(&value, &home))
            .unwrap_or_else(default_cache_root);
        let remote_root =
            std::env::var("YADISK_REMOTE_ROOT").unwrap_or_else(|_| DEFAULT_REMOTE_ROOT.to_string());
        let cloud_poll_interval = Duration::from_secs(read_u64_env(
            "YADISK_CLOUD_POLL_SECS",
            DEFAULT_CLOUD_POLL_SECS,
        ));
        let worker_interval = Duration::from_millis(read_u64_env(
            "YADISK_WORKER_LOOP_MS",
            DEFAULT_WORKER_LOOP_MS,
        ));
        let eviction_interval =
            Duration::from_secs(read_u64_env("YADISK_EVICTION_SECS", DEFAULT_EVICTION_SECS));
        let cache_max_bytes = read_u64_env("YADISK_CACHE_MAX_BYTES", DEFAULT_CACHE_MAX_BYTES);
        let enable_local_watcher = read_bool_env("YADISK_ENABLE_LOCAL_WATCHER", false);

        Ok(Self {
            sync_root,
            cache_root,
            remote_root,
            cloud_poll_interval,
            worker_interval,
            eviction_interval,
            cache_max_bytes,
            enable_local_watcher,
        })
    }
}

pub struct DaemonRuntime {
    config: DaemonConfig,
    engine: Arc<SyncEngine>,
}

impl DaemonRuntime {
    pub async fn bootstrap(config: DaemonConfig) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(&config.sync_root)
            .await
            .with_context(|| format!("failed to create sync root at {:?}", config.sync_root))?;
        tokio::fs::create_dir_all(&config.cache_root)
            .await
            .with_context(|| format!("failed to create cache root at {:?}", config.cache_root))?;

        let token = resolve_valid_token(None).await?;
        let client = YadiskClient::new(token)?;
        let index = IndexStore::new_default()
            .await
            .context("failed to initialize index store")?;
        let engine = Arc::new(SyncEngine::new(client, index, config.cache_root.clone()));

        Ok(Self { config, engine })
    }

    pub async fn run(self) -> anyhow::Result<()> {
        eprintln!(
            "[yadiskd] started: sync_root={}, remote_root={}, local_watcher={}",
            self.config.sync_root.display(),
            self.config.remote_root,
            if self.config.enable_local_watcher {
                "enabled"
            } else {
                "disabled (metadata-only default)"
            }
        );

        let dbus_connection = ConnectionBuilder::session()?
            .name(DBUS_NAME_SYNC)?
            .serve_at(
                DBUS_OBJECT_PATH_SYNC,
                SyncDbusService::with_engine(Arc::clone(&self.engine)),
            )?
            .build()
            .await
            .context("failed to start D-Bus object server")?;

        let (quit_tx, mut quit_rx) = mpsc::unbounded_channel::<()>();
        let (tray_state_tx, mut tray_state_rx) = mpsc::unbounded_channel::<TraySyncState>();
        let tray_controller = match start_status_tray(quit_tx.clone()) {
            Ok(controller) => controller,
            Err(err) => {
                eprintln!("[yadiskd] warning: failed to start status tray: {err}");
                None
            }
        };
        let tray_handle = tray_controller.map(|controller| {
            tokio::spawn(async move {
                while let Some(state) = tray_state_rx.recv().await {
                    controller.update(state);
                }
            })
        });
        let _ = tray_state_tx.send(TraySyncState::Syncing);

        let (watcher, mut local_rx): (
            Option<notify::RecommendedWatcher>,
            Option<mpsc::UnboundedReceiver<LocalEvent>>,
        ) = if self.config.enable_local_watcher {
            match start_notify_watcher(&self.config.sync_root).ok() {
                Some((watcher, rx)) => (Some(watcher), Some(rx)),
                None => {
                    eprintln!("[yadiskd] warning: failed to start local watcher");
                    (None, None)
                }
            }
        } else {
            (None, None)
        };

        let engine_for_cloud = Arc::clone(&self.engine);
        let remote_root = self.config.remote_root.clone();
        let cloud_poll_interval = self.config.cloud_poll_interval;
        let tray_state_tx_cloud = tray_state_tx.clone();
        let cloud_handle = tokio::spawn(async move {
            loop {
                match engine_for_cloud
                    .sync_directory_incremental(&remote_root)
                    .await
                {
                    Ok(delta) => {
                        if delta.indexed > 0 || delta.deleted > 0 || delta.enqueued_downloads > 0 {
                            eprintln!(
                                "[yadiskd] cloud delta: indexed={}, deleted={}, enqueued_downloads={}",
                                delta.indexed, delta.deleted, delta.enqueued_downloads
                            );
                        }
                    }
                    Err(err) => {
                        eprintln!("[yadiskd] cloud sync error: {err}");
                        let _ = tray_state_tx_cloud.send(TraySyncState::Error);
                    }
                }
                tokio::time::sleep(cloud_poll_interval).await;
            }
        });

        let engine_for_worker = Arc::clone(&self.engine);
        let worker_interval = self.config.worker_interval;
        let tray_state_tx_worker = tray_state_tx.clone();
        let worker_handle = tokio::spawn(async move {
            loop {
                match engine_for_worker.run_once().await {
                    Ok(true) => {
                        eprintln!("[yadiskd] worker: processed queued operation");
                        let _ = tray_state_tx_worker.send(TraySyncState::Syncing);
                    }
                    Ok(false) => {}
                    Err(err) => {
                        eprintln!("[yadiskd] worker error: {err}");
                        let _ = tray_state_tx_worker.send(TraySyncState::Error);
                    }
                }
                tokio::time::sleep(worker_interval).await;
            }
        });

        let engine_for_materialize = Arc::clone(&self.engine);
        let materialize_sync_root = self.config.sync_root.clone();
        let materialize_cache_root = self.config.cache_root.clone();
        let materialize_remote_root = self.config.remote_root.clone();
        let materialize_handle = tokio::spawn(async move {
            let mut initial_logged = false;
            let mut materialize_enabled = true;
            loop {
                if !materialize_enabled {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
                match materialize_sync_tree(
                    &engine_for_materialize,
                    &materialize_sync_root,
                    &materialize_cache_root,
                    &materialize_remote_root,
                )
                .await
                {
                    Ok(total_items) if !initial_logged => {
                        eprintln!("[yadiskd] metadata tree initialized: {total_items} entries");
                        initial_logged = true;
                    }
                    Ok(_) => {}
                    Err(err) => {
                        if error_contains_enosys(&err) {
                            eprintln!(
                                "[yadiskd] materialization disabled: filesystem does not support required write operations"
                            );
                            materialize_enabled = false;
                        } else {
                            eprintln!("[yadiskd] materialize error: {err}");
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });

        let engine_for_eviction = Arc::clone(&self.engine);
        let eviction_root = self.config.remote_root.clone();
        let cache_root = self.config.cache_root.clone();
        let cache_max_bytes = self.config.cache_max_bytes;
        let eviction_interval = self.config.eviction_interval;
        let eviction_handle = tokio::spawn(async move {
            loop {
                let _ = run_cache_eviction_once(
                    &engine_for_eviction,
                    &cache_root,
                    &eviction_root,
                    cache_max_bytes,
                )
                .await;
                tokio::time::sleep(eviction_interval).await;
            }
        });

        let signal_emitter = SignalEmitter::new(&dbus_connection, DBUS_OBJECT_PATH_SYNC)
            .context("failed to create D-Bus signal emitter")?
            .into_owned();
        let engine_for_signals = Arc::clone(&self.engine);
        let signal_root = self.config.remote_root.clone();
        let tray_state_tx_signal = tray_state_tx.clone();
        let signal_handle = tokio::spawn(async move {
            let mut known_states: HashMap<String, &'static str> = HashMap::new();
            let mut known_tray_state: Option<TraySyncState> = None;
            let mut last_conflict_id = 0i64;

            loop {
                if let Ok(states) = engine_for_signals.list_states_by_prefix(&signal_root).await {
                    let mut current_states = HashMap::with_capacity(states.len());
                    for (path, state) in states {
                        let state_str = match state {
                            crate::sync::index::FileState::CloudOnly => "cloud_only",
                            crate::sync::index::FileState::Cached => "cached",
                            crate::sync::index::FileState::Syncing => "syncing",
                            crate::sync::index::FileState::Error => "error",
                        };
                        let changed = known_states
                            .get(path.as_str())
                            .map(|existing| *existing != state_str)
                            .unwrap_or(true);
                        current_states.insert(path.clone(), state_str);
                        if changed {
                            let _ =
                                SyncDbusService::state_changed(&signal_emitter, &path, state_str)
                                    .await;
                        }
                    }
                    let tray_state = tray_state_from_states(&current_states);
                    if known_tray_state != Some(tray_state) {
                        let _ = tray_state_tx_signal.send(tray_state);
                        known_tray_state = Some(tray_state);
                    }
                    known_states = current_states;
                } else {
                    let _ = tray_state_tx_signal.send(TraySyncState::Error);
                }

                if let Ok(conflicts) = engine_for_signals.list_conflicts().await {
                    for conflict in conflicts {
                        if conflict.id <= last_conflict_id {
                            continue;
                        }
                        let _ = SyncDbusService::conflict_added(
                            &signal_emitter,
                            conflict.id as u64,
                            &conflict.path,
                            &conflict.renamed_local,
                        )
                        .await;
                        last_conflict_id = conflict.id;
                    }
                }

                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });

        let local_handle = if let Some(mut rx) = local_rx.take() {
            let engine_for_local = Arc::clone(&self.engine);
            Some(tokio::spawn(async move {
                while let Some(event) = rx.recv().await {
                    eprintln!("[yadiskd] local event: {:?}", event);
                    if let Err(err) = engine_for_local.ingest_local_event(event).await {
                        eprintln!("[yadiskd] local ingest error: {err}");
                    }
                }
            }))
        } else {
            None
        };

        let _watcher = watcher;
        tokio::select! {
            res = tokio::signal::ctrl_c() => {
                res.context("failed waiting for shutdown signal")?;
            }
            _ = quit_rx.recv() => {
                eprintln!("[yadiskd] quit requested from status tray");
            }
        }

        cloud_handle.abort();
        worker_handle.abort();
        materialize_handle.abort();
        eviction_handle.abort();
        signal_handle.abort();
        if let Some(handle) = local_handle {
            handle.abort();
        }
        if let Some(handle) = tray_handle {
            handle.abort();
        }

        Ok(())
    }
}

fn tray_state_from_states(states: &HashMap<String, &'static str>) -> TraySyncState {
    let mut has_syncing = false;
    for state in states.values() {
        if *state == "error" {
            return TraySyncState::Error;
        }
        if *state == "syncing" {
            has_syncing = true;
        }
    }
    if has_syncing {
        TraySyncState::Syncing
    } else {
        TraySyncState::Normal
    }
}

async fn resolve_valid_token(base_url: Option<&str>) -> anyhow::Result<String> {
    match std::env::var("YADISK_TOKEN") {
        Ok(token) => Ok(token),
        Err(_) => {
            let storage = TokenStorage::new()
                .await
                .context("failed to initialize token storage")?;
            let state = match storage.get_oauth_state() {
                Ok(state) => state,
                Err(_) => authenticate_and_store(&storage).await?,
            };
            let oauth_client = oauth_client_from_env(base_url)?;
            let mut provider = TokenProvider::new(state, oauth_client);
            let info = fetch_disk_info_with_retry(&mut provider, base_url)
                .await
                .context("failed to fetch disk info")?;
            let _ = info;
            storage
                .save_oauth_state(provider.state())
                .context("failed to persist oauth state")?;
            Ok(provider.state().access_token.clone())
        }
    }
}

async fn authenticate_and_store(storage: &TokenStorage) -> anyhow::Result<OAuthState> {
    let client_id = std::env::var("YADISK_CLIENT_ID").context("YADISK_CLIENT_ID is not set")?;
    let client_secret =
        std::env::var("YADISK_CLIENT_SECRET").context("YADISK_CLIENT_SECRET is not set")?;
    let flow = OAuthFlow::new(client_id, client_secret);
    let token = flow.authenticate().await?;
    let state = OAuthState::from_oauth_token(&token);
    storage
        .save_oauth_state(&state)
        .context("failed to save token")?;
    Ok(state)
}

fn oauth_client_from_env(base_url: Option<&str>) -> anyhow::Result<Option<OAuthClient>> {
    match (
        std::env::var("YADISK_CLIENT_ID"),
        std::env::var("YADISK_CLIENT_SECRET"),
    ) {
        (Ok(client_id), Ok(client_secret)) => Ok(Some(match base_url {
            Some(url) => OAuthClient::with_base_url(url, client_id, client_secret)
                .context("invalid oauth base url/config")?,
            None => OAuthClient::new(client_id, client_secret).context("invalid oauth config")?,
        })),
        _ => Ok(None),
    }
}

async fn fetch_disk_info_with_retry(
    provider: &mut TokenProvider,
    base_url: Option<&str>,
) -> anyhow::Result<DiskInfo> {
    let token = provider
        .valid_access_token()
        .await
        .context("failed to resolve valid access token")?;
    let client = build_client(base_url, &token)?;
    match client.get_disk_info().await {
        Ok(info) => Ok(info),
        Err(err) if matches!(err.classification(), Some(ApiErrorClass::Auth)) => {
            let refreshed = provider
                .refresh_now()
                .await
                .context("failed to refresh token after 401")?;
            let retry_client = build_client(base_url, &refreshed)?;
            Ok(retry_client.get_disk_info().await?)
        }
        Err(err) => Err(err.into()),
    }
}

fn build_client(
    base_url: Option<&str>,
    token: &str,
) -> Result<YadiskClient, yadisk_core::YadiskError> {
    match base_url {
        Some(url) => YadiskClient::with_base_url(url, token.to_string()),
        None => YadiskClient::new(token.to_string()),
    }
}

fn expand_with_home(value: &str, home: &Path) -> PathBuf {
    if value == "~" {
        return home.to_path_buf();
    }
    if let Some(rest) = value.strip_prefix("~/") {
        return home.join(rest);
    }
    PathBuf::from(value)
}

fn default_cache_root() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join("yadisk-gtk")
}

fn read_u64_env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn read_bool_env(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

async fn materialize_sync_tree(
    engine: &SyncEngine,
    sync_root: &Path,
    cache_root: &Path,
    remote_root: &str,
) -> anyhow::Result<usize> {
    let items = engine.list_items_by_prefix(remote_root).await?;
    let states: HashMap<_, _> = engine
        .list_states_by_prefix(remote_root)
        .await?
        .into_iter()
        .collect();
    let mut touched_dirs = HashSet::new();
    touched_dirs.insert(sync_root.to_path_buf());

    for item in &items {
        let local_path = sync_path_for(sync_root, &item.path)?;
        if item.item_type == crate::sync::index::ItemType::Dir {
            tokio::fs::create_dir_all(&local_path).await?;
            touched_dirs.insert(local_path);
            continue;
        }

        if let Some(parent) = local_path.parent()
            && !touched_dirs.contains(parent)
        {
            tokio::fs::create_dir_all(parent).await?;
            touched_dirs.insert(parent.to_path_buf());
        }
        let state = state_for_path(&states, &item.path);
        if matches!(state, Some(FileState::Cached)) {
            let cache_path = crate::sync::paths::cache_path_for(cache_root, &item.path)?;
            if tokio::fs::try_exists(&cache_path).await? {
                match tokio::fs::copy(&cache_path, &local_path).await {
                    Ok(_) => continue,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => return Err(err.into()),
                }
            }
        }

        if tokio::fs::try_exists(&local_path).await? {
            if matches!(state, Some(FileState::CloudOnly)) {
                let file = tokio::fs::OpenOptions::new()
                    .write(true)
                    .open(&local_path)
                    .await?;
                file.set_len(0).await?;
            }
            continue;
        }

        match tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&local_path)
            .await
        {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(err) => return Err(err.into()),
        }
    }

    Ok(items.len())
}

fn state_for_path(states: &HashMap<String, FileState>, path: &str) -> Option<FileState> {
    if let Some(state) = states.get(path) {
        return Some(state.clone());
    }
    if let Some(rest) = path.strip_prefix("disk:/") {
        let slash = format!("/{}", rest.trim_start_matches('/'));
        return states.get(&slash).cloned();
    }
    if let Some(rest) = path.strip_prefix('/') {
        let disk = format!("disk:/{}", rest);
        return states.get(&disk).cloned();
    }
    None
}

fn error_contains_enosys(err: &anyhow::Error) -> bool {
    err.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::raw_os_error)
            == Some(38)
    })
}

fn sync_path_for(sync_root: &Path, remote_path: &str) -> anyhow::Result<PathBuf> {
    let normalized = if let Some(rest) = remote_path.strip_prefix("disk:/") {
        format!("/{}", rest.trim_start_matches('/'))
    } else {
        remote_path.to_string()
    };

    if !normalized.starts_with('/') {
        anyhow::bail!("remote path must be absolute: {remote_path}");
    }

    let mut local = PathBuf::from(sync_root);
    for component in Path::new(&normalized).components() {
        match component {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(part) => local.push(part),
            Component::ParentDir => anyhow::bail!("parent path is not allowed: {remote_path}"),
            Component::Prefix(_) => anyhow::bail!("unsupported prefix in path: {remote_path}"),
        }
    }
    Ok(local)
}

async fn run_cache_eviction_once(
    engine: &SyncEngine,
    cache_root: &Path,
    remote_root: &str,
    max_bytes: u64,
) -> anyhow::Result<()> {
    let mut candidates = Vec::new();
    let mut total_bytes = 0u64;

    for (path, state, pinned) in engine
        .list_path_states_with_pin_by_prefix(remote_root)
        .await?
    {
        if pinned || !matches!(state, crate::sync::index::FileState::Cached) {
            continue;
        }
        let local_path = crate::sync::paths::cache_path_for(cache_root, &path)?;
        let Ok(metadata) = tokio::fs::metadata(&local_path).await else {
            continue;
        };
        let size = metadata.len();
        total_bytes = total_bytes.saturating_add(size);
        let modified = metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);
        candidates.push((path, local_path, size, modified));
    }

    if total_bytes <= max_bytes {
        return Ok(());
    }

    candidates.sort_by_key(|entry| entry.3);
    for (path, local_path, size, _) in candidates {
        if total_bytes <= max_bytes {
            break;
        }
        if tokio::fs::remove_file(&local_path).await.is_ok() {
            total_bytes = total_bytes.saturating_sub(size);
            let _ = engine.evict_path(&path).await;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::OAuthState;
    use crate::sync::index::{FileState, IndexStore, ItemInput, ItemType};
    use sqlx::SqlitePool;
    use tempfile::tempdir;
    use wiremock::matchers::{body_string_contains, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn expands_tilde_to_home_sync_dir() {
        let home = PathBuf::from("/tmp/home-user");
        assert_eq!(
            expand_with_home("~/Yandex Disk", &home),
            PathBuf::from("/tmp/home-user/Yandex Disk")
        );
    }

    #[test]
    fn reads_intervals_from_env_or_default() {
        assert_eq!(read_u64_env("NO_SUCH_ENV_FOR_TEST", 42), 42);
    }

    #[test]
    fn local_watcher_is_disabled_by_default() {
        assert!(!read_bool_env("NO_SUCH_BOOL_ENV_FOR_TEST", false));
    }

    #[test]
    fn detects_enosys_in_error_chain() {
        let err = anyhow::Error::new(std::io::Error::from_raw_os_error(38));
        assert!(error_contains_enosys(&err));
    }

    #[test]
    fn tray_state_prioritizes_error_then_syncing() {
        let mut states = HashMap::new();
        assert_eq!(tray_state_from_states(&states), TraySyncState::Normal);

        states.insert("/A".to_string(), "syncing");
        assert_eq!(tray_state_from_states(&states), TraySyncState::Syncing);

        states.insert("/B".to_string(), "cached");
        assert_eq!(tray_state_from_states(&states), TraySyncState::Syncing);

        states.insert("/C".to_string(), "error");
        assert_eq!(tray_state_from_states(&states), TraySyncState::Error);
    }

    #[tokio::test]
    async fn retries_once_after_unauthorized_with_refreshed_token() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/disk"))
            .and(header("authorization", "OAuth old-token"))
            .respond_with(ResponseTemplate::new(401).set_body_string("unauthorized"))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .and(body_string_contains("grant_type=refresh_token"))
            .and(body_string_contains("refresh_token=refresh-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "new-token",
                "token_type": "bearer",
                "expires_in": 3600,
                "refresh_token": "refresh-2",
                "scope": "disk:read"
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/disk"))
            .and(header("authorization", "OAuth new-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "total_space": 1000,
                "used_space": 100,
                "trash_size": 0,
                "is_paid": false
            })))
            .mount(&server)
            .await;

        let oauth_client =
            OAuthClient::with_base_url(&server.uri(), "client-id", "secret").expect("oauth client");
        let mut provider = TokenProvider::new(
            OAuthState {
                access_token: "old-token".into(),
                refresh_token: Some("refresh-1".into()),
                expires_at: Some(i64::MAX),
                scope: Some("disk:read".into()),
                token_type: Some("bearer".into()),
            },
            Some(oauth_client),
        );

        let info = fetch_disk_info_with_retry(&mut provider, Some(&server.uri()))
            .await
            .expect("retry should succeed");
        assert_eq!(info.total_space, 1000);
        assert_eq!(provider.state().access_token, "new-token");
    }

    #[tokio::test]
    async fn cache_eviction_skips_pinned_files() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let index = IndexStore::from_pool(pool);
        index.init().await.unwrap();
        let cache_dir = tempdir().unwrap();

        for path in ["/A.txt", "/B.txt", "/C.txt"] {
            let item = index
                .upsert_item(&ItemInput {
                    path: path.to_string(),
                    parent_path: Some("/".to_string()),
                    name: path.trim_start_matches('/').to_string(),
                    item_type: ItemType::File,
                    size: Some(5),
                    modified: None,
                    hash: None,
                    resource_id: None,
                    last_synced_hash: None,
                    last_synced_modified: None,
                })
                .await
                .unwrap();
            let pinned = path == "/B.txt";
            index
                .set_state(item.id, FileState::Cached, pinned, None)
                .await
                .unwrap();
            let local = crate::sync::paths::cache_path_for(cache_dir.path(), path).unwrap();
            if let Some(parent) = local.parent() {
                tokio::fs::create_dir_all(parent).await.unwrap();
            }
            tokio::fs::write(local, b"12345").await.unwrap();
        }

        let client = YadiskClient::with_base_url("http://127.0.0.1:9", "token").unwrap();
        let engine = SyncEngine::new(client, index, cache_dir.path().to_path_buf());
        run_cache_eviction_once(&engine, cache_dir.path(), "/", 8)
            .await
            .unwrap();

        let states = engine
            .list_path_states_with_pin_by_prefix("/")
            .await
            .unwrap();
        let unpinned_cached = states
            .iter()
            .filter(|(_, state, pinned)| !*pinned && matches!(state, FileState::Cached))
            .count();
        assert_eq!(unpinned_cached, 1);
        assert!(
            tokio::fs::metadata(cache_dir.path().join("B.txt"))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn materialize_creates_placeholder_tree_from_index() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let index = IndexStore::from_pool(pool);
        index.init().await.unwrap();
        let sync_dir = tempdir().unwrap();

        index
            .upsert_item(&ItemInput {
                path: "/Docs".into(),
                parent_path: Some("/".into()),
                name: "Docs".into(),
                item_type: ItemType::Dir,
                size: None,
                modified: None,
                hash: None,
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();
        index
            .upsert_item(&ItemInput {
                path: "/Docs/A.txt".into(),
                parent_path: Some("/Docs".into()),
                name: "A.txt".into(),
                item_type: ItemType::File,
                size: Some(123),
                modified: None,
                hash: None,
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();

        let client = YadiskClient::with_base_url("http://127.0.0.1:9", "token").unwrap();
        let engine = SyncEngine::new(client, index, sync_dir.path().to_path_buf());
        let count = materialize_sync_tree(&engine, sync_dir.path(), sync_dir.path(), "/")
            .await
            .unwrap();

        assert_eq!(count, 2);
        assert!(
            tokio::fs::metadata(sync_dir.path().join("Docs"))
                .await
                .is_ok()
        );
        assert!(
            tokio::fs::metadata(sync_dir.path().join("Docs/A.txt"))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn materialize_copies_cached_file_to_sync_tree() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let index = IndexStore::from_pool(pool);
        index.init().await.unwrap();
        let sync_dir = tempdir().unwrap();
        let cache_dir = tempdir().unwrap();

        let item = index
            .upsert_item(&ItemInput {
                path: "/Docs/A.txt".into(),
                parent_path: Some("/Docs".into()),
                name: "A.txt".into(),
                item_type: ItemType::File,
                size: Some(5),
                modified: None,
                hash: None,
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();
        index
            .set_state(item.id, FileState::Cached, true, None)
            .await
            .unwrap();

        let cache_path =
            crate::sync::paths::cache_path_for(cache_dir.path(), "/Docs/A.txt").unwrap();
        tokio::fs::create_dir_all(cache_path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&cache_path, b"hello").await.unwrap();

        let client = YadiskClient::with_base_url("http://127.0.0.1:9", "token").unwrap();
        let engine = SyncEngine::new(client, index, cache_dir.path().to_path_buf());
        materialize_sync_tree(&engine, sync_dir.path(), cache_dir.path(), "/")
            .await
            .unwrap();

        let local = sync_dir.path().join("Docs/A.txt");
        assert_eq!(tokio::fs::read(local).await.unwrap(), b"hello");
    }
}
