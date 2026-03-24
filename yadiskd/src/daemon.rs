use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Context;
use md5::Context as Md5Context;
use tokio::io::AsyncReadExt;
use tokio::sync::{Mutex as AsyncMutex, mpsc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use yadisk_core::YadiskClient;
#[cfg(test)]
use yadisk_core::{ApiErrorClass, DiskInfo, OAuthClient};
use yadisk_integrations::ids::{DBUS_NAME_SYNC, DBUS_OBJECT_PATH_CONTROL, DBUS_OBJECT_PATH_SYNC};
use zbus::connection::Builder as ConnectionBuilder;
use zbus::object_server::SignalEmitter;

use crate::dbus_api::{ControlDbusService, SyncDbusService};
use crate::storage::TokenStorage;
use crate::sync::engine::{EngineError, SyncEngine};
use crate::sync::index::{FileState, IndexStore};
use crate::sync::local_watcher::{LocalEvent, start_notify_watcher};
use crate::token_provider::TokenProvider;
use crate::tray::{TraySyncState, start_status_tray};
use yadisk_integrations::preferences::{load_ui_preferences, resolve_effective_language};

pub(crate) const DEFAULT_SYNC_DIR_NAME: &str = "Yandex Disk";
const DEFAULT_REMOTE_ROOT: &str = "disk:/";
const DEFAULT_CLOUD_POLL_SECS: u64 = 15;
const DEFAULT_WORKER_LOOP_MS: u64 = 500;
const DEFAULT_EVICTION_SECS: u64 = 60;
const DEFAULT_CACHE_MAX_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const SHUTDOWN_TASK_TIMEOUT: Duration = Duration::from_secs(5);

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
        let sync_root = resolve_sync_root_from_env()?;
        let home = dirs::home_dir().context("home directory is unavailable")?;
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
        let enable_local_watcher = read_bool_env("YADISK_ENABLE_LOCAL_WATCHER", true);

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

pub(crate) fn resolve_sync_root_from_env() -> anyhow::Result<PathBuf> {
    let home = dirs::home_dir().context("home directory is unavailable")?;
    Ok(resolve_sync_root_path(
        std::env::var("YADISK_SYNC_DIR").ok(),
        &home,
    ))
}

pub(crate) fn resolve_sync_root_path(value: Option<String>, home: &Path) -> PathBuf {
    value
        .filter(|path| !path.trim().is_empty())
        .map(|path| expand_with_home(&path, home))
        .unwrap_or_else(|| home.join(DEFAULT_SYNC_DIR_NAME))
}

pub struct DaemonRuntime {
    config: DaemonConfig,
    engine: Arc<SyncEngine>,
    auth_ready: Arc<AtomicBool>,
}

impl DaemonRuntime {
    pub async fn bootstrap(config: DaemonConfig) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(&config.sync_root)
            .await
            .with_context(|| format!("failed to create sync root at {:?}", config.sync_root))?;
        tokio::fs::create_dir_all(&config.cache_root)
            .await
            .with_context(|| format!("failed to create cache root at {:?}", config.cache_root))?;

        let oauth_state = resolve_oauth_state().await?;
        let auth_ready = Arc::new(AtomicBool::new(!oauth_state.access_token.trim().is_empty()));
        let client = YadiskClient::new(oauth_state.access_token.clone())?;
        let token_provider = Arc::new(AsyncMutex::new(TokenProvider::new(
            oauth_state,
            oauth_client_from_env(),
        )));
        let index = IndexStore::new_default()
            .await
            .context("failed to initialize index store")?;
        let engine = Arc::new(
            SyncEngine::new(client, index, config.cache_root.clone())
                .with_token_provider(token_provider),
        );

        Ok(Self {
            config,
            engine,
            auth_ready,
        })
    }

    pub async fn run(self) -> anyhow::Result<()> {
        eprintln!(
            "[yadiskd] started: sync_root={}, remote_root={}, local_watcher={}, auth={}",
            self.config.sync_root.display(),
            self.config.remote_root,
            if self.config.enable_local_watcher {
                "enabled"
            } else {
                "disabled"
            },
            if self.auth_ready.load(Ordering::SeqCst) {
                "ready"
            } else {
                "waiting_for_auth"
            }
        );

        let daemon_status = ControlDbusService::daemon_status_handle();
        let dbus_connection = ConnectionBuilder::session()?
            .name(DBUS_NAME_SYNC)?
            .serve_at(
                DBUS_OBJECT_PATH_SYNC,
                SyncDbusService::with_engine(Arc::clone(&self.engine)),
            )?
            .serve_at(
                DBUS_OBJECT_PATH_CONTROL,
                ControlDbusService::with_engine_and_status(
                    Arc::clone(&self.engine),
                    Arc::clone(&daemon_status),
                ),
            )?
            .build()
            .await
            .context("failed to start D-Bus object server")?;

        let (quit_tx, mut quit_rx) = mpsc::unbounded_channel::<()>();
        let (tray_state_tx, mut tray_state_rx) = mpsc::unbounded_channel::<TraySyncState>();
        let shutdown = CancellationToken::new();
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
        let _ = tray_state_tx.send(TraySyncState::Normal);
        let local_events_enabled = Arc::new(AtomicBool::new(false));
        let sync_root_available = Arc::new(AtomicBool::new(
            is_sync_root_available(&self.config.sync_root).await,
        ));
        let sync_root_generation = Arc::new(AtomicU64::new(0));
        let materialize_refresh_requested = Arc::new(AtomicBool::new(false));
        let cloud_sync_error = Arc::new(AtomicBool::new(false));
        let cloud_space_low = Arc::new(AtomicBool::new(false));
        let network_available = Arc::new(AtomicBool::new(true));
        if !sync_root_available.load(Ordering::SeqCst) {
            eprintln!(
                "[yadiskd] sync root is unavailable: {}",
                self.config.sync_root.display()
            );
        }

        let (local_tx, local_rx) = mpsc::unbounded_channel::<LocalEvent>();
        let watcher_handle = if self.config.enable_local_watcher {
            let watcher_sync_root = self.config.sync_root.clone();
            let sync_root_available_watcher = Arc::clone(&sync_root_available);
            let sync_root_generation_watcher = Arc::clone(&sync_root_generation);
            let shutdown_watcher = shutdown.child_token();
            Some(tokio::spawn(async move {
                let mut watcher: Option<(
                    notify::RecommendedWatcher,
                    mpsc::UnboundedReceiver<LocalEvent>,
                )> = None;
                let mut watcher_generation = sync_root_generation_watcher.load(Ordering::SeqCst);
                let mut warned = false;
                loop {
                    if shutdown_watcher.is_cancelled() {
                        break;
                    }
                    let current_generation = sync_root_generation_watcher.load(Ordering::SeqCst);
                    if current_generation != watcher_generation {
                        watcher = None;
                        watcher_generation = current_generation;
                    }
                    if !sync_root_available_watcher.load(Ordering::SeqCst) {
                        watcher = None;
                        warned = false;
                        if sleep_or_shutdown(&shutdown_watcher, Duration::from_secs(1)).await {
                            break;
                        }
                        continue;
                    }
                    if watcher.is_none() {
                        match start_notify_watcher(&watcher_sync_root) {
                            Ok(bundle) => {
                                watcher = Some(bundle);
                                warned = false;
                            }
                            Err(err) => {
                                if !warned {
                                    eprintln!(
                                        "[yadiskd] warning: failed to start local watcher: {err}"
                                    );
                                    warned = true;
                                }
                                if sleep_or_shutdown(&shutdown_watcher, Duration::from_secs(2))
                                    .await
                                {
                                    break;
                                }
                                continue;
                            }
                        }
                    }
                    let Some((_, rx)) = watcher.as_mut() else {
                        continue;
                    };
                    tokio::select! {
                        _ = shutdown_watcher.cancelled() => break,
                        event = rx.recv() => match event {
                            Some(event) => {
                                let _ = local_tx.send(event);
                            }
                            None => {
                                watcher = None;
                            }
                        },
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {
                            if !sync_root_available_watcher.load(Ordering::SeqCst)
                                || sync_root_generation_watcher.load(Ordering::SeqCst)
                                    != watcher_generation
                            {
                                watcher = None;
                            }
                        }
                    }
                }
            }))
        } else {
            None
        };

        let engine_for_cloud = Arc::clone(&self.engine);
        let remote_root = self.config.remote_root.clone();
        let cloud_poll_interval = self.config.cloud_poll_interval;
        let sync_root_available_cloud = Arc::clone(&sync_root_available);
        let cloud_sync_error_cloud = Arc::clone(&cloud_sync_error);
        let cloud_space_low_cloud = Arc::clone(&cloud_space_low);
        let network_available_cloud = Arc::clone(&network_available);
        let auth_ready_cloud = Arc::clone(&self.auth_ready);
        let shutdown_cloud = shutdown.child_token();
        let cloud_handle = tokio::spawn(async move {
            loop {
                if shutdown_cloud.is_cancelled() {
                    break;
                }
                if !auth_ready_cloud.load(Ordering::SeqCst)
                    || !sync_root_available_cloud.load(Ordering::SeqCst)
                {
                    if sleep_or_shutdown(&shutdown_cloud, cloud_poll_interval).await {
                        break;
                    }
                    continue;
                }
                match engine_for_cloud
                    .sync_directory_incremental(&remote_root)
                    .await
                {
                    Ok(delta) => {
                        if !network_available_cloud.swap(true, Ordering::SeqCst) {
                            eprintln!("[yadiskd] network restored");
                        }
                        cloud_sync_error_cloud.store(false, Ordering::SeqCst);
                        if let Some(space_status) = engine_for_cloud.cloud_space_status().await {
                            cloud_space_low_cloud.store(space_status.low, Ordering::SeqCst);
                        }
                        if delta.indexed > 0 || delta.deleted > 0 || delta.enqueued_downloads > 0 {
                            eprintln!(
                                "[yadiskd] cloud delta: indexed={}, deleted={}, enqueued_downloads={}",
                                delta.indexed, delta.deleted, delta.enqueued_downloads
                            );
                        }
                    }
                    Err(err) => {
                        if next_network_availability(Err(&err)) {
                            network_available_cloud.store(true, Ordering::SeqCst);
                            cloud_sync_error_cloud.store(true, Ordering::SeqCst);
                            eprintln!("[yadiskd] cloud sync error: {err}");
                        } else {
                            let was_online = network_available_cloud.swap(false, Ordering::SeqCst);
                            cloud_sync_error_cloud.store(false, Ordering::SeqCst);
                            cloud_space_low_cloud.store(false, Ordering::SeqCst);
                            if was_online {
                                eprintln!("[yadiskd] network unavailable: {err}");
                            }
                        }
                    }
                }
                if sleep_or_shutdown(&shutdown_cloud, cloud_poll_interval).await {
                    break;
                }
            }
        });

        let engine_for_worker = Arc::clone(&self.engine);
        let worker_interval = self.config.worker_interval;
        let sync_root_available_worker = Arc::clone(&sync_root_available);
        let network_available_worker = Arc::clone(&network_available);
        let auth_ready_worker = Arc::clone(&self.auth_ready);
        let shutdown_worker = shutdown.child_token();
        let worker_handle = tokio::spawn(async move {
            loop {
                if shutdown_worker.is_cancelled() {
                    break;
                }
                if !auth_ready_worker.load(Ordering::SeqCst)
                    || !sync_root_available_worker.load(Ordering::SeqCst)
                {
                    if sleep_or_shutdown(&shutdown_worker, worker_interval).await {
                        break;
                    }
                    continue;
                }
                if !network_available_worker.load(Ordering::SeqCst) {
                    if sleep_or_shutdown(&shutdown_worker, Duration::from_secs(2)).await {
                        break;
                    }
                    continue;
                }
                match engine_for_worker.run_once().await {
                    Ok(true) => {
                        eprintln!("[yadiskd] worker: processed queued operation");
                    }
                    Ok(false) => {}
                    Err(err) => {
                        eprintln!("[yadiskd] worker error: {err}");
                    }
                }
                if sleep_or_shutdown(&shutdown_worker, worker_interval).await {
                    break;
                }
            }
        });

        let engine_for_materialize = Arc::clone(&self.engine);
        let materialize_sync_root = self.config.sync_root.clone();
        let materialize_cache_root = self.config.cache_root.clone();
        let materialize_remote_root = self.config.remote_root.clone();
        let local_events_enabled_materialize = Arc::clone(&local_events_enabled);
        let sync_root_available_materialize = Arc::clone(&sync_root_available);
        let materialize_refresh_requested_materialize = Arc::clone(&materialize_refresh_requested);
        let auth_ready_materialize = Arc::clone(&self.auth_ready);
        let shutdown_materialize = shutdown.child_token();
        let materialize_handle = tokio::spawn(async move {
            let mut initial_logged = false;
            let mut materialize_enabled = true;
            let mut previous_materialized_paths: HashSet<PathBuf> = HashSet::new();
            loop {
                if shutdown_materialize.is_cancelled() {
                    break;
                }
                if !materialize_enabled {
                    if sleep_or_shutdown(&shutdown_materialize, Duration::from_secs(5)).await {
                        break;
                    }
                    continue;
                }
                if !auth_ready_materialize.load(Ordering::SeqCst)
                    || !sync_root_available_materialize.load(Ordering::SeqCst)
                {
                    local_events_enabled_materialize.store(false, Ordering::SeqCst);
                    if sleep_or_shutdown(&shutdown_materialize, Duration::from_secs(1)).await {
                        break;
                    }
                    continue;
                }
                if materialize_refresh_requested_materialize.swap(false, Ordering::SeqCst) {
                    initial_logged = false;
                    previous_materialized_paths.clear();
                    local_events_enabled_materialize.store(false, Ordering::SeqCst);
                }
                match materialize_sync_tree(
                    &engine_for_materialize,
                    &materialize_sync_root,
                    &materialize_cache_root,
                    &materialize_remote_root,
                )
                .await
                {
                    Ok(total_items) => {
                        match collect_materialized_local_paths(
                            &engine_for_materialize,
                            &materialize_sync_root,
                            &materialize_remote_root,
                        )
                        .await
                        {
                            Ok(current_paths) => {
                                if initial_logged
                                    && let Err(err) = prune_removed_materialized_paths(
                                        &previous_materialized_paths,
                                        &current_paths,
                                        &materialize_sync_root,
                                        &materialize_cache_root,
                                    )
                                    .await
                                {
                                    eprintln!("[yadiskd] materialize prune error: {err}");
                                }
                                previous_materialized_paths = current_paths;
                            }
                            Err(err) => {
                                eprintln!("[yadiskd] materialize metadata collection error: {err}")
                            }
                        }

                        if !initial_logged {
                            eprintln!("[yadiskd] metadata tree initialized: {total_items} entries");
                            local_events_enabled_materialize.store(true, Ordering::SeqCst);
                            initial_logged = true;
                        }
                    }
                    Err(err) => {
                        local_events_enabled_materialize.store(false, Ordering::SeqCst);
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
                if sleep_or_shutdown(&shutdown_materialize, Duration::from_secs(1)).await {
                    break;
                }
            }
        });

        let engine_for_storage = Arc::clone(&self.engine);
        let storage_sync_root = self.config.sync_root.clone();
        let sync_root_available_storage = Arc::clone(&sync_root_available);
        let sync_root_generation_storage = Arc::clone(&sync_root_generation);
        let local_events_enabled_storage = Arc::clone(&local_events_enabled);
        let materialize_refresh_requested_storage = Arc::clone(&materialize_refresh_requested);
        let shutdown_storage = shutdown.child_token();
        let storage_handle = tokio::spawn(async move {
            let mut known = sync_root_available_storage.load(Ordering::SeqCst);
            let mut known_identity = if known {
                sync_root_identity(&storage_sync_root).await
            } else {
                None
            };
            loop {
                if shutdown_storage.is_cancelled() {
                    break;
                }
                let current_identity = sync_root_identity(&storage_sync_root).await;
                let available = current_identity.is_some();
                let refresh_required =
                    should_refresh_materialized_sync_root(known_identity, current_identity);
                if available != known || refresh_required {
                    sync_root_available_storage.store(available, Ordering::SeqCst);
                    if available {
                        if known {
                            eprintln!(
                                "[yadiskd] sync root replaced, refreshing local snapshot: {}",
                                storage_sync_root.display()
                            );
                        } else {
                            eprintln!(
                                "[yadiskd] sync root restored: {}",
                                storage_sync_root.display()
                            );
                        }
                        local_events_enabled_storage.store(false, Ordering::SeqCst);
                        materialize_refresh_requested_storage.store(true, Ordering::SeqCst);
                        sync_root_generation_storage.fetch_add(1, Ordering::SeqCst);
                    } else {
                        eprintln!(
                            "[yadiskd] sync root unavailable, pausing local operations: {}",
                            storage_sync_root.display()
                        );
                        local_events_enabled_storage.store(false, Ordering::SeqCst);
                        materialize_refresh_requested_storage.store(false, Ordering::SeqCst);
                        engine_for_storage.cancel_all_transfers();
                    }
                    known = available;
                    known_identity = current_identity;
                } else {
                    known_identity = current_identity;
                }
                if sleep_or_shutdown(&shutdown_storage, Duration::from_secs(2)).await {
                    break;
                }
            }
        });

        let engine_for_eviction = Arc::clone(&self.engine);
        let eviction_root = self.config.remote_root.clone();
        let cache_root = self.config.cache_root.clone();
        let cache_max_bytes = self.config.cache_max_bytes;
        let eviction_interval = self.config.eviction_interval;
        let shutdown_eviction = shutdown.child_token();
        let eviction_handle = tokio::spawn(async move {
            loop {
                if shutdown_eviction.is_cancelled() {
                    break;
                }
                let _ = run_cache_eviction_once(
                    &engine_for_eviction,
                    &cache_root,
                    &eviction_root,
                    cache_max_bytes,
                )
                .await;
                if sleep_or_shutdown(&shutdown_eviction, eviction_interval).await {
                    break;
                }
            }
        });

        let signal_emitter = SignalEmitter::new(&dbus_connection, DBUS_OBJECT_PATH_SYNC)
            .context("failed to create D-Bus signal emitter")?
            .into_owned();
        let control_signal_emitter = SignalEmitter::new(&dbus_connection, DBUS_OBJECT_PATH_CONTROL)
            .context("failed to create control D-Bus signal emitter")?
            .into_owned();
        let engine_for_signals = Arc::clone(&self.engine);
        let signal_root = self.config.remote_root.clone();
        let tray_state_tx_signal = tray_state_tx.clone();
        let sync_root_available_signal = Arc::clone(&sync_root_available);
        let cloud_sync_error_signal = Arc::clone(&cloud_sync_error);
        let cloud_space_low_signal = Arc::clone(&cloud_space_low);
        let network_available_signal = Arc::clone(&network_available);
        let daemon_status_signal = Arc::clone(&daemon_status);
        let shutdown_signal = shutdown.child_token();
        let signal_handle = tokio::spawn(async move {
            let mut known_states: HashMap<String, &'static str> = HashMap::new();
            let mut last_signal_snapshot_complete = true;
            let mut known_tray_state: Option<TraySyncState> = None;
            let mut known_tray_language: Option<String> = None;
            let mut known_daemon_state: Option<(&'static str, &'static str)> = None;
            let mut last_conflict_id = 0i64;

            loop {
                if shutdown_signal.is_cancelled() {
                    break;
                }
                let tray_language =
                    resolve_effective_language(load_ui_preferences().language_preference);
                let language_changed =
                    known_tray_language.as_deref() != Some(tray_language.as_str());
                if let Ok(states) = engine_for_signals.list_states_by_prefix(&signal_root).await {
                    let mut current_states = HashMap::with_capacity(states.len());
                    for (path, state) in states {
                        let state_str = match state {
                            crate::sync::index::FileState::CloudOnly => "cloud_only",
                            crate::sync::index::FileState::Cached => "cached",
                            crate::sync::index::FileState::Syncing => "syncing",
                            crate::sync::index::FileState::Error => "error",
                        };
                        current_states.insert(path, state_str);
                    }
                    let force_snapshot = should_force_signal_snapshot(
                        last_signal_snapshot_complete,
                        &known_states,
                        &current_states,
                    );
                    let mut signal_errors = 0usize;
                    for (path, state_str) in &current_states {
                        let changed = known_states
                            .get(path.as_str())
                            .map(|existing| *existing != *state_str)
                            .unwrap_or(true);
                        if (force_snapshot || changed)
                            && let Err(err) =
                                SyncDbusService::state_changed(&signal_emitter, path, state_str)
                                    .await
                        {
                            signal_errors += 1;
                            eprintln!("[yadiskd] failed to emit state_changed for {path}: {err}");
                        }
                    }
                    last_signal_snapshot_complete = signal_errors == 0;
                    if signal_errors != 0 {
                        eprintln!(
                            "[yadiskd] D-Bus state snapshot incomplete, will replay on next tick"
                        );
                    }
                    let has_active_work = engine_for_signals
                        .has_active_or_queued_work()
                        .await
                        .unwrap_or(false);
                    let sync_root_ready = sync_root_available_signal.load(Ordering::SeqCst);
                    let network_ready = network_available_signal.load(Ordering::SeqCst);
                    let cloud_error = cloud_sync_error_signal.load(Ordering::SeqCst);
                    let cloud_space_warn = cloud_space_low_signal.load(Ordering::SeqCst);
                    let tray_state = effective_tray_state(
                        &current_states,
                        has_active_work,
                        sync_root_ready,
                        network_ready,
                        cloud_error,
                    );
                    if known_tray_state != Some(tray_state) || language_changed {
                        let _ = tray_state_tx_signal.send(tray_state);
                        known_tray_state = Some(tray_state);
                    }
                    let daemon_state = if !sync_root_ready {
                        ("error", "sync root unavailable")
                    } else if !network_ready {
                        ("offline", "network unavailable")
                    } else if cloud_space_warn {
                        ("running", "cloud space low")
                    } else {
                        match tray_state {
                            TraySyncState::Normal => ("running", "idle"),
                            TraySyncState::Syncing => ("busy", "queued or active operations"),
                            TraySyncState::Error => ("error", "sync engine reported an error"),
                        }
                    };
                    if known_daemon_state != Some(daemon_state) {
                        {
                            let mut status = daemon_status_signal.write().await;
                            *status = (daemon_state.0.to_string(), daemon_state.1.to_string());
                        }
                        if let Err(err) = ControlDbusService::daemon_status_changed(
                            &control_signal_emitter,
                            daemon_state.0,
                            daemon_state.1,
                        )
                        .await
                        {
                            eprintln!("[yadiskd] failed to emit daemon_status_changed: {err}");
                        } else {
                            known_daemon_state = Some(daemon_state);
                        }
                    }
                    known_states = current_states;
                } else if known_tray_state != Some(TraySyncState::Error) || language_changed {
                    let _ = tray_state_tx_signal.send(TraySyncState::Error);
                    known_tray_state = Some(TraySyncState::Error);
                    last_signal_snapshot_complete = false;
                }
                known_tray_language = Some(tray_language);

                if let Ok(conflicts) = engine_for_signals.list_conflicts().await {
                    for conflict in conflicts {
                        if conflict.id <= last_conflict_id {
                            continue;
                        }
                        let id = u64::try_from(conflict.id).unwrap_or(0);
                        if let Err(err) = SyncDbusService::conflict_added(
                            &signal_emitter,
                            id,
                            &conflict.path,
                            &conflict.renamed_local,
                        )
                        .await
                        {
                            eprintln!(
                                "[yadiskd] failed to emit conflict_added for {}: {err}",
                                conflict.path
                            );
                        } else {
                            last_conflict_id = conflict.id;
                        }
                    }
                }

                if sleep_or_shutdown(&shutdown_signal, Duration::from_secs(1)).await {
                    break;
                }
            }
        });

        let local_handle = if self.config.enable_local_watcher {
            let mut rx = local_rx;
            let engine_for_local = Arc::clone(&self.engine);
            let local_sync_root = self.config.sync_root.clone();
            let local_cache_root = self.config.cache_root.clone();
            let local_remote_root = self.config.remote_root.clone();
            let local_events_enabled_local = Arc::clone(&local_events_enabled);
            let sync_root_available_local = Arc::clone(&sync_root_available);
            let shutdown_local = shutdown.child_token();
            Some(tokio::spawn(async move {
                let mut seen_uploads: HashMap<String, (u64, u128)> = HashMap::new();
                loop {
                    let event = tokio::select! {
                        _ = shutdown_local.cancelled() => break,
                        event = rx.recv() => match event {
                            Some(event) => event,
                            None => break,
                        },
                    };
                    if !local_events_enabled_local.load(Ordering::SeqCst) {
                        continue;
                    }
                    if !sync_root_available_local.load(Ordering::SeqCst) {
                        continue;
                    }
                    let event = normalize_local_event_for_remote_root(event, &local_remote_root);
                    if should_ignore_local_event(&event) {
                        continue;
                    }
                    match &event {
                        LocalEvent::Upload { path } => {
                            if should_skip_local_upload_event(
                                &engine_for_local,
                                &local_sync_root,
                                &local_cache_root,
                                path,
                            )
                            .await
                            {
                                continue;
                            }
                            let Some(fp) = upload_fingerprint(&local_sync_root, path).await else {
                                continue;
                            };
                            if !should_process_upload_event(&mut seen_uploads, path, fp) {
                                continue;
                            }
                        }
                        LocalEvent::Delete { path } => {
                            if engine_for_local
                                .state_for_path(path)
                                .await
                                .ok()
                                .flatten()
                                .is_none()
                            {
                                continue;
                            }
                            seen_uploads.remove(path);
                        }
                        LocalEvent::Move { from, to } => {
                            if let Some(fp) = seen_uploads.remove(from) {
                                seen_uploads.insert(to.clone(), fp);
                            }
                        }
                        LocalEvent::Mkdir { .. } => {}
                    }
                    eprintln!("[yadiskd] local event: {:?}", event);
                    if let Err(err) =
                        mirror_local_event_to_cache(&local_sync_root, &local_cache_root, &event)
                            .await
                    {
                        eprintln!("[yadiskd] local cache mirror error: {err}");
                    }
                    if let Err(err) = engine_for_local.ingest_local_event(event).await {
                        eprintln!("[yadiskd] local ingest error: {err}");
                    }
                }
            }))
        } else {
            None
        };

        tokio::select! {
            res = tokio::signal::ctrl_c() => {
                res.context("failed waiting for shutdown signal")?;
            }
            _ = quit_rx.recv() => {
                eprintln!("[yadiskd] quit requested from status tray");
            }
        }

        shutdown.cancel();
        self.engine.cancel_all_transfers();
        drop(quit_tx);
        drop(tray_state_tx);

        shutdown_task("cloud", cloud_handle).await;
        shutdown_task("worker", worker_handle).await;
        shutdown_task("materialize", materialize_handle).await;
        shutdown_task("storage", storage_handle).await;
        shutdown_task("eviction", eviction_handle).await;
        shutdown_task("signals", signal_handle).await;
        if let Some(handle) = watcher_handle {
            shutdown_task("watcher", handle).await;
        }
        if let Some(handle) = local_handle {
            shutdown_task("local-events", handle).await;
        }
        if let Some(handle) = tray_handle {
            shutdown_task("tray", handle).await;
        }

        Ok(())
    }
}

pub(crate) async fn sleep_or_shutdown(shutdown: &CancellationToken, duration: Duration) -> bool {
    tokio::select! {
        _ = shutdown.cancelled() => true,
        _ = tokio::time::sleep(duration) => false,
    }
}

async fn shutdown_task(name: &str, mut handle: JoinHandle<()>) {
    match tokio::time::timeout(SHUTDOWN_TASK_TIMEOUT, &mut handle).await {
        Ok(Ok(())) => {}
        Ok(Err(err)) if err.is_cancelled() => {}
        Ok(Err(err)) => {
            eprintln!("[yadiskd] task {name} exited with join error: {err}");
        }
        Err(_) => {
            eprintln!("[yadiskd] task {name} did not stop in time; aborting");
            handle.abort();
            if let Err(err) = handle.await
                && !err.is_cancelled()
            {
                eprintln!("[yadiskd] task {name} aborted with join error: {err}");
            }
        }
    }
}

include!("daemon_helpers.rs");

#[cfg(test)]
#[path = "daemon_tests.rs"]
mod tests;
