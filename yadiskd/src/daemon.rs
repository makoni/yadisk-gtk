use std::collections::{HashMap, HashSet};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Context;
use md5::Context as Md5Context;
use tokio::io::AsyncReadExt;
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
                "disabled"
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
        let _ = tray_state_tx.send(TraySyncState::Normal);
        let local_events_enabled = Arc::new(AtomicBool::new(false));
        let sync_root_available = Arc::new(AtomicBool::new(
            is_sync_root_available(&self.config.sync_root).await,
        ));
        if !sync_root_available.load(Ordering::SeqCst) {
            eprintln!(
                "[yadiskd] sync root is unavailable: {}",
                self.config.sync_root.display()
            );
        }

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
        let sync_root_available_cloud = Arc::clone(&sync_root_available);
        let cloud_handle = tokio::spawn(async move {
            loop {
                if !sync_root_available_cloud.load(Ordering::SeqCst) {
                    tokio::time::sleep(cloud_poll_interval).await;
                    continue;
                }
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
        let sync_root_available_worker = Arc::clone(&sync_root_available);
        let worker_handle = tokio::spawn(async move {
            loop {
                if !sync_root_available_worker.load(Ordering::SeqCst) {
                    tokio::time::sleep(worker_interval).await;
                    continue;
                }
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
        let local_events_enabled_materialize = Arc::clone(&local_events_enabled);
        let sync_root_available_materialize = Arc::clone(&sync_root_available);
        let materialize_handle = tokio::spawn(async move {
            let mut initial_logged = false;
            let mut materialize_enabled = true;
            loop {
                if !materialize_enabled {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    continue;
                }
                if !sync_root_available_materialize.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_secs(1)).await;
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
                        local_events_enabled_materialize.store(true, Ordering::SeqCst);
                        initial_logged = true;
                    }
                    Ok(_) => {}
                    Err(err) => {
                        local_events_enabled_materialize.store(true, Ordering::SeqCst);
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

        let engine_for_storage = Arc::clone(&self.engine);
        let storage_sync_root = self.config.sync_root.clone();
        let sync_root_available_storage = Arc::clone(&sync_root_available);
        let local_events_enabled_storage = Arc::clone(&local_events_enabled);
        let tray_state_tx_storage = tray_state_tx.clone();
        let storage_handle = tokio::spawn(async move {
            let mut known = sync_root_available_storage.load(Ordering::SeqCst);
            loop {
                let available = is_sync_root_available(&storage_sync_root).await;
                if available != known {
                    sync_root_available_storage.store(available, Ordering::SeqCst);
                    if available {
                        eprintln!(
                            "[yadiskd] sync root restored: {}",
                            storage_sync_root.display()
                        );
                        local_events_enabled_storage.store(true, Ordering::SeqCst);
                    } else {
                        eprintln!(
                            "[yadiskd] sync root unavailable, pausing local operations: {}",
                            storage_sync_root.display()
                        );
                        local_events_enabled_storage.store(false, Ordering::SeqCst);
                        engine_for_storage.cancel_all_transfers();
                        let _ = tray_state_tx_storage.send(TraySyncState::Error);
                    }
                    known = available;
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
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
                    let has_active_work = engine_for_signals
                        .has_active_or_queued_work()
                        .await
                        .unwrap_or(false);
                    let tray_state = tray_state_from_states(&current_states, has_active_work);
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
            let local_sync_root = self.config.sync_root.clone();
            let local_cache_root = self.config.cache_root.clone();
            let local_remote_root = self.config.remote_root.clone();
            let local_events_enabled_local = Arc::clone(&local_events_enabled);
            let sync_root_available_local = Arc::clone(&sync_root_available);
            Some(tokio::spawn(async move {
                let mut seen_uploads: HashMap<String, (u64, u128)> = HashMap::new();
                while let Some(event) = rx.recv().await {
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
        storage_handle.abort();
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

include!("daemon_helpers.rs");

#[cfg(test)]
#[path = "daemon_tests.rs"]
mod tests;
