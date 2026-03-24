    use std::collections::HashSet;

    fn sync_root() -> &'static PathBuf {
        SYNC_ROOT.get_or_init(|| {
            std::env::var("YADISK_SYNC_DIR")
                .map(|value| expand_sync_root(&value))
                .unwrap_or_else(|_| {
                    dirs::home_dir()
                        .unwrap_or_else(|| PathBuf::from("/"))
                        .join("Yandex Disk")
                })
        })
    }

    fn expand_sync_root(value: &str) -> PathBuf {
        if value == "~" {
            return dirs::home_dir().unwrap_or_else(|| PathBuf::from("/"));
        }
        if let Some(rest) = value.strip_prefix("~/")
            && let Some(home) = dirs::home_dir()
        {
            return home.join(rest);
        }
        PathBuf::from(value)
    }

    fn dbus_client() -> Option<&'static Arc<SyncDbusClient>> {
        CLIENT
            .get_or_init(|| SyncDbusClient::connect_session().ok().map(Arc::new))
            .as_ref()
    }

    fn state_cache() -> &'static RwLock<HashMap<String, SyncUiState>> {
        STATE_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
    }

    fn action_contexts() -> &'static Mutex<HashMap<usize, ActionContext>> {
        ACTION_CONTEXTS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    fn cache_state(remote_path: &str, state: SyncUiState) {
        let Ok(mut cache) = state_cache().write() else {
            return;
        };
        for alias in remote_path_aliases(remote_path) {
            cache.insert(alias, state);
        }
    }

    fn apply_full_state_snapshot(
        states: &[(String, SyncUiState)],
        sync_root: &Path,
    ) -> Vec<PathBuf> {
        let new_cache = state_cache_from_snapshot(states);
        let mut changed = HashSet::new();
        let Ok(mut cache) = state_cache().write() else {
            return Vec::new();
        };

        for (path, state) in states {
            let aliases = remote_path_aliases(path);
            let cache_changed = aliases
                .iter()
                .any(|alias| cache.get(alias).copied() != Some(*state));
            if cache_changed {
                changed.insert(map_remote_to_local_path(path, sync_root));
            }
        }

        for existing in cache.keys() {
            if !new_cache.contains_key(existing) {
                let canonical = if existing.starts_with("disk:/") {
                    existing.clone()
                } else {
                    format!("disk:{}", existing)
                };
                changed.insert(map_remote_to_local_path(&canonical, sync_root));
            }
        }

        *cache = new_cache;
        changed.into_iter().collect()
    }

    fn state_for_local_path(local_path: &Path) -> Result<SyncUiState, ExtensionError> {
        let candidates = map_local_to_remote_candidates(local_path, sync_root())?;
        let client = dbus_client().ok_or(ExtensionError::Dbus(zbus::Error::Failure(
            "D-Bus unavailable".into(),
        )))?;

        if let Ok(state) = client.get_state_with_fallback(&candidates) {
            cache_state(&candidates[0], state);
            cache_state(&candidates[1], state);
            return Ok(state);
        }

        if let Ok(cache) = state_cache().read() {
            for candidate in &candidates {
                if let Some(state) = cache.get(candidate) {
                    return Ok(*state);
                }
            }
        }
        client.get_state_with_fallback(&candidates)
    }

    fn invalidate_file_info_for_local_path(local_path: &Path) {
        let Ok(uri) = Url::from_file_path(local_path) else {
            return;
        };
        let Ok(uri_c) = CString::new(uri.as_str()) else {
            return;
        };

        unsafe {
            let file_info = nautilus_file_info_lookup_for_uri(uri_c.as_ptr());
            if file_info.is_null() {
                return;
            }
            nautilus_file_info_invalidate_extension_info(file_info);
            g_object_unref(file_info as *mut GObject);
        }
    }

    fn invalidate_parent_info_for_local_path(local_path: &Path) {
        let Some(parent) = local_path.parent() else {
            return;
        };
        invalidate_file_info_for_local_path(parent);
    }

    fn start_signal_thread_once() {
        if SIGNAL_THREAD_STARTED
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return;
        }

        thread::spawn(move || {
            loop {
                let client = match SyncDbusClient::connect_session() {
                    Ok(c) => Arc::new(c),
                    Err(_) => {
                        thread::sleep(std::time::Duration::from_secs(5));
                        continue;
                    }
                };

                if let Ok(snapshot) = client.get_full_state() {
                    for local_path in apply_full_state_snapshot(&snapshot.states, sync_root()) {
                        invalidate_file_info_for_local_path(&local_path);
                        invalidate_parent_info_for_local_path(&local_path);
                    }
                }

                let Ok(mut listener) = client.subscribe_signals() else {
                    thread::sleep(std::time::Duration::from_secs(5));
                    continue;
                };

                while let Ok(Some(event)) = listener.next_event() {
                    match event {
                        SyncSignalEvent::StateChanged { path, state } => {
                            cache_state(&path, state);
                            let local_path = map_remote_to_local_path(&path, sync_root());
                            eprintln!(
                                "[yadisk-nautilus] state changed: path={} state={}",
                                path,
                                state.as_dbus()
                            );
                            invalidate_file_info_for_local_path(&local_path);
                            invalidate_parent_info_for_local_path(&local_path);
                        }
                        SyncSignalEvent::ConflictAdded { .. } => {}
                    }
                }

                eprintln!("[yadisk-nautilus] signal listener disconnected, reconnecting...");
                thread::sleep(std::time::Duration::from_secs(2));
            }
        });
    }
