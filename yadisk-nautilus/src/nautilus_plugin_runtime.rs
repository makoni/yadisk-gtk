    fn sync_root() -> &'static PathBuf {
        SYNC_ROOT.get_or_init(|| {
            std::env::var("YADISK_SYNC_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| {
                    dirs::home_dir()
                        .unwrap_or_else(|| PathBuf::from("/"))
                        .join("Yandex Disk")
                })
        })
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

        cache.insert(remote_path.to_string(), state);
        if let Some(rest) = remote_path.strip_prefix("disk:/") {
            cache.insert(format!("/{}", rest.trim_start_matches('/')), state);
        } else if let Some(rest) = remote_path.strip_prefix('/') {
            cache.insert(format!("disk:/{}", rest), state);
        }
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
        START_SIGNAL_THREAD.call_once(|| {
            let Some(client) = dbus_client().cloned() else {
                return;
            };
            SIGNAL_THREAD_STARTED.store(true, Ordering::SeqCst);

            thread::spawn(move || {
                let Ok(mut listener) = client.subscribe_signals() else {
                    return;
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
            });
        });
    }
