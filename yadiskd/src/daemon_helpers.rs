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

async fn mirror_local_event_to_cache(
    sync_root: &Path,
    cache_root: &Path,
    event: &LocalEvent,
) -> anyhow::Result<()> {
    match event {
        LocalEvent::Upload { path } => {
            let source = sync_path_for(sync_root, path)?;
            let Ok(meta) = tokio::fs::metadata(&source).await else {
                return Ok(());
            };
            if meta.is_dir() {
                return Ok(());
            }
            let target = crate::sync::paths::cache_path_for(cache_root, path)?;
            if let Some(parent) = target.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let _ = tokio::fs::copy(&source, &target).await?;
        }
        LocalEvent::Mkdir { path } => {
            let target = crate::sync::paths::cache_path_for(cache_root, path)?;
            tokio::fs::create_dir_all(&target).await?;
        }
        LocalEvent::Delete { path } => {
            let target = crate::sync::paths::cache_path_for(cache_root, path)?;
            if let Ok(meta) = tokio::fs::metadata(&target).await {
                if meta.is_dir() {
                    let _ = tokio::fs::remove_dir_all(&target).await;
                } else {
                    let _ = tokio::fs::remove_file(&target).await;
                }
            }
        }
        LocalEvent::Move { from, to } => {
            let from_cache = crate::sync::paths::cache_path_for(cache_root, from)?;
            let to_cache = crate::sync::paths::cache_path_for(cache_root, to)?;
            if let Some(parent) = to_cache.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            if tokio::fs::metadata(&from_cache).await.is_ok() {
                match tokio::fs::rename(&from_cache, &to_cache).await {
                    Ok(_) => {}
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                    Err(err) => return Err(err.into()),
                }
            }
            let local_target = sync_path_for(sync_root, to)?;
            if let Ok(meta) = tokio::fs::metadata(&local_target).await {
                if meta.is_dir() {
                    tokio::fs::create_dir_all(&to_cache).await?;
                } else {
                    if let Some(parent) = to_cache.parent() {
                        tokio::fs::create_dir_all(parent).await?;
                    }
                    let _ = tokio::fs::copy(&local_target, &to_cache).await?;
                }
            }
        }
    }
    Ok(())
}

async fn should_skip_local_upload_event(
    engine: &SyncEngine,
    sync_root: &Path,
    cache_root: &Path,
    path: &str,
) -> bool {
    let Ok(local_path) = sync_path_for(sync_root, path) else {
        return false;
    };
    let Ok(meta) = tokio::fs::metadata(&local_path).await else {
        return false;
    };
    let Ok(Some(state)) = engine.state_for_path(path).await else {
        return false;
    };
    match state {
        crate::sync::engine::PathDisplayState::CloudOnly => meta.len() == 0,
        crate::sync::engine::PathDisplayState::Cached => {
            let Ok(cache_path) = crate::sync::paths::cache_path_for(cache_root, path) else {
                return false;
            };
            let Ok(cache_meta) = tokio::fs::metadata(&cache_path).await else {
                return false;
            };
            if cache_meta.is_dir() || meta.is_dir() || cache_meta.len() != meta.len() {
                return false;
            }
            let Ok(local_bytes) = tokio::fs::read(&local_path).await else {
                return false;
            };
            let Ok(cache_bytes) = tokio::fs::read(&cache_path).await else {
                return false;
            };
            local_bytes == cache_bytes
        }
        _ => false,
    }
}

async fn upload_fingerprint(sync_root: &Path, path: &str) -> Option<(u64, u128)> {
    let local_path = sync_path_for(sync_root, path).ok()?;
    let meta = tokio::fs::metadata(local_path).await.ok()?;
    if meta.is_dir() {
        return None;
    }
    let modified_ns = meta
        .modified()
        .ok()?
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_nanos();
    Some((meta.len(), modified_ns))
}

fn should_process_upload_event(
    seen_uploads: &mut HashMap<String, (u64, u128)>,
    path: &str,
    fingerprint: (u64, u128),
) -> bool {
    if seen_uploads.get(path) == Some(&fingerprint) {
        return false;
    }
    seen_uploads.insert(path.to_string(), fingerprint);
    true
}

fn normalize_local_event_for_remote_root(event: LocalEvent, remote_root: &str) -> LocalEvent {
    fn normalize_path(path: String, remote_root: &str) -> String {
        if remote_root.starts_with("disk:/") {
            if path.starts_with("disk:/") {
                return path;
            }
            let slash = if let Some(rest) = path.strip_prefix("disk:/") {
                format!("/{}", rest.trim_start_matches('/'))
            } else if path.starts_with('/') {
                path
            } else {
                format!("/{}", path)
            };
            let rest = slash.trim_start_matches('/');
            if rest.is_empty() {
                "disk:/".to_string()
            } else {
                format!("disk:/{}", rest)
            }
        } else if let Some(rest) = path.strip_prefix("disk:/") {
            format!("/{}", rest.trim_start_matches('/'))
        } else if path.starts_with('/') {
            path
        } else {
            format!("/{}", path)
        }
    }

    match event {
        LocalEvent::Upload { path } => LocalEvent::Upload {
            path: normalize_path(path, remote_root),
        },
        LocalEvent::Mkdir { path } => LocalEvent::Mkdir {
            path: normalize_path(path, remote_root),
        },
        LocalEvent::Delete { path } => LocalEvent::Delete {
            path: normalize_path(path, remote_root),
        },
        LocalEvent::Move { from, to } => LocalEvent::Move {
            from: normalize_path(from, remote_root),
            to: normalize_path(to, remote_root),
        },
    }
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
        if tokio::fs::try_exists(&local_path).await? {
            match state {
                Some(FileState::CloudOnly) => {
                    let local_meta = tokio::fs::metadata(&local_path).await?;
                    if local_meta.len() > 0 {
                        let file = tokio::fs::OpenOptions::new()
                            .write(true)
                            .open(&local_path)
                            .await?;
                        file.set_len(0).await?;
                    }
                }
                Some(FileState::Cached) => {
                    let local_meta = tokio::fs::metadata(&local_path).await?;
                    if local_meta.len() == 0 {
                        let cache_path = crate::sync::paths::cache_path_for(cache_root, &item.path)?;
                        if tokio::fs::try_exists(&cache_path).await? {
                            let _ = tokio::fs::copy(&cache_path, &local_path).await?;
                        }
                    }
                }
                _ => {}
            }
            continue;
        }

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
