impl SyncEngine {
    pub async fn sync_directory_once(&self, path: &str) -> Result<usize, EngineError> {
        let list = self.client.list_directory_all(path, 100, None).await?;
        for item in &list {
            let input = ItemInput {
                path: item.path.clone(),
                parent_path: parent_path(&item.path),
                name: item.name.clone(),
                item_type: match item.resource_type {
                    ResourceType::File => ItemType::File,
                    ResourceType::Dir => ItemType::Dir,
                },
                size: item.size.map(|v| v as i64),
                modified: parse_modified(item.modified.as_deref())?,
                hash: item.md5.clone(),
                resource_id: item.resource_id.clone(),
                last_synced_hash: None,
                last_synced_modified: None,
            };
            let record = self.index.upsert_item(&input).await?;

            // For files, default to cloud-only unless already cached/pinned.
            if input.item_type == ItemType::File && self.index.get_state(record.id).await?.is_none()
            {
                self.index
                    .set_state(record.id, FileState::CloudOnly, false, None)
                    .await?;
            }
        }

        Ok(list.len())
    }

    pub async fn sync_directory_incremental(&self, path: &str) -> Result<SyncDelta, EngineError> {
        let remote_items = self.collect_remote_tree(path).await?;
        let local_items = self.index.list_items_by_prefix(path).await?;
        let local_by_path: HashMap<String, _> = local_items
            .iter()
            .map(|item| (item.path.clone(), item.clone()))
            .collect();
        let local_by_resource_id: HashMap<String, _> = local_items
            .iter()
            .filter_map(|item| item.resource_id.clone().map(|rid| (rid, item.clone())))
            .collect();
        let remote_paths: HashSet<String> = remote_items.iter().map(|r| r.path.clone()).collect();
        let remote_resource_ids: HashSet<String> = remote_items
            .iter()
            .filter_map(|r| r.resource_id.clone())
            .collect();

        let mut delta = SyncDelta::default();

        for item in &remote_items {
            let previous_by_path = path_variants(&item.path)
                .into_iter()
                .find_map(|candidate| local_by_path.get(&candidate).cloned());
            let input = ItemInput {
                path: item.path.clone(),
                parent_path: parent_path(&item.path),
                name: item.name.clone(),
                item_type: match item.resource_type {
                    ResourceType::File => ItemType::File,
                    ResourceType::Dir => ItemType::Dir,
                },
                size: item.size.map(|v| v as i64),
                modified: parse_modified(item.modified.as_deref())?,
                hash: item.md5.clone(),
                resource_id: item.resource_id.clone(),
                last_synced_hash: item.md5.clone(),
                last_synced_modified: parse_modified(item.modified.as_deref())?,
            };
            let record = self.index.upsert_item(&input).await?;
            delta.indexed += 1;

            if let Some(resource_id) = &item.resource_id
                && let Some(previous) = local_by_resource_id.get(resource_id)
                && previous.path != item.path
            {
                if let Some(prev_state) = self.index.get_state(previous.id).await? {
                    self.index
                        .set_state_with_meta(
                            record.id,
                            prev_state.state,
                            prev_state.pinned,
                            prev_state.last_error.as_deref(),
                            StateMeta {
                                retry_at: prev_state.retry_at,
                                last_success_at: prev_state.last_success_at,
                                last_error_at: prev_state.last_error_at,
                                dirty: prev_state.dirty,
                            },
                        )
                        .await?;
                } else if input.item_type == ItemType::File {
                    self.index
                        .set_state(record.id, FileState::CloudOnly, false, None)
                        .await?;
                }
                self.index.delete_item_by_path(&previous.path).await?;
                delta.deleted += 1;
            } else if input.item_type == ItemType::File
                && self.index.get_state(record.id).await?.is_none()
            {
                self.index
                    .set_state(record.id, FileState::CloudOnly, false, None)
                    .await?;
            }

            if input.item_type == ItemType::File
                && let Some(previous) = previous_by_path
            {
                let remote_changed = previous.hash != input.hash
                    || previous.modified != input.modified
                    || previous.size != input.size;
                if remote_changed
                    && let Some(state) = self.index.get_state(record.id).await?
                    && matches!(state.state, FileState::Cached)
                {
                    self.enqueue_download(&item.path).await?;
                    delta.enqueued_downloads += 1;
                }
            }
        }

        for old in &local_items {
            if remote_paths.contains(&old.path) {
                continue;
            }
            if let Some(resource_id) = &old.resource_id
                && remote_resource_ids.contains(resource_id)
            {
                continue;
            }
            if old.resource_id.is_none()
                && let Some(state) = self.index.get_state(old.id).await?
                && matches!(state.state, FileState::Syncing | FileState::Cached)
            {
                continue;
            }
            self.index.delete_item_by_path(&old.path).await?;
            delta.deleted += 1;
        }

        let pinned_cloud = self
            .index
            .list_pinned_cloud_only_paths_by_prefix(path)
            .await?;
        for path in pinned_cloud {
            self.enqueue_download(&path).await?;
            delta.enqueued_downloads += 1;
        }

        Ok(delta)
    }

    pub async fn enqueue_download(&self, path: &str) -> Result<i64, EngineError> {
        let item = self
            .index
            .get_item_by_path(path)
            .await?
            .ok_or_else(|| EngineError::MissingItem(path.to_string()))?;
        self.index
            .set_state(item.id, FileState::Syncing, true, None)
            .await?;
        Ok(self
            .index
            .enqueue_op(&Operation {
                kind: OperationKind::Download,
                path: path.to_string(),
                payload: None,
                attempt: 0,
                retry_at: None,
                priority: 50,
            })
            .await?)
    }

    pub async fn enqueue_upload(&self, path: &str) -> Result<i64, EngineError> {
        let item = if let Some(item) = self.index.get_item_by_path(path).await? {
            item
        } else {
            let local = cache_path_for(&self.cache_root, path)?;
            let meta = tokio::fs::metadata(&local)
                .await
                .map_err(|_| EngineError::MissingItem(path.to_string()))?;
            if meta.is_dir() {
                return self.enqueue_mkdir(path).await;
            }
            self.index
                .upsert_item(&ItemInput {
                    path: path.to_string(),
                    parent_path: parent_path(path),
                    name: path.split('/').next_back().unwrap_or(path).to_string(),
                    item_type: ItemType::File,
                    size: Some(meta.len() as i64),
                    modified: None,
                    hash: None,
                    resource_id: None,
                    last_synced_hash: None,
                    last_synced_modified: None,
                })
                .await?
        };
        self.index
            .set_state(item.id, FileState::Syncing, true, None)
            .await?;
        Ok(self
            .index
            .enqueue_op(&Operation {
                kind: OperationKind::Upload,
                path: path.to_string(),
                payload: None,
                attempt: 0,
                retry_at: None,
                priority: 50,
            })
            .await?)
    }

    pub async fn enqueue_mkdir(&self, path: &str) -> Result<i64, EngineError> {
        let item = if let Some(item) = self.index.get_item_by_path(path).await? {
            item
        } else {
            self.index
                .upsert_item(&ItemInput {
                    path: path.to_string(),
                    parent_path: parent_path(path),
                    name: path.split('/').next_back().unwrap_or(path).to_string(),
                    item_type: ItemType::Dir,
                    size: None,
                    modified: None,
                    hash: None,
                    resource_id: None,
                    last_synced_hash: None,
                    last_synced_modified: None,
                })
                .await?
        };
        self.index
            .set_state(item.id, FileState::Syncing, true, None)
            .await?;
        Ok(self
            .index
            .enqueue_op(&Operation {
                kind: OperationKind::Mkdir,
                path: path.to_string(),
                payload: None,
                attempt: 0,
                retry_at: None,
                priority: 55,
            })
            .await?)
    }

    pub async fn enqueue_delete(&self, path: &str) -> Result<i64, EngineError> {
        self.index.delete_ops_for_path(path).await?;
        self.cancel_transfer(path);
        if let Some(item) = self.index.get_item_by_path(path).await? {
            self.index
                .set_state(item.id, FileState::Syncing, true, None)
                .await?;
        }
        Ok(self
            .index
            .enqueue_op(&Operation {
                kind: OperationKind::Delete,
                path: path.to_string(),
                payload: None,
                attempt: 0,
                retry_at: None,
                priority: 60,
            })
            .await?)
    }

    pub async fn enqueue_move(
        &self,
        from: &str,
        to: &str,
        action: &str,
    ) -> Result<i64, EngineError> {
        let payload = serde_json::to_string(&MovePayload {
            from: from.to_string(),
            path: to.to_string(),
            overwrite: true,
            action: action.to_string(),
        })
        .map_err(|_| EngineError::OperationFailed)?;
        Ok(self
            .index
            .enqueue_op(&Operation {
                kind: OperationKind::Move,
                path: to.to_string(),
                payload: Some(payload),
                attempt: 0,
                retry_at: None,
                priority: 60,
            })
            .await?)
    }

    pub async fn ingest_local_event(&self, event: LocalEvent) -> Result<i64, EngineError> {
        match event {
            LocalEvent::Upload { path } => self.enqueue_upload(&path).await,
            LocalEvent::Mkdir { path } => self.enqueue_mkdir(&path).await,
            LocalEvent::Delete { path } => self.enqueue_delete(&path).await,
            LocalEvent::Move { from, to } => self.enqueue_move(&from, &to, "move").await,
        }
    }

    pub async fn pin_path(&self, path: &str, pinned: bool) -> Result<(), EngineError> {
        let item = self
            .index
            .get_item_by_path(path)
            .await?
            .ok_or_else(|| EngineError::MissingItem(path.to_string()))?;
        let mut targets = vec![item];
        if targets[0].item_type == ItemType::Dir {
            for descendant in self.index.list_items_by_prefix(path).await? {
                if descendant.id != targets[0].id {
                    targets.push(descendant);
                }
            }
        }
        for target in targets {
            let state = self.index.get_state(target.id).await?;
            let current_state = state
                .as_ref()
                .map(|row| row.state.clone())
                .unwrap_or(FileState::CloudOnly);
            let last_error = state.as_ref().and_then(|row| row.last_error.as_deref());
            self.index
                .set_state(target.id, current_state, pinned, last_error)
                .await?;
        }
        Ok(())
    }

    pub async fn evict_path(&self, path: &str) -> Result<(), EngineError> {
        let item = self
            .index
            .get_item_by_path(path)
            .await?
            .ok_or_else(|| EngineError::MissingItem(path.to_string()))?;
        let mut targets = vec![item];
        if targets[0].item_type == ItemType::Dir {
            for descendant in self.index.list_items_by_prefix(path).await? {
                if descendant.id != targets[0].id {
                    targets.push(descendant);
                }
            }
        }
        for target in targets {
            self.index
                .set_state(target.id, FileState::CloudOnly, false, None)
                .await?;
        }
        let cache_path = cache_path_for(&self.cache_root, path)?;
        if let Ok(meta) = tokio::fs::metadata(&cache_path).await {
            if meta.is_dir() {
                let _ = tokio::fs::remove_dir_all(&cache_path).await;
            } else {
                let _ = tokio::fs::remove_file(&cache_path).await;
            }
        }
        Ok(())
    }

    pub async fn retry_path(&self, path: &str) -> Result<(), EngineError> {
        self.enqueue_download(path).await?;
        Ok(())
    }

    pub async fn state_for_path(
        &self,
        path: &str,
    ) -> Result<Option<PathDisplayState>, EngineError> {
        let Some(item) = self.index.get_item_by_path(path).await? else {
            return Ok(None);
        };
        if item.item_type == ItemType::File {
            let state = self
                .index
                .get_state(item.id)
                .await?
                .map(|state| state.state)
                .unwrap_or(FileState::CloudOnly);
            return Ok(Some(PathDisplayState::from_file_state(state)));
        }

        let descendants = self.index.list_items_by_prefix(path).await?;
        let states: HashMap<_, _> = self
            .index
            .list_states_by_prefix(path)
            .await?
            .into_iter()
            .collect();

        let mut has_file = false;
        let mut has_cloud = false;
        let mut has_cached = false;
        let mut has_syncing = false;
        let mut has_error = false;

        for descendant in descendants {
            if descendant.item_type != ItemType::File {
                continue;
            }
            has_file = true;
            let state =
                state_for_path_variant(&states, &descendant.path).unwrap_or(FileState::CloudOnly);
            match state {
                FileState::CloudOnly => has_cloud = true,
                FileState::Cached => has_cached = true,
                FileState::Syncing => has_syncing = true,
                FileState::Error => has_error = true,
            }
        }

        if !has_file {
            let own = self
                .index
                .get_state(item.id)
                .await?
                .map(|state| state.state)
                .unwrap_or(FileState::CloudOnly);
            return Ok(Some(PathDisplayState::from_file_state(own)));
        }
        if has_error {
            return Ok(Some(PathDisplayState::Error));
        }
        if has_syncing {
            return Ok(Some(PathDisplayState::Syncing));
        }
        if has_cached && has_cloud {
            return Ok(Some(PathDisplayState::Partial));
        }
        if has_cached {
            return Ok(Some(PathDisplayState::Cached));
        }
        Ok(Some(PathDisplayState::CloudOnly))
    }

    pub async fn list_conflicts(&self) -> Result<Vec<ConflictRecord>, EngineError> {
        Ok(self.index.list_conflicts().await?)
    }

    pub async fn list_items_by_prefix(&self, prefix: &str) -> Result<Vec<ItemRecord>, EngineError> {
        Ok(self.index.list_items_by_prefix(prefix).await?)
    }

    pub async fn list_states_by_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, FileState)>, EngineError> {
        Ok(self.index.list_states_by_prefix(prefix).await?)
    }

    pub async fn list_path_states_with_pin_by_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, FileState, bool)>, EngineError> {
        Ok(self
            .index
            .list_path_states_with_pin_by_prefix(prefix)
            .await?)
    }

    pub async fn resolve_conflict_and_record(
        &self,
        path: &str,
        base: Option<&FileMetadata>,
        local: &FileMetadata,
        remote: &FileMetadata,
    ) -> Result<ConflictDecision, EngineError> {
        let decision = conflict::resolve_conflict(path, base, local, remote);
        if let ConflictDecision::KeepBoth { renamed_local } = &decision {
            self.index
                .record_conflict(path, renamed_local, now_unix(), "both-changed")
                .await?;
        }
        Ok(decision)
    }

    pub async fn run_once(&self) -> Result<bool, EngineError> {
        let Some(op) = self.index.dequeue_op().await? else {
            return Ok(false);
        };
        eprintln!("[yadiskd] op start: kind={:?} path={}", op.kind, op.path);

        let result = match op.kind.clone() {
            OperationKind::Download => self.execute_download(&op.path).await,
            OperationKind::Upload => self.execute_upload(&op.path).await,
            OperationKind::Mkdir => self.execute_mkdir(&op.path).await,
            OperationKind::Delete => {
                let link = self.client.delete_resource(&op.path, true).await?;
                if let Some(link) = link {
                    self.wait_for_operation(link.href.as_str()).await?;
                }
                self.index.delete_item_by_path(&op.path).await?;
                Ok(())
            }
            OperationKind::Move => self.execute_move_like_op(&op).await,
        };

        if let Err(err) = result {
            if is_transient_error(&err) {
                if op.attempt.saturating_add(1) >= MAX_RETRY_ATTEMPTS {
                    eprintln!(
                        "[yadiskd] op failed permanently after retries: kind={:?} path={} err={}",
                        op.kind, op.path, err
                    );
                    return Err(err);
                }
                let retry_after = match &err {
                    EngineError::Api(api) => api
                        .retry_after_secs()
                        .map(|seconds| now_unix().saturating_add(seconds as i64)),
                    _ => None,
                }
                .unwrap_or_else(|| {
                    now_unix().saturating_add(self.backoff.delay(op.attempt + 1).as_secs() as i64)
                });
                self.index
                    .requeue_op(&op, retry_after, Some(&err.to_string()))
                    .await?;
                eprintln!(
                    "[yadiskd] op requeued: kind={:?} path={} attempt={} retry_at={}",
                    op.kind,
                    op.path,
                    op.attempt.saturating_add(1),
                    retry_after
                );
                return Ok(true);
            }
            if let Ok(Some(item)) = self.index.get_item_by_path(&op.path).await {
                if matches!(
                    &err,
                    EngineError::Api(yadisk_core::YadiskError::Api { status, .. })
                        if matches!(
                            *status,
                            reqwest::StatusCode::PAYLOAD_TOO_LARGE
                                | reqwest::StatusCode::INSUFFICIENT_STORAGE
                        )
                ) {
                    self.refresh_upload_limit_cache();
                }
                let pinned = self
                    .index
                    .get_state(item.id)
                    .await
                    .ok()
                    .flatten()
                    .map(|state| state.pinned)
                    .unwrap_or(true);
                let _ = self
                    .index
                    .set_state_with_meta(
                        item.id,
                        FileState::Error,
                        pinned,
                        Some(&err.to_string()),
                        StateMeta {
                            retry_at: None,
                            last_success_at: None,
                            last_error_at: Some(now_unix()),
                            dirty: false,
                        },
                    )
                    .await;
            }
            eprintln!(
                "[yadiskd] op failed: kind={:?} path={} err={}",
                op.kind, op.path, err
            );
            return Err(err);
        }

        eprintln!("[yadiskd] op done: kind={:?} path={}", op.kind, op.path);
        Ok(true)
    }
}
