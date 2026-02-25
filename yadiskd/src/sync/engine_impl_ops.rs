impl SyncEngine {
    async fn execute_download(&self, path: &str) -> Result<(), EngineError> {
        let item = self
            .index
            .get_item_by_path(path)
            .await?
            .ok_or_else(|| EngineError::MissingItem(path.to_string()))?;

        if item.item_type == ItemType::Dir {
            self.ensure_cache_dir_for_remote(path).await?;
            let descendants = self.index.list_items_by_prefix(path).await?;
            for descendant in descendants {
                if descendant.item_type == ItemType::Dir {
                    self.ensure_cache_dir_for_remote(&descendant.path).await?;
                    self.index
                        .set_state_with_meta(
                            descendant.id,
                            FileState::Cached,
                            true,
                            None,
                            StateMeta {
                                retry_at: None,
                                last_success_at: Some(now_unix()),
                                last_error_at: None,
                                dirty: false,
                            },
                        )
                        .await?;
                    continue;
                }

                let state = self.index.get_state(descendant.id).await?;
                let current_state = state
                    .as_ref()
                    .map(|row| row.state.clone())
                    .unwrap_or(FileState::CloudOnly);
                let last_error = state.as_ref().and_then(|row| row.last_error.as_deref());
                let should_enqueue =
                    !matches!(&current_state, FileState::Cached | FileState::Syncing);
                self.index
                    .set_state(descendant.id, current_state, true, last_error)
                    .await?;
                if should_enqueue {
                    self.enqueue_download(&descendant.path).await?;
                }
            }
            return Ok(());
        }

        if let Some(parent) = parent_path(path) {
            self.ensure_cache_dir_for_remote(&parent).await?;
        }

        let link = self.client.get_download_link(path).await?;
        let target = cache_path_for(&self.cache_root, path)?;
        self.transfer
            .download_to_path_checked(link.href.as_str(), &target, item.hash.as_deref())
            .await?;

        self.index
            .set_state_with_meta(
                item.id,
                FileState::Cached,
                true,
                None,
                StateMeta {
                    retry_at: None,
                    last_success_at: Some(now_unix()),
                    last_error_at: None,
                    dirty: false,
                },
            )
            .await?;
        Ok(())
    }

    async fn ensure_cache_dir_for_remote(&self, path: &str) -> Result<(), EngineError> {
        let local = cache_path_for(&self.cache_root, path)?;
        if let Ok(meta) = tokio::fs::metadata(&local).await
            && meta.is_file()
        {
            tokio::fs::remove_file(&local).await?;
        }
        tokio::fs::create_dir_all(&local).await?;
        Ok(())
    }

    async fn execute_upload(&self, path: &str) -> Result<(), EngineError> {
        let source = cache_path_for(&self.cache_root, path)?;
        let link = self.client.get_upload_link(path, true).await?;
        self.transfer
            .upload_from_path(link.href.as_str(), &source)
            .await?;

        let item = self
            .index
            .get_item_by_path(path)
            .await?
            .ok_or_else(|| EngineError::MissingItem(path.to_string()))?;
        self.index
            .set_state_with_meta(
                item.id,
                FileState::Cached,
                true,
                None,
                StateMeta {
                    retry_at: None,
                    last_success_at: Some(now_unix()),
                    last_error_at: None,
                    dirty: false,
                },
            )
            .await?;
        Ok(())
    }

    async fn execute_mkdir(&self, path: &str) -> Result<(), EngineError> {
        let resource = self.client.create_folder(path).await?;
        let item = self
            .index
            .upsert_item(&ItemInput {
                path: resource.path.clone(),
                parent_path: parent_path(&resource.path),
                name: resource.name.clone(),
                item_type: ItemType::Dir,
                size: resource.size.map(|v| v as i64),
                modified: parse_modified(resource.modified.as_deref())?,
                hash: resource.md5.clone(),
                resource_id: resource.resource_id.clone(),
                last_synced_hash: resource.md5.clone(),
                last_synced_modified: parse_modified(resource.modified.as_deref())?,
            })
            .await?;
        self.index
            .set_state_with_meta(
                item.id,
                FileState::Cached,
                true,
                None,
                StateMeta {
                    retry_at: None,
                    last_success_at: Some(now_unix()),
                    last_error_at: None,
                    dirty: false,
                },
            )
            .await?;
        Ok(())
    }

    async fn execute_move_like_op(&self, op: &Operation) -> Result<(), EngineError> {
        let Some(payload) = &op.payload else {
            return Ok(());
        };
        let payload: MovePayload =
            serde_json::from_str(payload).map_err(|_| EngineError::OperationFailed)?;
        let source_item = self.index.get_item_by_path(&payload.from).await?;
        if payload.action != "copy"
            && source_item.is_none()
            && let Ok(target_local) = cache_path_for(&self.cache_root, &payload.path)
            && let Ok(meta) = tokio::fs::metadata(&target_local).await
        {
            if meta.is_dir() {
                return self.execute_mkdir(&payload.path).await;
            }
            if self.index.get_item_by_path(&payload.path).await?.is_none() {
                self.index
                    .upsert_item(&ItemInput {
                        path: payload.path.clone(),
                        parent_path: parent_path(&payload.path),
                        name: payload
                            .path
                            .split('/')
                            .next_back()
                            .unwrap_or(payload.path.as_str())
                            .to_string(),
                        item_type: ItemType::File,
                        size: Some(meta.len() as i64),
                        modified: None,
                        hash: None,
                        resource_id: None,
                        last_synced_hash: None,
                        last_synced_modified: None,
                    })
                    .await?;
            }
            return self.execute_upload(&payload.path).await;
        }
        let link = if payload.action == "copy" {
            self.client
                .copy_resource(&payload.from, &payload.path, payload.overwrite)
                .await?
        } else {
            self.client
                .move_resource(&payload.from, &payload.path, payload.overwrite)
                .await?
        };
        self.wait_for_operation(link.href.as_str()).await?;

        if let Some(source) = source_item {
            let mut input = ItemInput {
                path: payload.path.clone(),
                parent_path: parent_path(&payload.path),
                name: payload
                    .path
                    .split('/')
                    .next_back()
                    .unwrap_or(payload.path.as_str())
                    .to_string(),
                item_type: source.item_type.clone(),
                size: source.size,
                modified: source.modified,
                hash: source.hash.clone(),
                resource_id: source.resource_id.clone(),
                last_synced_hash: source.last_synced_hash.clone(),
                last_synced_modified: source.last_synced_modified,
            };
            if input.name.is_empty() {
                input.name = payload.path.clone();
            }
            let target = self.index.upsert_item(&input).await?;
            if let Some(state) = self.index.get_state(source.id).await? {
                self.index
                    .set_state_with_meta(
                        target.id,
                        state.state,
                        state.pinned,
                        state.last_error.as_deref(),
                        StateMeta {
                            retry_at: state.retry_at,
                            last_success_at: Some(now_unix()),
                            last_error_at: state.last_error_at,
                            dirty: false,
                        },
                    )
                    .await?;
            }
            if payload.action != "copy" {
                self.index.delete_item_by_path(&payload.from).await?;
            }
        }
        Ok(())
    }

    async fn wait_for_operation(&self, operation_url: &str) -> Result<(), EngineError> {
        for attempt in 0..10u32 {
            match self.client.get_operation_status(operation_url).await? {
                OperationStatus::Success => return Ok(()),
                OperationStatus::Failure => return Err(EngineError::OperationFailed),
                OperationStatus::InProgress => {
                    tokio::time::sleep(self.backoff.delay(attempt)).await;
                }
            }
        }
        Err(EngineError::OperationFailed)
    }

    async fn collect_remote_tree(
        &self,
        root: &str,
    ) -> Result<Vec<yadisk_core::Resource>, EngineError> {
        let mut stack = vec![root.to_string()];
        let mut out = Vec::new();
        while let Some(path) = stack.pop() {
            let items = self.client.list_directory_all(&path, 100, None).await?;
            for item in items {
                if item.resource_type == ResourceType::Dir {
                    stack.push(item.path.clone());
                }
                out.push(item);
            }
        }
        Ok(out)
    }
}
