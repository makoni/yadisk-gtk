#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use yadisk_core::{ApiErrorClass, OperationStatus, ResourceType, YadiskClient};

use super::backoff::Backoff;
use super::conflict::{self, ConflictDecision, FileMetadata};
use super::index::{
    ConflictRecord, FileState, IndexError, IndexStore, ItemInput, ItemRecord, ItemType, StateMeta,
};
use super::local_watcher::LocalEvent;
use super::paths::{PathError, cache_path_for};
use super::queue::{Operation, OperationKind};
use super::transfer::{TransferClient, TransferError};

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("index error: {0}")]
    Index(#[from] IndexError),
    #[error("api error: {0}")]
    Api(#[from] yadisk_core::YadiskError),
    #[error("transfer error: {0}")]
    Transfer(#[from] TransferError),
    #[error("path error: {0}")]
    Path(#[from] PathError),
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("time parse error: {0}")]
    Time(#[from] time::error::Parse),
    #[error("item not found for path: {0}")]
    MissingItem(String),
    #[error("operation failed")]
    OperationFailed,
}

pub struct SyncEngine {
    client: YadiskClient,
    index: IndexStore,
    transfer: TransferClient,
    cache_root: PathBuf,
    backoff: Backoff,
}

const MAX_RETRY_ATTEMPTS: u32 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathDisplayState {
    CloudOnly,
    Cached,
    Syncing,
    Error,
    Partial,
}

impl PathDisplayState {
    fn from_file_state(state: FileState) -> Self {
        match state {
            FileState::CloudOnly => Self::CloudOnly,
            FileState::Cached => Self::Cached,
            FileState::Syncing => Self::Syncing,
            FileState::Error => Self::Error,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SyncDelta {
    pub indexed: usize,
    pub deleted: usize,
    pub enqueued_downloads: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MovePayload {
    from: String,
    path: String,
    #[serde(default)]
    overwrite: bool,
    #[serde(default = "default_move_action")]
    action: String,
}

fn default_move_action() -> String {
    "move".to_string()
}

impl SyncEngine {
    pub fn new(client: YadiskClient, index: IndexStore, cache_root: PathBuf) -> Self {
        Self {
            client,
            index,
            transfer: TransferClient::new(),
            cache_root,
            backoff: Backoff::new(
                std::time::Duration::from_millis(250),
                std::time::Duration::from_secs(10),
                true,
            ),
        }
    }

    pub fn with_transfer(mut self, transfer: TransferClient) -> Self {
        self.transfer = transfer;
        self
    }

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
                kind: OperationKind::Upload,
                path: path.to_string(),
                payload: None,
                attempt: 0,
                retry_at: None,
                priority: 50,
            })
            .await?)
    }

    pub async fn enqueue_delete(&self, path: &str) -> Result<i64, EngineError> {
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
                let retry_after =
                    now_unix().saturating_add(self.backoff.delay(op.attempt + 1).as_secs() as i64);
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
            eprintln!(
                "[yadiskd] op failed: kind={:?} path={} err={}",
                op.kind, op.path, err
            );
            return Err(err);
        }

        eprintln!("[yadiskd] op done: kind={:?} path={}", op.kind, op.path);
        Ok(true)
    }

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

    async fn execute_move_like_op(&self, op: &Operation) -> Result<(), EngineError> {
        let Some(payload) = &op.payload else {
            return Ok(());
        };
        let payload: MovePayload =
            serde_json::from_str(payload).map_err(|_| EngineError::OperationFailed)?;
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

        if let Some(source) = self.index.get_item_by_path(&payload.from).await? {
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

fn parse_modified(value: Option<&str>) -> Result<Option<i64>, time::error::Parse> {
    let Some(value) = value else {
        return Ok(None);
    };
    let parsed = OffsetDateTime::parse(value, &Rfc3339)?;
    Ok(Some(parsed.unix_timestamp()))
}

fn parent_path(path: &str) -> Option<String> {
    let trimmed = path.trim_end_matches('/');
    trimmed.rfind('/').map(|idx| {
        if idx == 0 {
            "/".to_string()
        } else {
            trimmed[..idx].to_string()
        }
    })
}

fn state_for_path_variant(states: &HashMap<String, FileState>, path: &str) -> Option<FileState> {
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

fn now_unix() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn is_transient_error(err: &EngineError) -> bool {
    match err {
        EngineError::Api(api) => matches!(
            api.classification(),
            Some(ApiErrorClass::RateLimit | ApiErrorClass::Transient)
        ),
        EngineError::Io(_)
        | EngineError::Transfer(TransferError::Request(_))
        | EngineError::Transfer(TransferError::Io(_))
        | EngineError::Transfer(TransferError::ConcurrencyClosed)
        | EngineError::OperationFailed => true,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::SqlitePool;
    use std::path::Path;
    use tempfile::tempdir;
    use wiremock::matchers::{body_bytes, header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn make_engine(server: &MockServer, cache_root: &Path) -> SyncEngine {
        let client = YadiskClient::with_base_url(&server.uri(), "test-token").unwrap();
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let store = IndexStore::from_pool(pool);
        store.init().await.unwrap();
        SyncEngine::new(client, store, cache_root.to_path_buf())
    }

    #[tokio::test]
    async fn sync_directory_once_upserts_items() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/disk/resources"))
            .and(query_param("path", "/Docs"))
            .and(query_param("limit", "100"))
            .and(query_param("offset", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "_embedded": {
                    "limit": 100,
                    "offset": 0,
                    "total": 1,
                    "items": [
                        {
                            "path": "/Docs/A.txt",
                            "name": "A.txt",
                            "type": "file",
                            "size": 1,
                            "modified": "2024-01-01T00:00:00Z"
                        }
                    ]
                }
            })))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;
        assert_eq!(engine.sync_directory_once("/Docs").await.unwrap(), 1);

        let item = engine
            .index
            .get_item_by_path("/Docs/A.txt")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(item.name, "A.txt");
        assert_eq!(item.item_type, ItemType::File);
        let state = engine.index.get_state(item.id).await.unwrap().unwrap();
        assert_eq!(state.state, FileState::CloudOnly);
    }

    #[tokio::test]
    async fn run_once_download_fetches_file_and_sets_cached_state() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/v1/disk/resources/download"))
            .and(query_param("path", "/Docs/A.txt"))
            .and(header("authorization", "OAuth test-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "href": format!("{}/file", server.uri()),
                "method": "GET",
                "templated": false
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/file"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello"))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;
        engine
            .index
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

        engine.enqueue_download("/Docs/A.txt").await.unwrap();
        assert!(engine.run_once().await.unwrap());

        let target = cache_path_for(dir.path(), "/Docs/A.txt").unwrap();
        assert_eq!(std::fs::read(target).unwrap(), b"hello");

        let item = engine
            .index
            .get_item_by_path("/Docs/A.txt")
            .await
            .unwrap()
            .unwrap();
        let state = engine.index.get_state(item.id).await.unwrap().unwrap();
        assert_eq!(state.state, FileState::Cached);
    }

    #[tokio::test]
    async fn run_once_download_on_directory_queues_child_files_and_repairs_cache_dir() {
        let server = MockServer::start().await;
        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;

        let music_dir = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Music".into(),
                parent_path: Some("/".into()),
                name: "Music".into(),
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
        let song_a = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Music/A.mp3".into(),
                parent_path: Some("/Music".into()),
                name: "A.mp3".into(),
                item_type: ItemType::File,
                size: Some(1),
                modified: None,
                hash: None,
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();
        let _sub_dir = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Music/Sub".into(),
                parent_path: Some("/Music".into()),
                name: "Sub".into(),
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
        let song_b = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Music/Sub/B.mp3".into(),
                parent_path: Some("/Music/Sub".into()),
                name: "B.mp3".into(),
                item_type: ItemType::File,
                size: Some(1),
                modified: None,
                hash: None,
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();

        let broken_cache = cache_path_for(dir.path(), "/Music").unwrap();
        std::fs::write(&broken_cache, b"broken-file-instead-of-dir").unwrap();

        engine.enqueue_download("/Music").await.unwrap();
        assert!(engine.run_once().await.unwrap());

        assert!(std::fs::metadata(&broken_cache).unwrap().is_dir());

        let dir_state = engine.index.get_state(music_dir.id).await.unwrap().unwrap();
        assert_eq!(dir_state.state, FileState::Cached);
        assert!(dir_state.pinned);

        let state_a = engine.index.get_state(song_a.id).await.unwrap().unwrap();
        assert_eq!(state_a.state, FileState::Syncing);
        assert!(state_a.pinned);

        let state_b = engine.index.get_state(song_b.id).await.unwrap().unwrap();
        assert_eq!(state_b.state, FileState::Syncing);
        assert!(state_b.pinned);

        let op1 = engine.index.dequeue_op().await.unwrap().unwrap();
        let op2 = engine.index.dequeue_op().await.unwrap().unwrap();
        let queued: HashSet<String> = [op1.path, op2.path].into_iter().collect();
        assert_eq!(
            queued,
            HashSet::from(["/Music/A.mp3".to_string(), "/Music/Sub/B.mp3".to_string()])
        );
    }

    #[tokio::test]
    async fn evict_directory_applies_recursively_and_removes_cache_tree() {
        let server = MockServer::start().await;
        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;

        let music = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Music".into(),
                parent_path: Some("/".into()),
                name: "Music".into(),
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
        let a = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Music/A.mp3".into(),
                parent_path: Some("/Music".into()),
                name: "A.mp3".into(),
                item_type: ItemType::File,
                size: Some(1),
                modified: None,
                hash: None,
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();
        let b = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Music/B.mp3".into(),
                parent_path: Some("/Music".into()),
                name: "B.mp3".into(),
                item_type: ItemType::File,
                size: Some(1),
                modified: None,
                hash: None,
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();
        engine
            .index
            .set_state(music.id, FileState::Cached, true, None)
            .await
            .unwrap();
        engine
            .index
            .set_state(a.id, FileState::Cached, true, None)
            .await
            .unwrap();
        engine
            .index
            .set_state(b.id, FileState::Cached, true, None)
            .await
            .unwrap();

        let cache_root = cache_path_for(dir.path(), "/Music").unwrap();
        std::fs::create_dir_all(&cache_root).unwrap();
        std::fs::write(cache_root.join("A.mp3"), b"a").unwrap();
        std::fs::write(cache_root.join("B.mp3"), b"b").unwrap();

        engine.evict_path("/Music").await.unwrap();

        let s_music = engine.index.get_state(music.id).await.unwrap().unwrap();
        let s_a = engine.index.get_state(a.id).await.unwrap().unwrap();
        let s_b = engine.index.get_state(b.id).await.unwrap().unwrap();
        assert_eq!(s_music.state, FileState::CloudOnly);
        assert_eq!(s_a.state, FileState::CloudOnly);
        assert_eq!(s_b.state, FileState::CloudOnly);
        assert!(!s_music.pinned && !s_a.pinned && !s_b.pinned);
        assert!(!cache_root.exists());
    }

    #[tokio::test]
    async fn state_for_directory_reports_partial_when_files_are_mixed() {
        let server = MockServer::start().await;
        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;

        let music = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Music".into(),
                parent_path: Some("/".into()),
                name: "Music".into(),
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
        let a = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Music/A.mp3".into(),
                parent_path: Some("/Music".into()),
                name: "A.mp3".into(),
                item_type: ItemType::File,
                size: Some(1),
                modified: None,
                hash: None,
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();
        let b = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Music/B.mp3".into(),
                parent_path: Some("/Music".into()),
                name: "B.mp3".into(),
                item_type: ItemType::File,
                size: Some(1),
                modified: None,
                hash: None,
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();
        engine
            .index
            .set_state(music.id, FileState::CloudOnly, false, None)
            .await
            .unwrap();
        engine
            .index
            .set_state(a.id, FileState::Cached, true, None)
            .await
            .unwrap();
        engine
            .index
            .set_state(b.id, FileState::CloudOnly, false, None)
            .await
            .unwrap();

        assert_eq!(
            engine.state_for_path("/Music").await.unwrap(),
            Some(PathDisplayState::Partial)
        );
    }

    #[tokio::test]
    async fn pin_directory_applies_recursively() {
        let server = MockServer::start().await;
        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;

        let music = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Music".into(),
                parent_path: Some("/".into()),
                name: "Music".into(),
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
        let a = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Music/A.mp3".into(),
                parent_path: Some("/Music".into()),
                name: "A.mp3".into(),
                item_type: ItemType::File,
                size: Some(1),
                modified: None,
                hash: None,
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();
        let sub = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Music/Sub".into(),
                parent_path: Some("/Music".into()),
                name: "Sub".into(),
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
        let b = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Music/Sub/B.mp3".into(),
                parent_path: Some("/Music/Sub".into()),
                name: "B.mp3".into(),
                item_type: ItemType::File,
                size: Some(1),
                modified: None,
                hash: None,
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();
        engine
            .index
            .set_state(music.id, FileState::CloudOnly, false, None)
            .await
            .unwrap();
        engine
            .index
            .set_state(a.id, FileState::Cached, false, None)
            .await
            .unwrap();
        engine
            .index
            .set_state(sub.id, FileState::CloudOnly, false, None)
            .await
            .unwrap();
        engine
            .index
            .set_state(b.id, FileState::CloudOnly, false, None)
            .await
            .unwrap();

        engine.pin_path("/Music", true).await.unwrap();

        for id in [music.id, a.id, sub.id, b.id] {
            let state = engine.index.get_state(id).await.unwrap().unwrap();
            assert!(state.pinned);
        }
    }

    #[tokio::test]
    async fn state_for_directory_reports_cached_when_all_files_cached() {
        let server = MockServer::start().await;
        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;

        let music = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Music".into(),
                parent_path: Some("/".into()),
                name: "Music".into(),
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
        let a = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Music/A.mp3".into(),
                parent_path: Some("/Music".into()),
                name: "A.mp3".into(),
                item_type: ItemType::File,
                size: Some(1),
                modified: None,
                hash: None,
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();
        let b = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Music/B.mp3".into(),
                parent_path: Some("/Music".into()),
                name: "B.mp3".into(),
                item_type: ItemType::File,
                size: Some(1),
                modified: None,
                hash: None,
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();
        engine
            .index
            .set_state(music.id, FileState::CloudOnly, true, None)
            .await
            .unwrap();
        engine
            .index
            .set_state(a.id, FileState::Cached, true, None)
            .await
            .unwrap();
        engine
            .index
            .set_state(b.id, FileState::Cached, true, None)
            .await
            .unwrap();

        assert_eq!(
            engine.state_for_path("/Music").await.unwrap(),
            Some(PathDisplayState::Cached)
        );
    }

    #[tokio::test]
    async fn state_for_directory_prioritizes_error_and_syncing() {
        let server = MockServer::start().await;
        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;

        let _music = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Music".into(),
                parent_path: Some("/".into()),
                name: "Music".into(),
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
        let a = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Music/A.mp3".into(),
                parent_path: Some("/Music".into()),
                name: "A.mp3".into(),
                item_type: ItemType::File,
                size: Some(1),
                modified: None,
                hash: None,
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();
        let b = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Music/B.mp3".into(),
                parent_path: Some("/Music".into()),
                name: "B.mp3".into(),
                item_type: ItemType::File,
                size: Some(1),
                modified: None,
                hash: None,
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();
        let c = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Music/C.mp3".into(),
                parent_path: Some("/Music".into()),
                name: "C.mp3".into(),
                item_type: ItemType::File,
                size: Some(1),
                modified: None,
                hash: None,
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();

        engine
            .index
            .set_state(a.id, FileState::Cached, true, None)
            .await
            .unwrap();
        engine
            .index
            .set_state(b.id, FileState::Syncing, true, None)
            .await
            .unwrap();
        engine
            .index
            .set_state(c.id, FileState::CloudOnly, false, None)
            .await
            .unwrap();
        assert_eq!(
            engine.state_for_path("/Music").await.unwrap(),
            Some(PathDisplayState::Syncing)
        );

        engine
            .index
            .set_state(b.id, FileState::Error, true, Some("x"))
            .await
            .unwrap();
        assert_eq!(
            engine.state_for_path("/Music").await.unwrap(),
            Some(PathDisplayState::Error)
        );
    }

    #[tokio::test]
    async fn run_once_upload_sends_file_and_sets_cached_state() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/v1/disk/resources/upload"))
            .and(query_param("path", "/Docs/A.txt"))
            .and(query_param("overwrite", "true"))
            .and(header("authorization", "OAuth test-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "href": format!("{}/upload", server.uri()),
                "method": "PUT",
                "templated": false
            })))
            .mount(&server)
            .await;

        Mock::given(method("PUT"))
            .and(path("/upload"))
            .and(body_bytes(b"payload"))
            .respond_with(ResponseTemplate::new(201))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;
        engine
            .index
            .upsert_item(&ItemInput {
                path: "/Docs/A.txt".into(),
                parent_path: Some("/Docs".into()),
                name: "A.txt".into(),
                item_type: ItemType::File,
                size: Some(7),
                modified: None,
                hash: None,
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();

        let target = cache_path_for(dir.path(), "/Docs/A.txt").unwrap();
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(&target, b"payload").unwrap();

        engine.enqueue_upload("/Docs/A.txt").await.unwrap();
        assert!(engine.run_once().await.unwrap());

        let item = engine
            .index
            .get_item_by_path("/Docs/A.txt")
            .await
            .unwrap()
            .unwrap();
        let state = engine.index.get_state(item.id).await.unwrap().unwrap();
        assert_eq!(state.state, FileState::Cached);
    }

    #[tokio::test]
    async fn sync_directory_incremental_handles_rename_delete_and_pinned_download() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/disk/resources"))
            .and(query_param("path", "/Docs"))
            .and(query_param("limit", "100"))
            .and(query_param("offset", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "_embedded": {
                    "limit": 100,
                    "offset": 0,
                    "total": 2,
                    "items": [
                        {"path": "/Docs/New.txt", "name": "New.txt", "type": "file", "size": 10, "resource_id": "rid-1", "md5": "abcd"},
                        {"path": "/Docs/Sub", "name": "Sub", "type": "dir", "resource_id": "rid-sub"}
                    ]
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/disk/resources"))
            .and(query_param("path", "/Docs/Sub"))
            .and(query_param("limit", "100"))
            .and(query_param("offset", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "_embedded": {
                    "limit": 100,
                    "offset": 0,
                    "total": 1,
                    "items": [
                        {"path": "/Docs/Sub/B.txt", "name": "B.txt", "type": "file", "size": 5, "resource_id": "rid-2", "md5": "ef01"}
                    ]
                }
            })))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;

        let old = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Docs/Old.txt".into(),
                parent_path: Some("/Docs".into()),
                name: "Old.txt".into(),
                item_type: ItemType::File,
                size: Some(10),
                modified: None,
                hash: Some("abcd".into()),
                resource_id: Some("rid-1".into()),
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();
        engine
            .index
            .set_state(old.id, FileState::CloudOnly, true, None)
            .await
            .unwrap();

        engine
            .index
            .upsert_item(&ItemInput {
                path: "/Docs/Stale.txt".into(),
                parent_path: Some("/Docs".into()),
                name: "Stale.txt".into(),
                item_type: ItemType::File,
                size: Some(1),
                modified: None,
                hash: None,
                resource_id: Some("rid-3".into()),
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();

        let delta = engine.sync_directory_incremental("/Docs").await.unwrap();
        assert_eq!(delta.deleted, 2);
        assert_eq!(delta.enqueued_downloads, 1);
        assert!(
            engine
                .index
                .get_item_by_path("/Docs/Old.txt")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            engine
                .index
                .get_item_by_path("/Docs/Stale.txt")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            engine
                .index
                .get_item_by_path("/Docs/New.txt")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            engine
                .index
                .get_item_by_path("/Docs/Sub/B.txt")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn ingest_local_events_enqueue_upload_delete_and_move() {
        let server = MockServer::start().await;
        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;
        engine
            .index
            .upsert_item(&ItemInput {
                path: "/Docs/A.txt".into(),
                parent_path: Some("/Docs".into()),
                name: "A.txt".into(),
                item_type: ItemType::File,
                size: Some(1),
                modified: None,
                hash: None,
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();

        engine
            .ingest_local_event(LocalEvent::Upload {
                path: "/Docs/A.txt".into(),
            })
            .await
            .unwrap();
        engine
            .ingest_local_event(LocalEvent::Delete {
                path: "/Docs/A.txt".into(),
            })
            .await
            .unwrap();
        engine
            .ingest_local_event(LocalEvent::Move {
                from: "/Docs/A.txt".into(),
                to: "/Docs/B.txt".into(),
            })
            .await
            .unwrap();

        let first = engine.index.dequeue_op().await.unwrap().unwrap();
        assert_eq!(first.kind, OperationKind::Delete);
        let second = engine.index.dequeue_op().await.unwrap().unwrap();
        assert_eq!(second.kind, OperationKind::Move);
    }

    #[tokio::test]
    async fn conflict_resolution_keep_both_records_conflict() {
        let server = MockServer::start().await;
        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;
        let base = FileMetadata {
            modified: 1,
            hash: Some("base".into()),
        };
        let local = FileMetadata {
            modified: 2,
            hash: Some("local".into()),
        };
        let remote = FileMetadata {
            modified: 3,
            hash: Some("remote".into()),
        };
        let decision = engine
            .resolve_conflict_and_record("/Docs/A.txt", Some(&base), &local, &remote)
            .await
            .unwrap();
        assert!(matches!(decision, ConflictDecision::KeepBoth { .. }));
        let conflicts = engine.index.list_conflicts().await.unwrap();
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].path, "/Docs/A.txt");
    }

    #[tokio::test]
    async fn run_once_move_uses_payload_and_updates_index() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/v1/disk/resources/move"))
            .and(query_param("from", "/Docs/A.txt"))
            .and(query_param("path", "/Docs/B.txt"))
            .respond_with(ResponseTemplate::new(202).set_body_json(serde_json::json!({
                "href": format!("{}/v1/disk/operations/77", server.uri()),
                "method": "GET",
                "templated": false
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/disk/operations/77"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "success"
            })))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;
        engine
            .index
            .upsert_item(&ItemInput {
                path: "/Docs/A.txt".into(),
                parent_path: Some("/Docs".into()),
                name: "A.txt".into(),
                item_type: ItemType::File,
                size: Some(1),
                modified: None,
                hash: None,
                resource_id: Some("rid-1".into()),
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();
        engine
            .enqueue_move("/Docs/A.txt", "/Docs/B.txt", "move")
            .await
            .unwrap();
        assert!(engine.run_once().await.unwrap());
        assert!(
            engine
                .index
                .get_item_by_path("/Docs/A.txt")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            engine
                .index
                .get_item_by_path("/Docs/B.txt")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn e2e_sync_loop_cloud_list_to_cached_state() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/disk/resources"))
            .and(query_param("path", "/Docs"))
            .and(query_param("limit", "100"))
            .and(query_param("offset", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "_embedded": {
                    "limit": 100,
                    "offset": 0,
                    "total": 1,
                    "items": [
                        {"path": "/Docs/A.txt", "name": "A.txt", "type": "file", "size": 5, "resource_id": "rid-1", "md5": "5d41402abc4b2a76b9719d911017c592"}
                    ]
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/disk/resources/download"))
            .and(query_param("path", "/Docs/A.txt"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "href": format!("{}/file", server.uri()),
                "method": "GET",
                "templated": false
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/file"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello"))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;
        assert_eq!(engine.sync_directory_once("/Docs").await.unwrap(), 1);
        engine.enqueue_download("/Docs/A.txt").await.unwrap();
        assert!(engine.run_once().await.unwrap());

        let item = engine
            .index
            .get_item_by_path("/Docs/A.txt")
            .await
            .unwrap()
            .unwrap();
        let state = engine.index.get_state(item.id).await.unwrap().unwrap();
        assert_eq!(state.state, FileState::Cached);
    }

    #[tokio::test]
    async fn run_once_does_not_requeue_permanent_errors() {
        let server = MockServer::start().await;
        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;
        let op = Operation {
            kind: OperationKind::Download,
            path: "/Docs/Missing.txt".into(),
            payload: None,
            attempt: 0,
            retry_at: None,
            priority: 10,
        };
        engine.index.enqueue_op(&op).await.unwrap();

        let err = engine
            .run_once()
            .await
            .expect_err("expected permanent error");
        assert!(matches!(err, EngineError::MissingItem(_)));
        assert!(engine.index.dequeue_op().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn run_once_stops_requeue_at_max_attempts() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/disk/resources/download"))
            .and(query_param("path", "/Docs/A.txt"))
            .respond_with(ResponseTemplate::new(503).set_body_string("temporary error"))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;
        engine
            .index
            .upsert_item(&ItemInput {
                path: "/Docs/A.txt".into(),
                parent_path: Some("/Docs".into()),
                name: "A.txt".into(),
                item_type: ItemType::File,
                size: Some(1),
                modified: None,
                hash: None,
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();
        engine
            .index
            .enqueue_op(&Operation {
                kind: OperationKind::Download,
                path: "/Docs/A.txt".into(),
                payload: None,
                attempt: MAX_RETRY_ATTEMPTS - 1,
                retry_at: None,
                priority: 10,
            })
            .await
            .unwrap();

        let err = engine
            .run_once()
            .await
            .expect_err("expected max-attempt failure");
        assert!(matches!(err, EngineError::Api(_)));
        assert!(engine.index.dequeue_op().await.unwrap().is_none());
    }
}
