#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use md5::Context;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;
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
    #[error("upload size {size} exceeds server limit {max_size}")]
    UploadTooLarge { size: u64, max_size: u64 },
}

pub struct SyncEngine {
    client: YadiskClient,
    index: IndexStore,
    transfer: TransferClient,
    cache_root: PathBuf,
    backoff: Backoff,
    active_transfers: Arc<Mutex<HashMap<String, CancellationToken>>>,
    upload_limit_cache: Arc<Mutex<UploadLimitCache>>,
}

const MAX_RETRY_ATTEMPTS: u32 = 5;
const UPLOAD_LIMIT_CACHE_TTL: Duration = Duration::from_secs(300);
type UploadLimitCache = Option<(Option<u64>, Instant)>;

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

struct LocalFileVersion {
    hash: String,
    modified: i64,
    size: u64,
    meta: FileMetadata,
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
            active_transfers: Arc::new(Mutex::new(HashMap::new())),
            upload_limit_cache: Arc::new(Mutex::new(None)),
        }
    }

    pub fn with_transfer(mut self, transfer: TransferClient) -> Self {
        self.transfer = transfer;
        self
    }

    pub fn cancel_transfer(&self, path: &str) {
        let mut map = self
            .active_transfers
            .lock()
            .expect("transfer mutex poisoned");
        for key in path_variants(path) {
            if let Some(token) = map.remove(&key) {
                token.cancel();
            }
        }
    }

    pub fn cancel_all_transfers(&self) {
        let mut map = self
            .active_transfers
            .lock()
            .expect("transfer mutex poisoned");
        for (_, token) in map.drain() {
            token.cancel();
        }
    }

    pub async fn has_active_or_queued_work(&self) -> Result<bool, EngineError> {
        if !self
            .active_transfers
            .lock()
            .expect("transfer mutex poisoned")
            .is_empty()
        {
            return Ok(true);
        }
        Ok(self.index.has_ready_op().await?)
    }

    fn register_transfer_token(&self, path: &str) -> CancellationToken {
        let token = CancellationToken::new();
        let mut map = self
            .active_transfers
            .lock()
            .expect("transfer mutex poisoned");
        for key in path_variants(path) {
            map.insert(key, token.clone());
        }
        token
    }

    fn unregister_transfer_token(&self, path: &str) {
        let mut map = self
            .active_transfers
            .lock()
            .expect("transfer mutex poisoned");
        for key in path_variants(path) {
            map.remove(&key);
        }
    }

    async fn max_upload_size(&self) -> Option<u64> {
        let cached_entry = *self
            .upload_limit_cache
            .lock()
            .expect("upload limit mutex poisoned");
        if let Some((cached, fetched_at)) = cached_entry
            && fetched_at.elapsed() < UPLOAD_LIMIT_CACHE_TTL
        {
            return cached;
        }

        match self.client.get_disk_info().await {
            Ok(info) => {
                let value = info.max_file_size;
                *self
                    .upload_limit_cache
                    .lock()
                    .expect("upload limit mutex poisoned") = Some((value, Instant::now()));
                value
            }
            Err(_) => (*self
                .upload_limit_cache
                .lock()
                .expect("upload limit mutex poisoned"))
            .and_then(|(value, _)| value),
        }
    }

    fn refresh_upload_limit_cache(&self) {
        *self
            .upload_limit_cache
            .lock()
            .expect("upload limit mutex poisoned") = None;
    }
}

include!("engine_impl_core.rs");
include!("engine_impl_ops.rs");

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

fn path_variants(path: &str) -> [String; 2] {
    if let Some(rest) = path.strip_prefix("disk:/") {
        let slash = format!("/{}", rest.trim_start_matches('/'));
        return [path.to_string(), slash];
    }
    if let Some(rest) = path.strip_prefix('/') {
        let disk = if rest.is_empty() {
            "disk:/".to_string()
        } else {
            format!("disk:/{}", rest)
        };
        return [path.to_string(), disk];
    }
    [path.to_string(), path.to_string()]
}

fn is_transient_error(err: &EngineError) -> bool {
    match err {
        EngineError::Api(yadisk_core::YadiskError::Api { status, .. })
            if matches!(
                *status,
                reqwest::StatusCode::PAYLOAD_TOO_LARGE | reqwest::StatusCode::INSUFFICIENT_STORAGE
            ) =>
        {
            false
        }
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
#[path = "engine_tests/mod.rs"]
mod tests;
