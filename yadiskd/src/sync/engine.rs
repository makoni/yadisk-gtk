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
#[path = "engine_tests/mod.rs"]
mod tests;
