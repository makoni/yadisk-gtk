#![allow(dead_code)]

use std::{fs, path::PathBuf};

use sqlx::{Row, SqlitePool, migrate::Migrator, sqlite::SqliteConnectOptions};
use thiserror::Error;

use super::queue::{Operation, OperationKind};

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("XDG data directory is unavailable")]
    MissingDataDir,
    #[error("invalid item type: {0}")]
    InvalidItemType(String),
    #[error("invalid file state: {0}")]
    InvalidState(String),
    #[error("invalid operation kind: {0}")]
    InvalidOperationKind(String),
    #[error("item not found after upsert")]
    MissingItem,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ItemType {
    File,
    Dir,
}

impl ItemType {
    fn as_str(&self) -> &'static str {
        match self {
            ItemType::File => "file",
            ItemType::Dir => "dir",
        }
    }

    fn parse(value: &str) -> Result<Self, IndexError> {
        match value {
            "file" => Ok(ItemType::File),
            "dir" => Ok(ItemType::Dir),
            other => Err(IndexError::InvalidItemType(other.to_string())),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ItemInput {
    pub path: String,
    pub parent_path: Option<String>,
    pub name: String,
    pub item_type: ItemType,
    pub size: Option<i64>,
    pub modified: Option<i64>,
    pub hash: Option<String>,
    pub resource_id: Option<String>,
    pub last_synced_hash: Option<String>,
    pub last_synced_modified: Option<i64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ItemRecord {
    pub id: i64,
    pub path: String,
    pub parent_path: Option<String>,
    pub name: String,
    pub item_type: ItemType,
    pub size: Option<i64>,
    pub modified: Option<i64>,
    pub hash: Option<String>,
    pub resource_id: Option<String>,
    pub last_synced_hash: Option<String>,
    pub last_synced_modified: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileState {
    CloudOnly,
    Cached,
    Syncing,
    Error,
}

impl FileState {
    fn as_str(&self) -> &'static str {
        match self {
            FileState::CloudOnly => "cloud_only",
            FileState::Cached => "cached",
            FileState::Syncing => "syncing",
            FileState::Error => "error",
        }
    }

    fn parse(value: &str) -> Result<Self, IndexError> {
        match value {
            "cloud_only" => Ok(FileState::CloudOnly),
            "cached" => Ok(FileState::Cached),
            "syncing" => Ok(FileState::Syncing),
            "error" => Ok(FileState::Error),
            other => Err(IndexError::InvalidState(other.to_string())),
        }
    }
}

fn operation_kind_as_str(kind: &OperationKind) -> &'static str {
    match kind {
        OperationKind::Upload => "upload",
        OperationKind::Download => "download",
        OperationKind::Delete => "delete",
        OperationKind::Move => "move",
        OperationKind::Mkdir => "mkdir",
    }
}

fn parse_operation_kind(value: &str) -> Result<OperationKind, IndexError> {
    match value {
        "upload" => Ok(OperationKind::Upload),
        "download" => Ok(OperationKind::Download),
        "delete" => Ok(OperationKind::Delete),
        "move" => Ok(OperationKind::Move),
        "mkdir" => Ok(OperationKind::Mkdir),
        other => Err(IndexError::InvalidOperationKind(other.to_string())),
    }
}

fn prefix_variants(prefix: &str) -> [String; 2] {
    if let Some(rest) = prefix.strip_prefix("disk:/") {
        let suffix = rest.trim_start_matches('/');
        let slash = if suffix.is_empty() {
            "/".to_string()
        } else {
            format!("/{suffix}")
        };
        return [prefix.to_string(), slash];
    }
    if let Some(rest) = prefix.strip_prefix('/') {
        let disk = if rest.is_empty() {
            "disk:/".to_string()
        } else {
            format!("disk:/{}", rest)
        };
        return [prefix.to_string(), disk];
    }
    [prefix.to_string(), prefix.to_string()]
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRecord {
    pub item_id: i64,
    pub state: FileState,
    pub pinned: bool,
    pub last_error: Option<String>,
    pub retry_at: Option<i64>,
    pub last_success_at: Option<i64>,
    pub last_error_at: Option<i64>,
    pub dirty: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StateMeta {
    pub retry_at: Option<i64>,
    pub last_success_at: Option<i64>,
    pub last_error_at: Option<i64>,
    pub dirty: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncCursor {
    pub cursor: Option<String>,
    pub last_sync: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictRecord {
    pub id: i64,
    pub path: String,
    pub renamed_local: String,
    pub created: i64,
    pub reason: String,
}

pub struct IndexStore {
    pool: SqlitePool,
}

include!("index_store_impl.rs");

fn default_db_path() -> Result<PathBuf, IndexError> {
    let mut path = dirs::data_dir().ok_or(IndexError::MissingDataDir)?;
    path.push("yadisk-gtk");
    path.push("sync");
    path.push("index.db");
    Ok(path)
}

#[cfg(test)]
#[path = "index_tests.rs"]
mod tests;
