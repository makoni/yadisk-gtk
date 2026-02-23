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
    }
}

fn parse_operation_kind(value: &str) -> Result<OperationKind, IndexError> {
    match value {
        "upload" => Ok(OperationKind::Upload),
        "download" => Ok(OperationKind::Download),
        "delete" => Ok(OperationKind::Delete),
        "move" => Ok(OperationKind::Move),
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

impl IndexStore {
    pub fn from_pool(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn new(database_url: &str) -> Result<Self, IndexError> {
        let pool = SqlitePool::connect(database_url).await?;
        let store = Self { pool };
        store.init().await?;
        Ok(store)
    }

    pub async fn new_default() -> Result<Self, IndexError> {
        let db_path = default_db_path()?;
        if let Some(parent) = db_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let options = SqliteConnectOptions::new()
            .filename(&db_path)
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await?;
        let store = Self { pool };
        store.init().await?;
        Ok(store)
    }

    pub async fn init(&self) -> Result<(), IndexError> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    pub async fn upsert_item(&self, item: &ItemInput) -> Result<ItemRecord, IndexError> {
        sqlx::query(
            "\n            INSERT INTO items (\n                path,\n                parent_path,\n                name,\n                item_type,\n                size,\n                modified,\n                hash,\n                resource_id,\n                last_synced_hash,\n                last_synced_modified\n            )\n            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)\n            ON CONFLICT(path) DO UPDATE SET\n                parent_path = excluded.parent_path,\n                name = excluded.name,\n                item_type = excluded.item_type,\n                size = excluded.size,\n                modified = excluded.modified,\n                hash = excluded.hash,\n                resource_id = excluded.resource_id,\n                last_synced_hash = excluded.last_synced_hash,\n                last_synced_modified = excluded.last_synced_modified;\n            ",
        )
        .bind(&item.path)
        .bind(&item.parent_path)
        .bind(&item.name)
        .bind(item.item_type.as_str())
        .bind(item.size)
        .bind(item.modified)
        .bind(&item.hash)
        .bind(&item.resource_id)
        .bind(&item.last_synced_hash)
        .bind(item.last_synced_modified)
        .execute(&self.pool)
        .await?;

        self.get_item_by_path(&item.path)
            .await?
            .ok_or(IndexError::MissingItem)
    }

    pub async fn get_item_by_path(&self, path: &str) -> Result<Option<ItemRecord>, IndexError> {
        let row = sqlx::query(
            "SELECT id, path, parent_path, name, item_type, size, modified, hash, resource_id, last_synced_hash, last_synced_modified FROM items WHERE path = ?1",
        )
        .bind(path)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let item_type: String = row.try_get("item_type")?;
        Ok(Some(ItemRecord {
            id: row.try_get("id")?,
            path: row.try_get("path")?,
            parent_path: row.try_get("parent_path")?,
            name: row.try_get("name")?,
            item_type: ItemType::parse(&item_type)?,
            size: row.try_get("size")?,
            modified: row.try_get("modified")?,
            hash: row.try_get("hash")?,
            resource_id: row.try_get("resource_id")?,
            last_synced_hash: row.try_get("last_synced_hash")?,
            last_synced_modified: row.try_get("last_synced_modified")?,
        }))
    }

    pub async fn list_items_by_prefix(&self, prefix: &str) -> Result<Vec<ItemRecord>, IndexError> {
        let [prefix_a, prefix_b] = prefix_variants(prefix);
        let pattern_a = format!("{}/%", prefix_a.trim_end_matches('/'));
        let pattern_b = format!("{}/%", prefix_b.trim_end_matches('/'));
        let rows = sqlx::query(
            "SELECT id, path, parent_path, name, item_type, size, modified, hash, resource_id, last_synced_hash, last_synced_modified
             FROM items
             WHERE path = ?1 OR path LIKE ?2 OR path = ?3 OR path LIKE ?4
             ORDER BY path ASC",
        )
        .bind(prefix_a)
        .bind(pattern_a)
        .bind(prefix_b)
        .bind(pattern_b)
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let item_type: String = row.try_get("item_type")?;
            out.push(ItemRecord {
                id: row.try_get("id")?,
                path: row.try_get("path")?,
                parent_path: row.try_get("parent_path")?,
                name: row.try_get("name")?,
                item_type: ItemType::parse(&item_type)?,
                size: row.try_get("size")?,
                modified: row.try_get("modified")?,
                hash: row.try_get("hash")?,
                resource_id: row.try_get("resource_id")?,
                last_synced_hash: row.try_get("last_synced_hash")?,
                last_synced_modified: row.try_get("last_synced_modified")?,
            });
        }
        Ok(out)
    }

    pub async fn delete_item_by_path(&self, path: &str) -> Result<(), IndexError> {
        sqlx::query("DELETE FROM items WHERE path = ?1")
            .bind(path)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn set_state(
        &self,
        item_id: i64,
        state: FileState,
        pinned: bool,
        last_error: Option<&str>,
    ) -> Result<(), IndexError> {
        self.set_state_with_meta(item_id, state, pinned, last_error, StateMeta::default())
            .await
    }

    pub async fn set_state_with_meta(
        &self,
        item_id: i64,
        state: FileState,
        pinned: bool,
        last_error: Option<&str>,
        meta: StateMeta,
    ) -> Result<(), IndexError> {
        sqlx::query(
            "\n            INSERT INTO states (\n                item_id,\n                state,\n                pinned,\n                last_error,\n                retry_at,\n                last_success_at,\n                last_error_at,\n                dirty\n            )\n            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)\n            ON CONFLICT(item_id) DO UPDATE SET\n                state = excluded.state,\n                pinned = excluded.pinned,\n                last_error = excluded.last_error,\n                retry_at = excluded.retry_at,\n                last_success_at = excluded.last_success_at,\n                last_error_at = excluded.last_error_at,\n                dirty = excluded.dirty;\n            ",
        )
        .bind(item_id)
        .bind(state.as_str())
        .bind(if pinned { 1 } else { 0 })
        .bind(last_error)
        .bind(meta.retry_at)
        .bind(meta.last_success_at)
        .bind(meta.last_error_at)
        .bind(if meta.dirty { 1 } else { 0 })
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_state(&self, item_id: i64) -> Result<Option<StateRecord>, IndexError> {
        let row =
            sqlx::query(
                "SELECT item_id, state, pinned, last_error, retry_at, last_success_at, last_error_at, dirty FROM states WHERE item_id = ?1",
            )
                .bind(item_id)
                .fetch_optional(&self.pool)
                .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let state: String = row.try_get("state")?;
        let pinned: i64 = row.try_get("pinned")?;
        let dirty: i64 = row.try_get("dirty")?;

        Ok(Some(StateRecord {
            item_id: row.try_get("item_id")?,
            state: FileState::parse(&state)?,
            pinned: pinned != 0,
            last_error: row.try_get("last_error")?,
            retry_at: row.try_get("retry_at")?,
            last_success_at: row.try_get("last_success_at")?,
            last_error_at: row.try_get("last_error_at")?,
            dirty: dirty != 0,
        }))
    }

    pub async fn list_states_by_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, FileState)>, IndexError> {
        let [prefix_a, prefix_b] = prefix_variants(prefix);
        let pattern_a = format!("{}/%", prefix_a.trim_end_matches('/'));
        let pattern_b = format!("{}/%", prefix_b.trim_end_matches('/'));
        let rows = sqlx::query(
            "SELECT i.path, s.state
             FROM states s
             JOIN items i ON i.id = s.item_id
             WHERE i.path = ?1 OR i.path LIKE ?2 OR i.path = ?3 OR i.path LIKE ?4
             ORDER BY i.path ASC",
        )
        .bind(prefix_a)
        .bind(pattern_a)
        .bind(prefix_b)
        .bind(pattern_b)
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let path: String = row.try_get("path")?;
            let state: String = row.try_get("state")?;
            out.push((path, FileState::parse(&state)?));
        }
        Ok(out)
    }

    pub async fn list_path_states_with_pin_by_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<(String, FileState, bool)>, IndexError> {
        let [prefix_a, prefix_b] = prefix_variants(prefix);
        let pattern_a = format!("{}/%", prefix_a.trim_end_matches('/'));
        let pattern_b = format!("{}/%", prefix_b.trim_end_matches('/'));
        let rows = sqlx::query(
            "SELECT i.path, s.state, s.pinned
             FROM states s
             JOIN items i ON i.id = s.item_id
             WHERE i.path = ?1 OR i.path LIKE ?2 OR i.path = ?3 OR i.path LIKE ?4
             ORDER BY i.path ASC",
        )
        .bind(prefix_a)
        .bind(pattern_a)
        .bind(prefix_b)
        .bind(pattern_b)
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            let path: String = row.try_get("path")?;
            let state: String = row.try_get("state")?;
            let pinned: i64 = row.try_get("pinned")?;
            out.push((path, FileState::parse(&state)?, pinned != 0));
        }
        Ok(out)
    }

    pub async fn list_pinned_cloud_only_paths_by_prefix(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, IndexError> {
        let [prefix_a, prefix_b] = prefix_variants(prefix);
        let pattern_a = format!("{}/%", prefix_a.trim_end_matches('/'));
        let pattern_b = format!("{}/%", prefix_b.trim_end_matches('/'));
        let rows = sqlx::query(
            "SELECT i.path
             FROM states s
             JOIN items i ON i.id = s.item_id
             WHERE (i.path = ?1 OR i.path LIKE ?2 OR i.path = ?3 OR i.path LIKE ?4)
                AND s.pinned = 1
                AND s.state = 'cloud_only'
             ORDER BY i.path ASC",
        )
        .bind(prefix_a)
        .bind(pattern_a)
        .bind(prefix_b)
        .bind(pattern_b)
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter()
            .map(|row| row.try_get::<String, _>("path").map_err(IndexError::from))
            .collect()
    }

    pub async fn set_pinned(&self, item_id: i64, pinned: bool) -> Result<(), IndexError> {
        sqlx::query("UPDATE states SET pinned = ?1 WHERE item_id = ?2")
            .bind(if pinned { 1 } else { 0 })
            .bind(item_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn set_sync_cursor(
        &self,
        cursor: Option<&str>,
        last_sync: Option<i64>,
    ) -> Result<(), IndexError> {
        sqlx::query(
            "\n            INSERT INTO sync_cursor (id, cursor, last_sync)\n            VALUES (1, ?1, ?2)\n            ON CONFLICT(id) DO UPDATE SET\n                cursor = excluded.cursor,\n                last_sync = excluded.last_sync;\n            ",
        )
        .bind(cursor)
        .bind(last_sync)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn get_sync_cursor(&self) -> Result<SyncCursor, IndexError> {
        let row = sqlx::query("SELECT cursor, last_sync FROM sync_cursor WHERE id = 1")
            .fetch_optional(&self.pool)
            .await?;

        if let Some(row) = row {
            Ok(SyncCursor {
                cursor: row.try_get("cursor")?,
                last_sync: row.try_get("last_sync")?,
            })
        } else {
            Ok(SyncCursor {
                cursor: None,
                last_sync: None,
            })
        }
    }

    pub async fn enqueue_op(&self, op: &Operation) -> Result<i64, IndexError> {
        let result = sqlx::query(
            "INSERT INTO ops_queue (kind, path, payload, attempt, retry_at, priority) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(kind, path) DO UPDATE SET
                payload = excluded.payload,
                attempt = MIN(ops_queue.attempt, excluded.attempt),
                retry_at = excluded.retry_at,
                priority = MAX(ops_queue.priority, excluded.priority)",
        )
            .bind(operation_kind_as_str(&op.kind))
            .bind(&op.path)
            .bind(&op.payload)
            .bind(op.attempt)
            .bind(op.retry_at)
            .bind(op.priority)
            .execute(&self.pool)
            .await?;

        Ok(result.last_insert_rowid())
    }

    pub async fn requeue_op(
        &self,
        op: &Operation,
        retry_at: i64,
        last_error: Option<&str>,
    ) -> Result<(), IndexError> {
        let mut op = op.clone();
        op.attempt = op.attempt.saturating_add(1);
        op.retry_at = Some(retry_at);
        self.enqueue_op(&op).await?;
        if let Some(item) = self.get_item_by_path(&op.path).await? {
            self.set_state_with_meta(
                item.id,
                FileState::Error,
                true,
                last_error,
                StateMeta {
                    retry_at: Some(retry_at),
                    last_success_at: None,
                    last_error_at: Some(retry_at),
                    dirty: true,
                },
            )
            .await?;
        }
        Ok(())
    }

    pub async fn dequeue_op(&self) -> Result<Option<Operation>, IndexError> {
        let row = sqlx::query(
            "SELECT id, kind, path, payload, attempt, retry_at, priority
             FROM ops_queue
             WHERE retry_at IS NULL OR retry_at <= CAST(strftime('%s','now') AS INTEGER)
             ORDER BY priority DESC, id ASC
             LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let id: i64 = row.try_get("id")?;
        let kind: String = row.try_get("kind")?;
        let operation = Operation {
            kind: parse_operation_kind(&kind)?,
            path: row.try_get("path")?,
            payload: row.try_get("payload")?,
            attempt: row.try_get("attempt")?,
            retry_at: row.try_get("retry_at")?,
            priority: row.try_get("priority")?,
        };

        sqlx::query("DELETE FROM ops_queue WHERE id = ?1")
            .bind(id)
            .execute(&self.pool)
            .await?;

        Ok(Some(operation))
    }

    pub async fn record_conflict(
        &self,
        path: &str,
        renamed_local: &str,
        created: i64,
        reason: &str,
    ) -> Result<i64, IndexError> {
        let result = sqlx::query(
            "INSERT INTO conflicts (path, renamed_local, created, reason) VALUES (?1, ?2, ?3, ?4)",
        )
        .bind(path)
        .bind(renamed_local)
        .bind(created)
        .bind(reason)
        .execute(&self.pool)
        .await?;

        Ok(result.last_insert_rowid())
    }

    pub async fn list_conflicts(&self) -> Result<Vec<ConflictRecord>, IndexError> {
        let rows = sqlx::query(
            "SELECT id, path, renamed_local, created, reason FROM conflicts ORDER BY id ASC",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            out.push(ConflictRecord {
                id: row.try_get("id")?,
                path: row.try_get("path")?,
                renamed_local: row.try_get("renamed_local")?,
                created: row.try_get("created")?,
                reason: row.try_get("reason")?,
            });
        }
        Ok(out)
    }
}

fn default_db_path() -> Result<PathBuf, IndexError> {
    let mut path = dirs::data_dir().ok_or(IndexError::MissingDataDir)?;
    path.push("yadisk-gtk");
    path.push("sync");
    path.push("index.db");
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn make_store() -> IndexStore {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let store = IndexStore::from_pool(pool);
        store.init().await.unwrap();
        store
    }

    #[tokio::test]
    async fn upsert_and_fetch_item() {
        let store = make_store().await;
        let item = ItemInput {
            path: "/Docs/A.txt".into(),
            parent_path: Some("/Docs".into()),
            name: "A.txt".into(),
            item_type: ItemType::File,
            size: Some(12),
            modified: Some(1_700_000_000),
            hash: Some("hash".into()),
            resource_id: Some("id".into()),
            last_synced_hash: Some("hash".into()),
            last_synced_modified: Some(1_700_000_000),
        };

        let inserted = store.upsert_item(&item).await.unwrap();
        let fetched = store.get_item_by_path("/Docs/A.txt").await.unwrap();

        assert_eq!(inserted, fetched.unwrap());
    }

    #[tokio::test]
    async fn upsert_updates_existing_item() {
        let store = make_store().await;
        let mut item = ItemInput {
            path: "/Docs/A.txt".into(),
            parent_path: Some("/Docs".into()),
            name: "A.txt".into(),
            item_type: ItemType::File,
            size: Some(12),
            modified: Some(1_700_000_000),
            hash: None,
            resource_id: None,
            last_synced_hash: None,
            last_synced_modified: None,
        };

        store.upsert_item(&item).await.unwrap();
        item.size = Some(24);
        let updated = store.upsert_item(&item).await.unwrap();

        assert_eq!(updated.size, Some(24));
    }

    #[tokio::test]
    async fn set_and_get_state() {
        let store = make_store().await;
        let item = ItemInput {
            path: "/Docs/A.txt".into(),
            parent_path: Some("/Docs".into()),
            name: "A.txt".into(),
            item_type: ItemType::File,
            size: Some(12),
            modified: Some(1_700_000_000),
            hash: None,
            resource_id: None,
            last_synced_hash: None,
            last_synced_modified: None,
        };

        let inserted = store.upsert_item(&item).await.unwrap();
        store
            .set_state(inserted.id, FileState::Cached, true, Some("ok"))
            .await
            .unwrap();

        let state = store.get_state(inserted.id).await.unwrap().unwrap();
        assert_eq!(state.state, FileState::Cached);
        assert!(state.pinned);
        assert_eq!(state.last_error.as_deref(), Some("ok"));
        assert!(!state.dirty);

        store.set_pinned(inserted.id, false).await.unwrap();
        let state = store.get_state(inserted.id).await.unwrap().unwrap();
        assert!(!state.pinned);
    }

    #[tokio::test]
    async fn disk_prefix_queries_match_slash_paths() {
        let store = make_store().await;
        let item = store
            .upsert_item(&ItemInput {
                path: "/Docs/A.txt".into(),
                parent_path: Some("/Docs".into()),
                name: "A.txt".into(),
                item_type: ItemType::File,
                size: Some(12),
                modified: Some(1_700_000_000),
                hash: None,
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();
        store
            .set_state(item.id, FileState::CloudOnly, true, None)
            .await
            .unwrap();

        let items = store.list_items_by_prefix("disk:/").await.unwrap();
        assert_eq!(items.len(), 1);
        let states = store.list_states_by_prefix("disk:/").await.unwrap();
        assert_eq!(states.len(), 1);
        let pinned = store
            .list_pinned_cloud_only_paths_by_prefix("disk:/")
            .await
            .unwrap();
        assert_eq!(pinned, vec!["/Docs/A.txt".to_string()]);
    }

    #[tokio::test]
    async fn set_and_get_sync_cursor() {
        let store = make_store().await;
        store
            .set_sync_cursor(Some("cursor-1"), Some(42))
            .await
            .unwrap();
        let cursor = store.get_sync_cursor().await.unwrap();
        assert_eq!(cursor.cursor.as_deref(), Some("cursor-1"));
        assert_eq!(cursor.last_sync, Some(42));
    }

    #[tokio::test]
    async fn enqueue_and_dequeue_ops() {
        let store = make_store().await;
        let op = Operation {
            kind: OperationKind::Upload,
            path: "/Docs/A.txt".into(),
            payload: Some("{\"overwrite\":true}".into()),
            attempt: 0,
            retry_at: None,
            priority: 10,
        };

        store.enqueue_op(&op).await.unwrap();
        let fetched = store.dequeue_op().await.unwrap().expect("expected op");

        assert_eq!(fetched, op);
        assert!(store.dequeue_op().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn enqueue_deduplicates_by_kind_and_path() {
        let store = make_store().await;
        let first = Operation {
            kind: OperationKind::Upload,
            path: "/Docs/A.txt".into(),
            payload: Some("{\"v\":1}".into()),
            attempt: 2,
            retry_at: Some(100),
            priority: 1,
        };
        let second = Operation {
            kind: OperationKind::Upload,
            path: "/Docs/A.txt".into(),
            payload: Some("{\"v\":2}".into()),
            attempt: 0,
            retry_at: None,
            priority: 5,
        };

        store.enqueue_op(&first).await.unwrap();
        store.enqueue_op(&second).await.unwrap();
        let fetched = store.dequeue_op().await.unwrap().unwrap();

        assert_eq!(fetched.attempt, 0);
        assert_eq!(fetched.priority, 5);
        assert_eq!(fetched.payload.as_deref(), Some("{\"v\":2}"));
    }

    #[tokio::test]
    async fn requeue_increments_attempt_and_sets_retry_at() {
        let store = make_store().await;
        let item = ItemInput {
            path: "/Docs/A.txt".into(),
            parent_path: Some("/Docs".into()),
            name: "A.txt".into(),
            item_type: ItemType::File,
            size: Some(12),
            modified: Some(1_700_000_000),
            hash: None,
            resource_id: None,
            last_synced_hash: None,
            last_synced_modified: None,
        };
        let inserted = store.upsert_item(&item).await.unwrap();
        store
            .set_state(inserted.id, FileState::Syncing, true, None)
            .await
            .unwrap();

        let op = Operation {
            kind: OperationKind::Download,
            path: "/Docs/A.txt".into(),
            payload: None,
            attempt: 0,
            retry_at: None,
            priority: 0,
        };
        store.requeue_op(&op, 999, Some("transient")).await.unwrap();

        let fetched = store.dequeue_op().await.unwrap().unwrap();
        assert_eq!(fetched.attempt, 1);
        assert_eq!(fetched.retry_at, Some(999));
        let state = store.get_state(inserted.id).await.unwrap().unwrap();
        assert!(state.dirty);
        assert_eq!(state.retry_at, Some(999));
    }

    #[tokio::test]
    async fn records_and_lists_conflicts() {
        let store = make_store().await;
        store
            .record_conflict("/Docs/A.txt", "/Docs/A (conflict).txt", 123, "both-changed")
            .await
            .unwrap();

        let conflicts = store.list_conflicts().await.unwrap();
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].path, "/Docs/A.txt");
        assert_eq!(conflicts[0].reason, "both-changed");
    }

    #[tokio::test]
    async fn init_upgrades_legacy_schema() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        sqlx::query(
            "CREATE TABLE items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                item_type TEXT NOT NULL,
                size INTEGER,
                modified INTEGER,
                hash TEXT,
                resource_id TEXT
            );",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "CREATE TABLE states (
                item_id INTEGER PRIMARY KEY,
                state TEXT NOT NULL,
                pinned INTEGER NOT NULL,
                last_error TEXT,
                FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE
            );",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "CREATE TABLE sync_cursor (
                id INTEGER PRIMARY KEY CHECK(id = 1),
                cursor TEXT,
                last_sync INTEGER
            );",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "CREATE TABLE ops_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT NOT NULL,
                path TEXT NOT NULL,
                attempt INTEGER NOT NULL
            );",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "CREATE TABLE conflicts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                path TEXT NOT NULL,
                renamed_local TEXT NOT NULL,
                created INTEGER NOT NULL,
                reason TEXT NOT NULL
            );",
        )
        .execute(&pool)
        .await
        .unwrap();

        let store = IndexStore::from_pool(pool.clone());
        store.init().await.unwrap();

        let item = ItemInput {
            path: "/Docs/A.txt".into(),
            parent_path: Some("/Docs".into()),
            name: "A.txt".into(),
            item_type: ItemType::File,
            size: Some(1),
            modified: Some(123),
            hash: Some("h".into()),
            resource_id: Some("rid".into()),
            last_synced_hash: Some("h".into()),
            last_synced_modified: Some(123),
        };
        let inserted = store.upsert_item(&item).await.unwrap();
        assert_eq!(inserted.parent_path.as_deref(), Some("/Docs"));

        let first = Operation {
            kind: OperationKind::Upload,
            path: "/Docs/A.txt".into(),
            payload: Some("{\"v\":1}".into()),
            attempt: 1,
            retry_at: None,
            priority: 1,
        };
        let second = Operation {
            kind: OperationKind::Upload,
            path: "/Docs/A.txt".into(),
            payload: Some("{\"v\":2}".into()),
            attempt: 0,
            retry_at: None,
            priority: 2,
        };
        store.enqueue_op(&first).await.unwrap();
        store.enqueue_op(&second).await.unwrap();
        let op = store.dequeue_op().await.unwrap().unwrap();
        assert_eq!(op.priority, 2);
        assert_eq!(op.payload.as_deref(), Some("{\"v\":2}"));
    }
}
