#![allow(dead_code)]

use sqlx::{Row, SqlitePool};
use thiserror::Error;

use super::queue::{Operation, OperationKind};

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),
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
    pub name: String,
    pub item_type: ItemType,
    pub size: Option<i64>,
    pub modified: Option<i64>,
    pub hash: Option<String>,
    pub resource_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ItemRecord {
    pub id: i64,
    pub path: String,
    pub name: String,
    pub item_type: ItemType,
    pub size: Option<i64>,
    pub modified: Option<i64>,
    pub hash: Option<String>,
    pub resource_id: Option<String>,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateRecord {
    pub item_id: i64,
    pub state: FileState,
    pub pinned: bool,
    pub last_error: Option<String>,
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

    pub async fn init(&self) -> Result<(), IndexError> {
        sqlx::query(
            "\n            CREATE TABLE IF NOT EXISTS items (\n                id INTEGER PRIMARY KEY AUTOINCREMENT,\n                path TEXT NOT NULL UNIQUE,\n                name TEXT NOT NULL,\n                item_type TEXT NOT NULL,\n                size INTEGER,\n                modified INTEGER,\n                hash TEXT,\n                resource_id TEXT\n            );\n            ",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "\n            CREATE TABLE IF NOT EXISTS states (\n                item_id INTEGER PRIMARY KEY,\n                state TEXT NOT NULL,\n                pinned INTEGER NOT NULL,\n                last_error TEXT,\n                FOREIGN KEY(item_id) REFERENCES items(id) ON DELETE CASCADE\n            );\n            ",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "\n            CREATE TABLE IF NOT EXISTS sync_cursor (\n                id INTEGER PRIMARY KEY CHECK(id = 1),\n                cursor TEXT,\n                last_sync INTEGER\n            );\n            ",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "\n            CREATE TABLE IF NOT EXISTS ops_queue (\n                id INTEGER PRIMARY KEY AUTOINCREMENT,\n                kind TEXT NOT NULL,\n                path TEXT NOT NULL,\n                attempt INTEGER NOT NULL\n            );\n            ",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "\n            CREATE TABLE IF NOT EXISTS conflicts (\n                id INTEGER PRIMARY KEY AUTOINCREMENT,\n                path TEXT NOT NULL,\n                renamed_local TEXT NOT NULL,\n                created INTEGER NOT NULL,\n                reason TEXT NOT NULL\n            );\n            ",
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn upsert_item(&self, item: &ItemInput) -> Result<ItemRecord, IndexError> {
        sqlx::query(
            "\n            INSERT INTO items (path, name, item_type, size, modified, hash, resource_id)\n            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)\n            ON CONFLICT(path) DO UPDATE SET\n                name = excluded.name,\n                item_type = excluded.item_type,\n                size = excluded.size,\n                modified = excluded.modified,\n                hash = excluded.hash,\n                resource_id = excluded.resource_id;\n            ",
        )
        .bind(&item.path)
        .bind(&item.name)
        .bind(item.item_type.as_str())
        .bind(item.size)
        .bind(item.modified)
        .bind(&item.hash)
        .bind(&item.resource_id)
        .execute(&self.pool)
        .await?;

        self.get_item_by_path(&item.path)
            .await?
            .ok_or(IndexError::MissingItem)
    }

    pub async fn get_item_by_path(&self, path: &str) -> Result<Option<ItemRecord>, IndexError> {
        let row = sqlx::query(
            "SELECT id, path, name, item_type, size, modified, hash, resource_id FROM items WHERE path = ?1",
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
            name: row.try_get("name")?,
            item_type: ItemType::parse(&item_type)?,
            size: row.try_get("size")?,
            modified: row.try_get("modified")?,
            hash: row.try_get("hash")?,
            resource_id: row.try_get("resource_id")?,
        }))
    }

    pub async fn set_state(
        &self,
        item_id: i64,
        state: FileState,
        pinned: bool,
        last_error: Option<&str>,
    ) -> Result<(), IndexError> {
        sqlx::query(
            "\n            INSERT INTO states (item_id, state, pinned, last_error)\n            VALUES (?1, ?2, ?3, ?4)\n            ON CONFLICT(item_id) DO UPDATE SET\n                state = excluded.state,\n                pinned = excluded.pinned,\n                last_error = excluded.last_error;\n            ",
        )
        .bind(item_id)
        .bind(state.as_str())
        .bind(if pinned { 1 } else { 0 })
        .bind(last_error)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_state(&self, item_id: i64) -> Result<Option<StateRecord>, IndexError> {
        let row =
            sqlx::query("SELECT item_id, state, pinned, last_error FROM states WHERE item_id = ?1")
                .bind(item_id)
                .fetch_optional(&self.pool)
                .await?;

        let Some(row) = row else {
            return Ok(None);
        };

        let state: String = row.try_get("state")?;
        let pinned: i64 = row.try_get("pinned")?;

        Ok(Some(StateRecord {
            item_id: row.try_get("item_id")?,
            state: FileState::parse(&state)?,
            pinned: pinned != 0,
            last_error: row.try_get("last_error")?,
        }))
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
        let result = sqlx::query("INSERT INTO ops_queue (kind, path, attempt) VALUES (?1, ?2, ?3)")
            .bind(operation_kind_as_str(&op.kind))
            .bind(&op.path)
            .bind(op.attempt)
            .execute(&self.pool)
            .await?;

        Ok(result.last_insert_rowid())
    }

    pub async fn dequeue_op(&self) -> Result<Option<Operation>, IndexError> {
        let row =
            sqlx::query("SELECT id, kind, path, attempt FROM ops_queue ORDER BY id ASC LIMIT 1")
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
            attempt: row.try_get("attempt")?,
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
            name: "A.txt".into(),
            item_type: ItemType::File,
            size: Some(12),
            modified: Some(1_700_000_000),
            hash: Some("hash".into()),
            resource_id: Some("id".into()),
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
            name: "A.txt".into(),
            item_type: ItemType::File,
            size: Some(12),
            modified: Some(1_700_000_000),
            hash: None,
            resource_id: None,
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
            name: "A.txt".into(),
            item_type: ItemType::File,
            size: Some(12),
            modified: Some(1_700_000_000),
            hash: None,
            resource_id: None,
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

        store.set_pinned(inserted.id, false).await.unwrap();
        let state = store.get_state(inserted.id).await.unwrap().unwrap();
        assert!(!state.pinned);
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
            attempt: 0,
        };

        store.enqueue_op(&op).await.unwrap();
        let fetched = store.dequeue_op().await.unwrap().expect("expected op");

        assert_eq!(fetched, op);
        assert!(store.dequeue_op().await.unwrap().is_none());
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
}
