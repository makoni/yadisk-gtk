use thiserror::Error;
use yadiskd::sync::index::{FileState, IndexError, IndexStore, ItemInput, ItemType};
use yadiskd::sync::queue::{Operation, OperationKind};

pub const XATTR_STATE: &str = "user.yadisk.state";

#[derive(Debug, Error)]
pub enum FuseBridgeError {
    #[error("index error: {0}")]
    Index(#[from] IndexError),
    #[error("item not found: {0}")]
    NotFound(String),
}

pub struct YadiskFuseBridge {
    index: IndexStore,
}

impl YadiskFuseBridge {
    pub fn new(index: IndexStore) -> Self {
        Self { index }
    }

    pub async fn getattr(
        &self,
        path: &str,
    ) -> Result<Option<(ItemType, Option<i64>, Option<i64>)>, FuseBridgeError> {
        Ok(self
            .index
            .get_item_by_path(path)
            .await?
            .map(|item| (item.item_type, item.size, item.modified)))
    }

    pub async fn readdir(&self, path: &str) -> Result<Vec<String>, FuseBridgeError> {
        let prefix = if path == "/" {
            "/".to_string()
        } else {
            path.trim_end_matches('/').to_string()
        };
        let items = self.index.list_items_by_prefix(&prefix).await?;
        let mut out: Vec<String> = items
            .into_iter()
            .filter_map(|item| {
                if item.path == prefix {
                    return None;
                }
                let rest = item
                    .path
                    .trim_start_matches(&prefix)
                    .trim_start_matches('/');
                rest.split('/').next().map(|name| name.to_string())
            })
            .collect();
        out.sort();
        out.dedup();
        Ok(out)
    }

    pub async fn open_read(&self, path: &str) -> Result<(), FuseBridgeError> {
        let item = self
            .index
            .get_item_by_path(path)
            .await?
            .ok_or_else(|| FuseBridgeError::NotFound(path.to_string()))?;
        if let Some(state) = self.index.get_state(item.id).await?
            && state.state == FileState::CloudOnly
        {
            self.index
                .enqueue_op(&Operation {
                    kind: OperationKind::Download,
                    path: path.to_string(),
                    payload: None,
                    attempt: 0,
                    retry_at: None,
                    priority: 80,
                })
                .await?;
        }
        Ok(())
    }

    pub async fn write_flush(&self, path: &str) -> Result<(), FuseBridgeError> {
        self.index
            .enqueue_op(&Operation {
                kind: OperationKind::Upload,
                path: path.to_string(),
                payload: None,
                attempt: 0,
                retry_at: None,
                priority: 80,
            })
            .await?;
        Ok(())
    }

    pub async fn rename(&self, from: &str, to: &str) -> Result<(), FuseBridgeError> {
        let payload = format!(
            "{{\"from\":\"{from}\",\"path\":\"{to}\",\"overwrite\":true,\"action\":\"move\"}}"
        );
        self.index
            .enqueue_op(&Operation {
                kind: OperationKind::Move,
                path: to.to_string(),
                payload: Some(payload),
                attempt: 0,
                retry_at: None,
                priority: 80,
            })
            .await?;
        Ok(())
    }

    pub async fn unlink_or_rmdir(&self, path: &str) -> Result<(), FuseBridgeError> {
        self.index
            .enqueue_op(&Operation {
                kind: OperationKind::Delete,
                path: path.to_string(),
                payload: None,
                attempt: 0,
                retry_at: None,
                priority: 80,
            })
            .await?;
        Ok(())
    }

    pub async fn mkdir(&self, path: &str) -> Result<(), FuseBridgeError> {
        let name = path.split('/').next_back().unwrap_or(path).to_string();
        self.index
            .upsert_item(&ItemInput {
                path: path.to_string(),
                parent_path: path.rsplit_once('/').map(|(p, _)| {
                    if p.is_empty() {
                        "/".to_string()
                    } else {
                        p.to_string()
                    }
                }),
                name,
                item_type: ItemType::Dir,
                size: None,
                modified: None,
                hash: None,
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::SqlitePool;

    async fn make_bridge() -> YadiskFuseBridge {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let store = IndexStore::from_pool(pool);
        store.init().await.unwrap();
        YadiskFuseBridge::new(store)
    }

    #[tokio::test]
    async fn readdir_and_getattr_use_indexstore() {
        let bridge = make_bridge().await;
        bridge.mkdir("/Docs").await.unwrap();
        bridge.mkdir("/Docs/Sub").await.unwrap();
        let dirents = bridge.readdir("/Docs").await.unwrap();
        assert_eq!(dirents, vec!["Sub".to_string()]);
        let attr = bridge.getattr("/Docs").await.unwrap();
        assert!(attr.is_some());
    }

    #[tokio::test]
    async fn read_write_rename_delete_enqueue_ops() {
        let bridge = make_bridge().await;
        bridge.mkdir("/Docs").await.unwrap();
        bridge
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
        let item = bridge
            .index
            .get_item_by_path("/Docs/A.txt")
            .await
            .unwrap()
            .unwrap();
        bridge
            .index
            .set_state(item.id, FileState::CloudOnly, true, None)
            .await
            .unwrap();

        bridge.open_read("/Docs/A.txt").await.unwrap();
        bridge.write_flush("/Docs/A.txt").await.unwrap();
        bridge.rename("/Docs/A.txt", "/Docs/B.txt").await.unwrap();
        bridge.unlink_or_rmdir("/Docs/B.txt").await.unwrap();

        let op1 = bridge.index.dequeue_op().await.unwrap().unwrap();
        let op2 = bridge.index.dequeue_op().await.unwrap().unwrap();
        let op3 = bridge.index.dequeue_op().await.unwrap().unwrap();
        let op4 = bridge.index.dequeue_op().await.unwrap().unwrap();
        assert_eq!(op1.kind, OperationKind::Download);
        assert_eq!(op2.kind, OperationKind::Upload);
        assert_eq!(op3.kind, OperationKind::Move);
        assert_eq!(op4.kind, OperationKind::Delete);
    }
}
