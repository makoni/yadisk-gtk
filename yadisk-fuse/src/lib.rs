use thiserror::Error;
use yadiskd::sync::index::{
    FileState, IndexError, IndexStore, ItemInput, ItemRecord, ItemType, StateMeta, StateRecord,
};
use yadiskd::sync::queue::{Operation, OperationKind};

pub const XATTR_STATE: &str = "user.yadisk.state";

#[derive(Debug, Error)]
pub enum FuseBridgeError {
    #[error("index error: {0}")]
    Index(#[from] IndexError),
    #[error("item not found: {0}")]
    NotFound(String),
    #[error("invalid path: {0}")]
    InvalidPath(String),
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

    pub async fn create_file(&self, path: &str, modified: i64) -> Result<(), FuseBridgeError> {
        self.upsert_file(path, 0, modified).await?;
        Ok(())
    }

    pub async fn stage_write(
        &self,
        path: &str,
        size: i64,
        modified: i64,
    ) -> Result<(), FuseBridgeError> {
        self.upsert_file(path, size, modified).await?;
        Ok(())
    }

    pub async fn write_flush(
        &self,
        path: &str,
        size: i64,
        modified: i64,
    ) -> Result<(), FuseBridgeError> {
        self.upsert_file(path, size, modified).await?;
        self.enqueue_upload(path).await?;
        Ok(())
    }

    pub async fn rename(&self, from: &str, to: &str) -> Result<(), FuseBridgeError> {
        if from == "/" || to == "/" || to.starts_with(&(format!("{}/", from.trim_end_matches('/'))))
        {
            return Err(FuseBridgeError::InvalidPath(format!("{from} -> {to}")));
        }
        let source = self.collect_tree(from).await?;
        if source.is_empty() {
            return Err(FuseBridgeError::NotFound(from.to_string()));
        }
        self.ensure_parent_dirs(to).await?;
        self.index.delete_ops_by_prefix(from).await?;
        self.index.delete_ops_by_prefix(to).await?;
        let target = self.collect_tree(to).await?;
        if !target.is_empty() {
            self.delete_tree_now(&target).await?;
        }

        let source_local_only = is_local_only(&source[0].0);
        let mut moved_paths = Vec::with_capacity(source.len());
        for (item, state) in &source {
            let new_path = replace_prefix(&item.path, from, to)?;
            let new_item = self
                .index
                .upsert_item(&ItemInput {
                    path: new_path.clone(),
                    parent_path: parent_path(&new_path),
                    name: leaf_name(&new_path),
                    item_type: item.item_type.clone(),
                    size: item.size,
                    modified: item.modified,
                    hash: item.hash.clone(),
                    resource_id: if source_local_only {
                        None
                    } else {
                        item.resource_id.clone()
                    },
                    last_synced_hash: if source_local_only {
                        None
                    } else {
                        item.last_synced_hash.clone()
                    },
                    last_synced_modified: if source_local_only {
                        None
                    } else {
                        item.last_synced_modified
                    },
                })
                .await?;
            self.set_pending_state(&new_item, state.as_ref()).await?;
            moved_paths.push((new_path, item.item_type.clone()));
        }
        self.delete_tree_now(&source).await?;

        if source_local_only {
            for (path, kind) in moved_paths {
                match kind {
                    ItemType::Dir => {
                        self.index
                            .enqueue_op(&Operation {
                                kind: OperationKind::Mkdir,
                                path,
                                payload: None,
                                attempt: 0,
                                retry_at: None,
                                priority: 55,
                            })
                            .await?;
                    }
                    ItemType::File => {
                        self.enqueue_upload(&path).await?;
                    }
                }
            }
        } else {
            let payload = serde_json::json!({
                "from": from,
                "path": to,
                "overwrite": true,
                "action": "move"
            })
            .to_string();
            self.index
                .enqueue_op(&Operation {
                    kind: OperationKind::Move,
                    path: to.to_string(),
                    payload: Some(payload),
                    attempt: 0,
                    retry_at: None,
                    priority: 60,
                })
                .await?;
        }
        Ok(())
    }

    pub async fn unlink_or_rmdir(&self, path: &str) -> Result<(), FuseBridgeError> {
        let tree = self.collect_tree(path).await?;
        if tree.is_empty() {
            return Err(FuseBridgeError::NotFound(path.to_string()));
        }
        self.index.delete_ops_by_prefix(path).await?;
        let remote_known = !is_local_only(&tree[0].0);
        self.delete_tree_now(&tree).await?;
        if remote_known {
            self.index
                .enqueue_op(&Operation {
                    kind: OperationKind::Delete,
                    path: path.to_string(),
                    payload: None,
                    attempt: 0,
                    retry_at: None,
                    priority: 60,
                })
                .await?;
        }
        Ok(())
    }

    pub async fn mkdir(&self, path: &str) -> Result<(), FuseBridgeError> {
        self.ensure_parent_dirs(path).await?;
        if self.index.get_item_by_path(path).await?.is_some() {
            return Ok(());
        }
        let record = self
            .index
            .upsert_item(&ItemInput {
                path: path.to_string(),
                parent_path: parent_path(path),
                name: leaf_name(path),
                item_type: ItemType::Dir,
                size: None,
                modified: None,
                hash: None,
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await?;
        self.set_pending_state(&record, None).await?;
        self.index
            .enqueue_op(&Operation {
                kind: OperationKind::Mkdir,
                path: path.to_string(),
                payload: None,
                attempt: 0,
                retry_at: None,
                priority: 55,
            })
            .await?;
        Ok(())
    }

    async fn enqueue_upload(&self, path: &str) -> Result<(), FuseBridgeError> {
        self.index
            .enqueue_op(&Operation {
                kind: OperationKind::Upload,
                path: path.to_string(),
                payload: None,
                attempt: 0,
                retry_at: None,
                priority: 50,
            })
            .await?;
        Ok(())
    }

    async fn upsert_file(
        &self,
        path: &str,
        size: i64,
        modified: i64,
    ) -> Result<ItemRecord, FuseBridgeError> {
        self.ensure_parent_dirs(path).await?;
        let existing = self.index.get_item_by_path(path).await?;
        let record = self
            .index
            .upsert_item(&ItemInput {
                path: path.to_string(),
                parent_path: parent_path(path),
                name: leaf_name(path),
                item_type: ItemType::File,
                size: Some(size),
                modified: Some(modified),
                hash: None,
                resource_id: existing.as_ref().and_then(|item| item.resource_id.clone()),
                last_synced_hash: existing
                    .as_ref()
                    .and_then(|item| item.last_synced_hash.clone()),
                last_synced_modified: existing.as_ref().and_then(|item| item.last_synced_modified),
            })
            .await?;
        let state = self.index.get_state(record.id).await?;
        self.set_pending_state(&record, state.as_ref()).await?;
        Ok(record)
    }

    async fn ensure_parent_dirs(&self, path: &str) -> Result<(), FuseBridgeError> {
        for dir in ancestor_dirs(path) {
            match self.index.get_item_by_path(&dir).await? {
                Some(existing) if existing.item_type == ItemType::Dir => continue,
                Some(existing) => {
                    return Err(FuseBridgeError::InvalidPath(format!(
                        "{} is not a directory",
                        existing.path
                    )));
                }
                None => {
                    let record = self
                        .index
                        .upsert_item(&ItemInput {
                            path: dir.clone(),
                            parent_path: parent_path(&dir),
                            name: leaf_name(&dir),
                            item_type: ItemType::Dir,
                            size: None,
                            modified: None,
                            hash: None,
                            resource_id: None,
                            last_synced_hash: None,
                            last_synced_modified: None,
                        })
                        .await?;
                    self.set_pending_state(&record, None).await?;
                    self.index
                        .enqueue_op(&Operation {
                            kind: OperationKind::Mkdir,
                            path: dir,
                            payload: None,
                            attempt: 0,
                            retry_at: None,
                            priority: 55,
                        })
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn set_pending_state(
        &self,
        item: &ItemRecord,
        previous: Option<&StateRecord>,
    ) -> Result<(), FuseBridgeError> {
        let previous_pinned = previous.map(|state| state.pinned).unwrap_or(true);
        let previous_accessed = previous.and_then(|state| state.last_accessed);
        self.index
            .set_state_with_meta(
                item.id,
                FileState::Syncing,
                previous_pinned,
                None,
                StateMeta {
                    retry_at: None,
                    last_success_at: previous.and_then(|state| state.last_success_at),
                    last_error_at: None,
                    last_accessed: previous_accessed,
                    dirty: true,
                },
            )
            .await?;
        Ok(())
    }

    async fn collect_tree(
        &self,
        path: &str,
    ) -> Result<Vec<(ItemRecord, Option<StateRecord>)>, FuseBridgeError> {
        let mut items = self.index.list_items_by_prefix(path).await?;
        items.retain(|item| item.path == path || item.path.starts_with(&(path.to_string() + "/")));
        items.sort_by_key(|item| item.path.len());
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            let state = self.index.get_state(item.id).await?;
            out.push((item, state));
        }
        Ok(out)
    }

    async fn delete_tree_now(
        &self,
        tree: &[(ItemRecord, Option<StateRecord>)],
    ) -> Result<(), FuseBridgeError> {
        for (item, _) in tree.iter().rev() {
            self.index.delete_item_by_path(&item.path).await?;
        }
        Ok(())
    }
}

fn ancestor_dirs(path: &str) -> Vec<String> {
    let mut current = parent_path(path);
    let mut out = Vec::new();
    while let Some(parent) = current {
        if parent == "/" {
            break;
        }
        out.push(parent.clone());
        current = parent_path(&parent);
    }
    out.reverse();
    out
}

fn leaf_name(path: &str) -> String {
    path.split('/').next_back().unwrap_or(path).to_string()
}

fn parent_path(path: &str) -> Option<String> {
    let trimmed = path.trim_end_matches('/');
    let idx = trimmed.rfind('/')?;
    Some(if idx == 0 {
        "/".to_string()
    } else {
        trimmed[..idx].to_string()
    })
}

fn replace_prefix(path: &str, from: &str, to: &str) -> Result<String, FuseBridgeError> {
    if path == from {
        return Ok(to.to_string());
    }
    let suffix = path
        .strip_prefix(from)
        .and_then(|value| value.strip_prefix('/'))
        .ok_or_else(|| FuseBridgeError::InvalidPath(format!("{path} is not under {from}")))?;
    Ok(format!("{}/{}", to.trim_end_matches('/'), suffix))
}

fn is_local_only(item: &ItemRecord) -> bool {
    item.resource_id.is_none()
        && item.last_synced_hash.is_none()
        && item.last_synced_modified.is_none()
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

    async fn dequeue_all(index: &IndexStore) -> Vec<Operation> {
        let mut ops = Vec::new();
        while let Some(op) = index.dequeue_op().await.unwrap() {
            ops.push(op);
        }
        ops
    }

    #[tokio::test]
    async fn readdir_and_getattr_use_indexstore() {
        let bridge = make_bridge().await;
        bridge.mkdir("/Docs").await.unwrap();
        bridge.mkdir("/Docs/Sub").await.unwrap();
        let _ = dequeue_all(&bridge.index).await;
        let dirents = bridge.readdir("/Docs").await.unwrap();
        assert_eq!(dirents, vec!["Sub".to_string()]);
        let attr = bridge.getattr("/Docs").await.unwrap();
        assert!(attr.is_some());
    }

    #[tokio::test]
    async fn write_flush_creates_missing_parent_dirs_before_upload() {
        let bridge = make_bridge().await;

        bridge.create_file("/Docs/Sub/New.txt", 100).await.unwrap();
        bridge
            .write_flush("/Docs/Sub/New.txt", 3, 101)
            .await
            .unwrap();

        let ops = dequeue_all(&bridge.index).await;
        assert_eq!(
            ops.iter()
                .map(|op| (&op.kind, op.path.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (&OperationKind::Mkdir, "/Docs"),
                (&OperationKind::Mkdir, "/Docs/Sub"),
                (&OperationKind::Upload, "/Docs/Sub/New.txt"),
            ]
        );
        let item = bridge
            .index
            .get_item_by_path("/Docs/Sub/New.txt")
            .await
            .unwrap()
            .unwrap();
        let state = bridge.index.get_state(item.id).await.unwrap().unwrap();
        assert_eq!(state.state, FileState::Syncing);
        assert_eq!(item.size, Some(3));
        assert_eq!(item.modified, Some(101));
    }

    #[tokio::test]
    async fn rename_local_only_file_requeues_as_upload() {
        let bridge = make_bridge().await;
        bridge
            .write_flush("/Docs/.goutputstream-1", 5, 100)
            .await
            .unwrap();
        let _ = dequeue_all(&bridge.index).await;

        bridge
            .rename("/Docs/.goutputstream-1", "/Docs/Final.txt")
            .await
            .unwrap();

        let ops = dequeue_all(&bridge.index).await;
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].kind, OperationKind::Upload);
        assert_eq!(ops[0].path, "/Docs/Final.txt");
        assert!(
            bridge
                .index
                .get_item_by_path("/Docs/.goutputstream-1")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            bridge
                .index
                .get_item_by_path("/Docs/Final.txt")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn rename_synced_file_queues_move() {
        let bridge = make_bridge().await;
        let docs = bridge
            .index
            .upsert_item(&ItemInput {
                path: "/Docs".into(),
                parent_path: Some("/".into()),
                name: "Docs".into(),
                item_type: ItemType::Dir,
                size: None,
                modified: Some(100),
                hash: None,
                resource_id: Some("rid-docs".into()),
                last_synced_hash: None,
                last_synced_modified: Some(100),
            })
            .await
            .unwrap();
        bridge
            .index
            .set_state(docs.id, FileState::Cached, true, None)
            .await
            .unwrap();
        let record = bridge
            .index
            .upsert_item(&ItemInput {
                path: "/Docs/A.txt".into(),
                parent_path: Some("/Docs".into()),
                name: "A.txt".into(),
                item_type: ItemType::File,
                size: Some(1),
                modified: Some(100),
                hash: Some("abc".into()),
                resource_id: Some("rid-a".into()),
                last_synced_hash: Some("abc".into()),
                last_synced_modified: Some(100),
            })
            .await
            .unwrap();
        bridge
            .index
            .set_state(record.id, FileState::Cached, true, None)
            .await
            .unwrap();

        bridge.rename("/Docs/A.txt", "/Docs/B.txt").await.unwrap();

        let ops = dequeue_all(&bridge.index).await;
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].kind, OperationKind::Move);
        assert_eq!(ops[0].path, "/Docs/B.txt");
        assert!(
            bridge
                .index
                .get_item_by_path("/Docs/A.txt")
                .await
                .unwrap()
                .is_none()
        );
        let moved = bridge
            .index
            .get_item_by_path("/Docs/B.txt")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(moved.resource_id.as_deref(), Some("rid-a"));
    }

    #[tokio::test]
    async fn unlink_local_only_file_drops_item_without_delete_op() {
        let bridge = make_bridge().await;
        bridge.write_flush("/Docs/New.txt", 3, 100).await.unwrap();
        let _ = dequeue_all(&bridge.index).await;

        bridge.unlink_or_rmdir("/Docs/New.txt").await.unwrap();

        assert!(dequeue_all(&bridge.index).await.is_empty());
        assert!(
            bridge
                .index
                .get_item_by_path("/Docs/New.txt")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn unlink_synced_file_queues_delete() {
        let bridge = make_bridge().await;
        let record = bridge
            .index
            .upsert_item(&ItemInput {
                path: "/Docs/A.txt".into(),
                parent_path: Some("/Docs".into()),
                name: "A.txt".into(),
                item_type: ItemType::File,
                size: Some(1),
                modified: Some(100),
                hash: Some("abc".into()),
                resource_id: Some("rid-a".into()),
                last_synced_hash: Some("abc".into()),
                last_synced_modified: Some(100),
            })
            .await
            .unwrap();
        bridge
            .index
            .set_state(record.id, FileState::Cached, true, None)
            .await
            .unwrap();

        bridge.unlink_or_rmdir("/Docs/A.txt").await.unwrap();

        let ops = dequeue_all(&bridge.index).await;
        assert_eq!(ops.len(), 1);
        assert_eq!(ops[0].kind, OperationKind::Delete);
        assert_eq!(ops[0].path, "/Docs/A.txt");
        assert!(
            bridge
                .index
                .get_item_by_path("/Docs/A.txt")
                .await
                .unwrap()
                .is_none()
        );
    }
}
