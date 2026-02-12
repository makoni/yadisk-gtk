#![allow(dead_code)]

use std::path::PathBuf;

use thiserror::Error;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use yadisk_core::{OperationStatus, ResourceType, YadiskClient};

use super::backoff::Backoff;
use super::index::{FileState, IndexError, IndexStore, ItemInput, ItemType};
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
        let list = self.client.list_directory(path, Some(100), Some(0)).await?;
        for item in &list.items {
            let input = ItemInput {
                path: item.path.clone(),
                name: item.name.clone(),
                item_type: match item.resource_type {
                    ResourceType::File => ItemType::File,
                    ResourceType::Dir => ItemType::Dir,
                },
                size: item.size.map(|v| v as i64),
                modified: parse_modified(item.modified.as_deref())?,
                hash: None,
                resource_id: None,
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

        Ok(list.items.len())
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
                attempt: 0,
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
                attempt: 0,
            })
            .await?)
    }

    pub async fn run_once(&self) -> Result<bool, EngineError> {
        let Some(op) = self.index.dequeue_op().await? else {
            return Ok(false);
        };

        match op.kind {
            OperationKind::Download => self.execute_download(&op.path).await?,
            OperationKind::Upload => self.execute_upload(&op.path).await?,
            OperationKind::Delete => {
                let link = self.client.delete_resource(&op.path, true).await?;
                if let Some(link) = link {
                    self.wait_for_operation(link.href.as_str()).await?;
                }
            }
            OperationKind::Move => {
                // Move needs both source/destination; not wired yet in Operation payload.
            }
        }

        Ok(true)
    }

    async fn execute_download(&self, path: &str) -> Result<(), EngineError> {
        let link = self.client.get_download_link(path).await?;
        let target = cache_path_for(&self.cache_root, path)?;
        self.transfer
            .download_to_path(link.href.as_str(), &target)
            .await?;

        let item = self
            .index
            .get_item_by_path(path)
            .await?
            .ok_or_else(|| EngineError::MissingItem(path.to_string()))?;
        self.index
            .set_state(item.id, FileState::Cached, true, None)
            .await?;
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
            .set_state(item.id, FileState::Cached, true, None)
            .await?;
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
}

fn parse_modified(value: Option<&str>) -> Result<Option<i64>, time::error::Parse> {
    let Some(value) = value else {
        return Ok(None);
    };
    let parsed = OffsetDateTime::parse(value, &Rfc3339)?;
    Ok(Some(parsed.unix_timestamp()))
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
                name: "A.txt".into(),
                item_type: ItemType::File,
                size: Some(5),
                modified: None,
                hash: None,
                resource_id: None,
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
                name: "A.txt".into(),
                item_type: ItemType::File,
                size: Some(7),
                modified: None,
                hash: None,
                resource_id: None,
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
}
