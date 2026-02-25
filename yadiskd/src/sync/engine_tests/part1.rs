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

