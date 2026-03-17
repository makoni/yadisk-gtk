    #[tokio::test]
    async fn cancelled_download_reverts_state_to_cloud_only() {
        let server = MockServer::start().await;
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
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(b"hello")
                    .set_delay(std::time::Duration::from_secs(5)),
            )
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let engine = std::sync::Arc::new(make_engine(&server, dir.path()).await);
        let item = engine
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
        let state = engine.index.get_state(item.id).await.unwrap().unwrap();
        assert_eq!(state.state, FileState::Syncing);

        let engine_clone = std::sync::Arc::clone(&engine);
        let handle = tokio::spawn(async move { engine_clone.run_once().await });
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        engine.cancel_transfer("/Docs/A.txt");
        handle.await.unwrap().unwrap();

        let state = engine.index.get_state(item.id).await.unwrap().unwrap();
        assert_eq!(state.state, FileState::CloudOnly);
        assert!(state.pinned);
    }

    #[tokio::test]
    async fn cancelled_upload_reverts_state_to_cached() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/disk/resources/upload"))
            .and(query_param("path", "/Docs/A.txt"))
            .and(query_param("overwrite", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "href": format!("{}/upload", server.uri()),
                "method": "PUT",
                "templated": false
            })))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/upload"))
            .respond_with(
                ResponseTemplate::new(201)
                    .set_delay(std::time::Duration::from_secs(5)),
            )
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let engine = std::sync::Arc::new(make_engine(&server, dir.path()).await);
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

        let engine_clone = std::sync::Arc::clone(&engine);
        let handle = tokio::spawn(async move { engine_clone.run_once().await });
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        engine.cancel_transfer("/Docs/A.txt");
        handle.await.unwrap().unwrap();

        let item = engine
            .index
            .get_item_by_path("/Docs/A.txt")
            .await
            .unwrap()
            .unwrap();
        let state = engine.index.get_state(item.id).await.unwrap().unwrap();
        assert_eq!(state.state, FileState::Cached);
        assert!(state.pinned);
    }

    #[tokio::test]
    async fn evict_path_cancels_pending_download() {
        let server = MockServer::start().await;
        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;

        let item = engine
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

        let op_before = engine.index.dequeue_op().await.unwrap();
        assert!(op_before.is_some());
        engine.index.enqueue_op(&op_before.unwrap()).await.unwrap();

        engine.evict_path("/Docs/A.txt").await.unwrap();

        let op_after = engine.index.dequeue_op().await.unwrap();
        assert!(op_after.is_none(), "pending op should have been deleted by evict_path");

        let state = engine.index.get_state(item.id).await.unwrap().unwrap();
        assert_eq!(state.state, FileState::CloudOnly);
        assert!(!state.pinned);
    }

    #[tokio::test]
    async fn incremental_sync_does_not_delete_items_matching_path_variant_in_remote() {
        let server = MockServer::start().await;
        // Remote returns items with disk:/ prefix
        Mock::given(method("GET"))
            .and(path("/v1/disk/resources"))
            .and(query_param("path", "/"))
            .and(query_param("limit", "100"))
            .and(query_param("offset", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "_embedded": {
                    "limit": 100,
                    "offset": 0,
                    "total": 1,
                    "items": [
                        {"path": "disk:/A.txt", "name": "A.txt", "type": "file", "size": 10, "md5": "aaa"}
                    ]
                }
            })))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;

        // Index item with slash-prefixed path (no resource_id to avoid rename detection)
        let item = engine
            .index
            .upsert_item(&ItemInput {
                path: "/A.txt".into(),
                parent_path: Some("/".into()),
                name: "A.txt".into(),
                item_type: ItemType::File,
                size: Some(10),
                modified: None,
                hash: Some("aaa".into()),
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();
        engine
            .index
            .set_state(item.id, FileState::CloudOnly, false, None)
            .await
            .unwrap();

        let delta = engine.sync_directory_incremental("/").await.unwrap();

        // The local item /A.txt should NOT be deleted because remote has disk:/A.txt
        // which is a path variant match
        assert_eq!(
            delta.deleted, 0,
            "item should not be deleted when remote has matching path variant"
        );
    }

    #[tokio::test]
    async fn delete_op_treats_404_as_success() {
        let server = MockServer::start().await;
        // Remote returns 404 — resource already deleted
        Mock::given(method("DELETE"))
            .and(path("/v1/disk/resources"))
            .and(query_param("path", "/Docs/Gone.txt"))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "message": "Resource not found",
                "error": "DiskNotFoundError"
            })))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;

        // Create an item in the index
        let item = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Docs/Gone.txt".into(),
                parent_path: Some("/Docs".into()),
                name: "Gone.txt".into(),
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
        engine
            .index
            .set_state(item.id, FileState::Syncing, true, None)
            .await
            .unwrap();

        // Enqueue a delete operation
        engine.enqueue_delete("/Docs/Gone.txt").await.unwrap();

        // run_once should succeed (404 treated as success)
        let processed = engine.run_once().await.unwrap();
        assert!(processed, "delete operation should have been processed");

        // The item should be removed from the index
        let after = engine
            .index
            .get_item_by_path("/Docs/Gone.txt")
            .await
            .unwrap();
        assert!(
            after.is_none(),
            "item should be deleted from index after 404 delete"
        );
    }
