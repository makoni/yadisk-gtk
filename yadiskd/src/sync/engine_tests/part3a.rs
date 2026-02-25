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

    #[tokio::test]
    async fn run_once_upload_supports_disk_prefixed_paths() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/disk/resources/upload"))
            .and(query_param("path", "disk:/Docs/New.txt"))
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
            .and(body_bytes(b"new-file"))
            .respond_with(ResponseTemplate::new(201))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;
        let target = cache_path_for(dir.path(), "disk:/Docs/New.txt").unwrap();
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(&target, b"new-file").unwrap();

        engine
            .ingest_local_event(LocalEvent::Upload {
                path: "disk:/Docs/New.txt".into(),
            })
            .await
            .unwrap();
        assert!(engine.run_once().await.unwrap());

        assert!(engine
            .index
            .get_item_by_path("disk:/Docs/New.txt")
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn run_once_delete_supports_disk_prefixed_paths() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/v1/disk/resources"))
            .and(query_param("path", "disk:/Docs/Delete.txt"))
            .and(query_param("permanently", "true"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;
        engine
            .index
            .upsert_item(&ItemInput {
                path: "disk:/Docs/Delete.txt".into(),
                parent_path: Some("disk:/Docs".into()),
                name: "Delete.txt".into(),
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
            .ingest_local_event(LocalEvent::Delete {
                path: "disk:/Docs/Delete.txt".into(),
            })
            .await
            .unwrap();
        assert!(engine.run_once().await.unwrap());
        assert!(engine
            .index
            .get_item_by_path("disk:/Docs/Delete.txt")
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn delete_cancels_pending_upload_for_same_path() {
        let server = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/v1/disk/resources"))
            .and(query_param("path", "disk:/Docs/Race.txt"))
            .and(query_param("permanently", "true"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;
        let target = cache_path_for(dir.path(), "disk:/Docs/Race.txt").unwrap();
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(&target, b"race").unwrap();

        engine
            .ingest_local_event(LocalEvent::Upload {
                path: "disk:/Docs/Race.txt".into(),
            })
            .await
            .unwrap();
        engine
            .ingest_local_event(LocalEvent::Delete {
                path: "disk:/Docs/Race.txt".into(),
            })
            .await
            .unwrap();

        assert!(engine.run_once().await.unwrap());
        assert!(!engine.run_once().await.unwrap());
    }

    #[tokio::test]
    async fn run_once_mkdir_creates_remote_folder_and_sets_cached_state() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/v1/disk/resources"))
            .and(query_param("path", "/Docs/NewFolder"))
            .and(header("authorization", "OAuth test-token"))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "path": "/Docs/NewFolder",
                "name": "NewFolder",
                "type": "dir"
            })))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;
        engine.enqueue_mkdir("/Docs/NewFolder").await.unwrap();
        assert!(engine.run_once().await.unwrap());

        let item = engine
            .index
            .get_item_by_path("/Docs/NewFolder")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(item.item_type, ItemType::Dir);
        let state = engine.index.get_state(item.id).await.unwrap().unwrap();
        assert_eq!(state.state, FileState::Cached);
    }

    #[tokio::test]
    async fn sync_directory_incremental_handles_rename_delete_and_pinned_download() {
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
                    "total": 2,
                    "items": [
                        {"path": "/Docs/New.txt", "name": "New.txt", "type": "file", "size": 10, "resource_id": "rid-1", "md5": "abcd"},
                        {"path": "/Docs/Sub", "name": "Sub", "type": "dir", "resource_id": "rid-sub"}
                    ]
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/disk/resources"))
            .and(query_param("path", "/Docs/Sub"))
            .and(query_param("limit", "100"))
            .and(query_param("offset", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "_embedded": {
                    "limit": 100,
                    "offset": 0,
                    "total": 1,
                    "items": [
                        {"path": "/Docs/Sub/B.txt", "name": "B.txt", "type": "file", "size": 5, "resource_id": "rid-2", "md5": "ef01"}
                    ]
                }
            })))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;

        let old = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Docs/Old.txt".into(),
                parent_path: Some("/Docs".into()),
                name: "Old.txt".into(),
                item_type: ItemType::File,
                size: Some(10),
                modified: None,
                hash: Some("abcd".into()),
                resource_id: Some("rid-1".into()),
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();
        engine
            .index
            .set_state(old.id, FileState::CloudOnly, true, None)
            .await
            .unwrap();

        engine
            .index
            .upsert_item(&ItemInput {
                path: "/Docs/Stale.txt".into(),
                parent_path: Some("/Docs".into()),
                name: "Stale.txt".into(),
                item_type: ItemType::File,
                size: Some(1),
                modified: None,
                hash: None,
                resource_id: Some("rid-3".into()),
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();

        let delta = engine.sync_directory_incremental("/Docs").await.unwrap();
        assert_eq!(delta.deleted, 2);
        assert_eq!(delta.enqueued_downloads, 1);
        assert!(
            engine
                .index
                .get_item_by_path("/Docs/Old.txt")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            engine
                .index
                .get_item_by_path("/Docs/Stale.txt")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            engine
                .index
                .get_item_by_path("/Docs/New.txt")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            engine
                .index
                .get_item_by_path("/Docs/Sub/B.txt")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn sync_directory_incremental_keeps_local_pending_items_without_resource_id() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/disk/resources"))
            .and(query_param("path", "disk:/"))
            .and(query_param("limit", "100"))
            .and(query_param("offset", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "_embedded": {
                    "limit": 100,
                    "offset": 0,
                    "total": 0,
                    "items": []
                }
            })))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;
        let item = engine
            .index
            .upsert_item(&ItemInput {
                path: "disk:/LocalPending.txt".into(),
                parent_path: Some("disk:/".into()),
                name: "LocalPending.txt".into(),
                item_type: ItemType::File,
                size: Some(4),
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

        let delta = engine.sync_directory_incremental("disk:/").await.unwrap();
        assert_eq!(delta.deleted, 0);
        assert!(engine
            .index
            .get_item_by_path("disk:/LocalPending.txt")
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn ingest_local_events_enqueue_upload_delete_and_move() {
        let server = MockServer::start().await;
        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;
        engine
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

        engine
            .ingest_local_event(LocalEvent::Upload {
                path: "/Docs/A.txt".into(),
            })
            .await
            .unwrap();
        engine
            .ingest_local_event(LocalEvent::Delete {
                path: "/Docs/A.txt".into(),
            })
            .await
            .unwrap();
        engine
            .ingest_local_event(LocalEvent::Move {
                from: "/Docs/A.txt".into(),
                to: "/Docs/B.txt".into(),
            })
            .await
            .unwrap();
        engine
            .ingest_local_event(LocalEvent::Mkdir {
                path: "/Docs/NewFolder".into(),
            })
            .await
            .unwrap();

        let first = engine.index.dequeue_op().await.unwrap().unwrap();
        assert_eq!(first.kind, OperationKind::Delete);
        let second = engine.index.dequeue_op().await.unwrap().unwrap();
        assert_eq!(second.kind, OperationKind::Move);
        let third = engine.index.dequeue_op().await.unwrap().unwrap();
        assert_eq!(third.kind, OperationKind::Mkdir);
    }

    #[tokio::test]
    async fn ingest_local_upload_for_new_file_creates_item() {
        let server = MockServer::start().await;
        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;
        let local = cache_path_for(dir.path(), "/Docs/New.txt").unwrap();
        tokio::fs::create_dir_all(local.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&local, b"new").await.unwrap();

        engine
            .ingest_local_event(LocalEvent::Upload {
                path: "/Docs/New.txt".into(),
            })
            .await
            .unwrap();

        let op = engine.index.dequeue_op().await.unwrap().unwrap();
        assert_eq!(op.kind, OperationKind::Upload);
        assert_eq!(op.path, "/Docs/New.txt");

        let item = engine
            .index
            .get_item_by_path("/Docs/New.txt")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(item.item_type, ItemType::File);
    }

    #[tokio::test]
    async fn conflict_resolution_keep_both_records_conflict() {
        let server = MockServer::start().await;
        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;
        let base = FileMetadata {
            modified: 1,
            hash: Some("base".into()),
        };
        let local = FileMetadata {
            modified: 2,
            hash: Some("local".into()),
        };
        let remote = FileMetadata {
            modified: 3,
            hash: Some("remote".into()),
        };
        let decision = engine
            .resolve_conflict_and_record("/Docs/A.txt", Some(&base), &local, &remote)
            .await
            .unwrap();
        assert!(matches!(decision, ConflictDecision::KeepBoth { .. }));
        let conflicts = engine.index.list_conflicts().await.unwrap();
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].path, "/Docs/A.txt");
    }

    #[tokio::test]
    async fn run_once_move_uses_payload_and_updates_index() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/v1/disk/resources/move"))
            .and(query_param("from", "/Docs/A.txt"))
            .and(query_param("path", "/Docs/B.txt"))
            .respond_with(ResponseTemplate::new(202).set_body_json(serde_json::json!({
                "href": format!("{}/v1/disk/operations/77", server.uri()),
                "method": "GET",
                "templated": false
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/disk/operations/77"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "status": "success"
            })))
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
                size: Some(1),
                modified: None,
                hash: None,
                resource_id: Some("rid-1".into()),
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();
        engine
            .enqueue_move("/Docs/A.txt", "/Docs/B.txt", "move")
            .await
            .unwrap();
        assert!(engine.run_once().await.unwrap());
        assert!(
            engine
                .index
                .get_item_by_path("/Docs/A.txt")
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            engine
                .index
                .get_item_by_path("/Docs/B.txt")
                .await
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn run_once_move_with_missing_source_falls_back_to_upload() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/disk/resources/upload"))
            .and(query_param("path", "/Docs/B.txt"))
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
            .and(body_bytes(b"payload"))
            .respond_with(ResponseTemplate::new(201))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;
        let target = cache_path_for(dir.path(), "/Docs/B.txt").unwrap();
        std::fs::create_dir_all(target.parent().unwrap()).unwrap();
        std::fs::write(&target, b"payload").unwrap();

        engine
            .index
            .enqueue_op(&Operation {
                kind: OperationKind::Move,
                path: "/Docs/B.txt".into(),
                payload: Some(
                    serde_json::json!({
                        "from": "/Docs/.tmp-atomic",
                        "path": "/Docs/B.txt",
                        "overwrite": true,
                        "action": "move"
                    })
                    .to_string(),
                ),
                attempt: 0,
                retry_at: None,
                priority: 60,
            })
            .await
            .unwrap();

        assert!(engine.run_once().await.unwrap());
        let item = engine
            .index
            .get_item_by_path("/Docs/B.txt")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(item.item_type, ItemType::File);
        let state = engine.index.get_state(item.id).await.unwrap().unwrap();
        assert_eq!(state.state, FileState::Cached);
    }
