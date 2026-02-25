    #[tokio::test]
    async fn e2e_sync_loop_cloud_list_to_cached_state() {
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
                        {"path": "/Docs/A.txt", "name": "A.txt", "type": "file", "size": 5, "resource_id": "rid-1", "md5": "5d41402abc4b2a76b9719d911017c592"}
                    ]
                }
            })))
            .mount(&server)
            .await;
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
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello"))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;
        assert_eq!(engine.sync_directory_once("/Docs").await.unwrap(), 1);
        engine.enqueue_download("/Docs/A.txt").await.unwrap();
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
    async fn upload_conflict_downloads_remote_when_only_remote_changed() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/disk/resources"))
            .and(query_param("path", "/Docs/A.txt"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "path": "/Docs/A.txt",
                "name": "A.txt",
                "type": "file",
                "size": 5,
                "modified": "2024-01-01T00:00:00Z",
                "md5": "7d793037a0760186574b0282f2f435e7"
            })))
            .mount(&server)
            .await;
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
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"world"))
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
                modified: Some(1),
                hash: Some("5d41402abc4b2a76b9719d911017c592".into()),
                resource_id: None,
                last_synced_hash: Some("5d41402abc4b2a76b9719d911017c592".into()),
                last_synced_modified: Some(1),
            })
            .await
            .unwrap();
        let source = cache_path_for(dir.path(), "/Docs/A.txt").unwrap();
        tokio::fs::create_dir_all(source.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&source, b"hello").await.unwrap();

        engine.enqueue_upload("/Docs/A.txt").await.unwrap();
        assert!(engine.run_once().await.unwrap());

        let downloaded = tokio::fs::read(&source).await.unwrap();
        assert_eq!(downloaded, b"world");
    }

    #[tokio::test]
    async fn run_once_does_not_requeue_permanent_errors() {
        let server = MockServer::start().await;
        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;
        let op = Operation {
            kind: OperationKind::Download,
            path: "/Docs/Missing.txt".into(),
            payload: None,
            attempt: 0,
            retry_at: None,
            priority: 10,
        };
        engine.index.enqueue_op(&op).await.unwrap();

        let err = engine
            .run_once()
            .await
            .expect_err("expected permanent error");
        assert!(matches!(err, EngineError::MissingItem(_)));
        assert!(engine.index.dequeue_op().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn run_once_stops_requeue_at_max_attempts() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/disk/resources/download"))
            .and(query_param("path", "/Docs/A.txt"))
            .respond_with(ResponseTemplate::new(503).set_body_string("temporary error"))
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
                resource_id: None,
                last_synced_hash: None,
                last_synced_modified: None,
            })
            .await
            .unwrap();
        engine
            .index
            .enqueue_op(&Operation {
                kind: OperationKind::Download,
                path: "/Docs/A.txt".into(),
                payload: None,
                attempt: MAX_RETRY_ATTEMPTS - 1,
                retry_at: None,
                priority: 10,
            })
            .await
            .unwrap();

        let err = engine
            .run_once()
            .await
            .expect_err("expected max-attempt failure");
        assert!(matches!(err, EngineError::Api(_)));
        assert!(engine.index.dequeue_op().await.unwrap().is_none());
    }
