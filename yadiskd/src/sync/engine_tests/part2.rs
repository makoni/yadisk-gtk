    #[tokio::test]
    async fn pin_directory_applies_recursively() {
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
        let sub = engine
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
        let b = engine
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
        engine
            .index
            .set_state(music.id, FileState::CloudOnly, false, None)
            .await
            .unwrap();
        engine
            .index
            .set_state(a.id, FileState::Cached, false, None)
            .await
            .unwrap();
        engine
            .index
            .set_state(sub.id, FileState::CloudOnly, false, None)
            .await
            .unwrap();
        engine
            .index
            .set_state(b.id, FileState::CloudOnly, false, None)
            .await
            .unwrap();

        engine.pin_path("/Music", true).await.unwrap();

        for id in [music.id, a.id, sub.id, b.id] {
            let state = engine.index.get_state(id).await.unwrap().unwrap();
            assert!(state.pinned);
        }
    }

    #[tokio::test]
    async fn state_for_directory_reports_cached_when_all_files_cached() {
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
            .set_state(music.id, FileState::CloudOnly, true, None)
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

        assert_eq!(
            engine.state_for_path("/Music").await.unwrap(),
            Some(PathDisplayState::Cached)
        );
    }

    #[tokio::test]
    async fn state_for_directory_prioritizes_error_and_syncing() {
        let server = MockServer::start().await;
        let dir = tempdir().unwrap();
        let engine = make_engine(&server, dir.path()).await;

        let _music = engine
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
        let c = engine
            .index
            .upsert_item(&ItemInput {
                path: "/Music/C.mp3".into(),
                parent_path: Some("/Music".into()),
                name: "C.mp3".into(),
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
            .set_state(a.id, FileState::Cached, true, None)
            .await
            .unwrap();
        engine
            .index
            .set_state(b.id, FileState::Syncing, true, None)
            .await
            .unwrap();
        engine
            .index
            .set_state(c.id, FileState::CloudOnly, false, None)
            .await
            .unwrap();
        assert_eq!(
            engine.state_for_path("/Music").await.unwrap(),
            Some(PathDisplayState::Syncing)
        );

        engine
            .index
            .set_state(b.id, FileState::Error, true, Some("x"))
            .await
            .unwrap();
        assert_eq!(
            engine.state_for_path("/Music").await.unwrap(),
            Some(PathDisplayState::Error)
        );
    }

