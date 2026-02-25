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
