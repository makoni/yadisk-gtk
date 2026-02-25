use super::*;
use crate::storage::OAuthState;
use crate::sync::index::{FileState, IndexStore, ItemInput, ItemType};
use crate::sync::local_watcher::LocalEvent;
use sqlx::SqlitePool;
use tempfile::tempdir;
use wiremock::matchers::{body_string_contains, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn expands_tilde_to_home_sync_dir() {
    let home = PathBuf::from("/tmp/home-user");
    assert_eq!(
        expand_with_home("~/Yandex Disk", &home),
        PathBuf::from("/tmp/home-user/Yandex Disk")
    );
}

#[test]
fn reads_intervals_from_env_or_default() {
    assert_eq!(read_u64_env("NO_SUCH_ENV_FOR_TEST", 42), 42);
}

#[test]
fn local_watcher_is_enabled_by_default() {
    assert!(read_bool_env("NO_SUCH_BOOL_ENV_FOR_TEST", true));
}

#[test]
fn detects_enosys_in_error_chain() {
    let err = anyhow::Error::new(std::io::Error::from_raw_os_error(38));
    assert!(error_contains_enosys(&err));
}

#[test]
fn tray_state_prioritizes_error_then_syncing() {
    let mut states = HashMap::new();
    assert_eq!(tray_state_from_states(&states), TraySyncState::Normal);

    states.insert("/A".to_string(), "syncing");
    assert_eq!(tray_state_from_states(&states), TraySyncState::Syncing);

    states.insert("/B".to_string(), "cached");
    assert_eq!(tray_state_from_states(&states), TraySyncState::Syncing);

    states.insert("/C".to_string(), "error");
    assert_eq!(tray_state_from_states(&states), TraySyncState::Error);
}

#[test]
fn normalizes_local_events_to_remote_root_prefix() {
    let upload = normalize_local_event_for_remote_root(
        LocalEvent::Upload {
            path: "/Docs/A.txt".into(),
        },
        "disk:/",
    );
    assert_eq!(
        upload,
        LocalEvent::Upload {
            path: "disk:/Docs/A.txt".into()
        }
    );

    let delete = normalize_local_event_for_remote_root(
        LocalEvent::Delete {
            path: "disk:/Docs/A.txt".into(),
        },
        "/",
    );
    assert_eq!(
        delete,
        LocalEvent::Delete {
            path: "/Docs/A.txt".into()
        }
    );

    let mv = normalize_local_event_for_remote_root(
        LocalEvent::Move {
            from: "/Docs/A.txt".into(),
            to: "/Docs/B.txt".into(),
        },
        "disk:/",
    );
    assert_eq!(
        mv,
        LocalEvent::Move {
            from: "disk:/Docs/A.txt".into(),
            to: "disk:/Docs/B.txt".into()
        }
    );
}

#[tokio::test]
async fn deduplicates_same_upload_fingerprint() {
    let sync_dir = tempdir().unwrap();
    let sync_file = sync_dir.path().join("Docs/A.txt");
    tokio::fs::create_dir_all(sync_file.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&sync_file, b"hello").await.unwrap();

    let fp1 = upload_fingerprint(sync_dir.path(), "disk:/Docs/A.txt")
        .await
        .unwrap();
    let mut seen = HashMap::new();
    assert!(should_process_upload_event(
        &mut seen,
        "disk:/Docs/A.txt",
        fp1
    ));
    assert!(!should_process_upload_event(
        &mut seen,
        "disk:/Docs/A.txt",
        fp1
    ));

    tokio::fs::write(&sync_file, b"hello world").await.unwrap();
    let fp2 = upload_fingerprint(sync_dir.path(), "disk:/Docs/A.txt")
        .await
        .unwrap();
    assert!(should_process_upload_event(
        &mut seen,
        "disk:/Docs/A.txt",
        fp2
    ));
}

#[tokio::test]
async fn mirrors_local_upload_to_cache() {
    let sync_dir = tempdir().unwrap();
    let cache_dir = tempdir().unwrap();
    let sync_file = sync_dir.path().join("Docs/A.txt");
    tokio::fs::create_dir_all(sync_file.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&sync_file, b"local-data").await.unwrap();

    mirror_local_event_to_cache(
        sync_dir.path(),
        cache_dir.path(),
        &LocalEvent::Upload {
            path: "/Docs/A.txt".into(),
        },
    )
    .await
    .unwrap();

    let cache_file = crate::sync::paths::cache_path_for(cache_dir.path(), "/Docs/A.txt").unwrap();
    let cached = tokio::fs::read(&cache_file).await.unwrap();
    assert_eq!(cached, b"local-data");
}

#[tokio::test]
async fn skips_cached_upload_when_local_matches_cache() {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    let index = IndexStore::from_pool(pool);
    index.init().await.unwrap();
    let sync_dir = tempdir().unwrap();
    let cache_dir = tempdir().unwrap();

    let item = index
        .upsert_item(&ItemInput {
            path: "disk:/Docs/A.txt".into(),
            parent_path: Some("disk:/Docs".into()),
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
    index
        .set_state(item.id, FileState::Cached, true, None)
        .await
        .unwrap();

    let sync_file = sync_dir.path().join("Docs/A.txt");
    tokio::fs::create_dir_all(sync_file.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&sync_file, b"hello").await.unwrap();

    let cache_file =
        crate::sync::paths::cache_path_for(cache_dir.path(), "disk:/Docs/A.txt").unwrap();
    tokio::fs::create_dir_all(cache_file.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&cache_file, b"hello").await.unwrap();

    let client = YadiskClient::with_base_url("http://127.0.0.1:9", "token").unwrap();
    let engine = SyncEngine::new(client, index, cache_dir.path().to_path_buf());
    assert!(
        should_skip_local_upload_event(
            &engine,
            sync_dir.path(),
            cache_dir.path(),
            "disk:/Docs/A.txt"
        )
        .await
    );

    tokio::fs::write(&sync_file, b"hello2").await.unwrap();
    assert!(
        !should_skip_local_upload_event(
            &engine,
            sync_dir.path(),
            cache_dir.path(),
            "disk:/Docs/A.txt"
        )
        .await
    );
}

#[tokio::test]
async fn retries_once_after_unauthorized_with_refreshed_token() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/v1/disk"))
        .and(header("authorization", "OAuth old-token"))
        .respond_with(ResponseTemplate::new(401).set_body_string("unauthorized"))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/token"))
        .and(body_string_contains("grant_type=refresh_token"))
        .and(body_string_contains("refresh_token=refresh-1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "new-token",
            "token_type": "bearer",
            "expires_in": 3600,
            "refresh_token": "refresh-2",
            "scope": "disk:read"
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/disk"))
        .and(header("authorization", "OAuth new-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "total_space": 1000,
            "used_space": 100,
            "trash_size": 0,
            "is_paid": false
        })))
        .mount(&server)
        .await;

    let oauth_client =
        OAuthClient::with_base_url(&server.uri(), "client-id", "secret").expect("oauth client");
    let mut provider = TokenProvider::new(
        OAuthState {
            access_token: "old-token".into(),
            refresh_token: Some("refresh-1".into()),
            expires_at: Some(i64::MAX),
            scope: Some("disk:read".into()),
            token_type: Some("bearer".into()),
        },
        Some(oauth_client),
    );

    let info = fetch_disk_info_with_retry(&mut provider, Some(&server.uri()))
        .await
        .expect("retry should succeed");
    assert_eq!(info.total_space, 1000);
    assert_eq!(provider.state().access_token, "new-token");
}

#[tokio::test]
async fn cache_eviction_skips_pinned_files() {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    let index = IndexStore::from_pool(pool);
    index.init().await.unwrap();
    let cache_dir = tempdir().unwrap();

    for path in ["/A.txt", "/B.txt", "/C.txt"] {
        let item = index
            .upsert_item(&ItemInput {
                path: path.to_string(),
                parent_path: Some("/".to_string()),
                name: path.trim_start_matches('/').to_string(),
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
        let pinned = path == "/B.txt";
        index
            .set_state(item.id, FileState::Cached, pinned, None)
            .await
            .unwrap();
        let local = crate::sync::paths::cache_path_for(cache_dir.path(), path).unwrap();
        if let Some(parent) = local.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(local, b"12345").await.unwrap();
    }

    let client = YadiskClient::with_base_url("http://127.0.0.1:9", "token").unwrap();
    let engine = SyncEngine::new(client, index, cache_dir.path().to_path_buf());
    run_cache_eviction_once(&engine, cache_dir.path(), "/", 8)
        .await
        .unwrap();

    let states = engine
        .list_path_states_with_pin_by_prefix("/")
        .await
        .unwrap();
    let unpinned_cached = states
        .iter()
        .filter(|(_, state, pinned)| !*pinned && matches!(state, FileState::Cached))
        .count();
    assert_eq!(unpinned_cached, 1);
    assert!(
        tokio::fs::metadata(cache_dir.path().join("B.txt"))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn materialize_creates_placeholder_tree_from_index() {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    let index = IndexStore::from_pool(pool);
    index.init().await.unwrap();
    let sync_dir = tempdir().unwrap();

    index
        .upsert_item(&ItemInput {
            path: "/Docs".into(),
            parent_path: Some("/".into()),
            name: "Docs".into(),
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
    index
        .upsert_item(&ItemInput {
            path: "/Docs/A.txt".into(),
            parent_path: Some("/Docs".into()),
            name: "A.txt".into(),
            item_type: ItemType::File,
            size: Some(123),
            modified: None,
            hash: None,
            resource_id: None,
            last_synced_hash: None,
            last_synced_modified: None,
        })
        .await
        .unwrap();

    let client = YadiskClient::with_base_url("http://127.0.0.1:9", "token").unwrap();
    let engine = SyncEngine::new(client, index, sync_dir.path().to_path_buf());
    let count = materialize_sync_tree(&engine, sync_dir.path(), sync_dir.path(), "/")
        .await
        .unwrap();

    assert_eq!(count, 2);
    assert!(
        tokio::fs::metadata(sync_dir.path().join("Docs"))
            .await
            .is_ok()
    );
    assert!(
        tokio::fs::metadata(sync_dir.path().join("Docs/A.txt"))
            .await
            .is_ok()
    );
}

#[tokio::test]
async fn materialize_copies_cached_file_to_sync_tree() {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    let index = IndexStore::from_pool(pool);
    index.init().await.unwrap();
    let sync_dir = tempdir().unwrap();
    let cache_dir = tempdir().unwrap();

    let item = index
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
    index
        .set_state(item.id, FileState::Cached, true, None)
        .await
        .unwrap();

    let cache_path = crate::sync::paths::cache_path_for(cache_dir.path(), "/Docs/A.txt").unwrap();
    tokio::fs::create_dir_all(cache_path.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&cache_path, b"hello").await.unwrap();

    let client = YadiskClient::with_base_url("http://127.0.0.1:9", "token").unwrap();
    let engine = SyncEngine::new(client, index, cache_dir.path().to_path_buf());
    materialize_sync_tree(&engine, sync_dir.path(), cache_dir.path(), "/")
        .await
        .unwrap();

    let local = sync_dir.path().join("Docs/A.txt");
    assert_eq!(tokio::fs::read(local).await.unwrap(), b"hello");
}

#[tokio::test]
async fn materialize_updates_existing_files_for_state_transitions() {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    let index = IndexStore::from_pool(pool);
    index.init().await.unwrap();
    let sync_dir = tempdir().unwrap();
    let cache_dir = tempdir().unwrap();

    let cloud_item = index
        .upsert_item(&ItemInput {
            path: "/Docs/CloudOnly.txt".into(),
            parent_path: Some("/Docs".into()),
            name: "CloudOnly.txt".into(),
            item_type: ItemType::File,
            size: Some(9),
            modified: None,
            hash: None,
            resource_id: None,
            last_synced_hash: None,
            last_synced_modified: None,
        })
        .await
        .unwrap();
    index
        .set_state(cloud_item.id, FileState::CloudOnly, false, None)
        .await
        .unwrap();

    let cached_item = index
        .upsert_item(&ItemInput {
            path: "/Docs/Cached.txt".into(),
            parent_path: Some("/Docs".into()),
            name: "Cached.txt".into(),
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
    index
        .set_state(cached_item.id, FileState::Cached, true, None)
        .await
        .unwrap();

    let local_docs = sync_dir.path().join("Docs");
    tokio::fs::create_dir_all(&local_docs).await.unwrap();
    tokio::fs::write(local_docs.join("CloudOnly.txt"), b"keep-me")
        .await
        .unwrap();
    tokio::fs::write(local_docs.join("Cached.txt"), b"")
        .await
        .unwrap();

    let cached_path =
        crate::sync::paths::cache_path_for(cache_dir.path(), "/Docs/Cached.txt").unwrap();
    tokio::fs::create_dir_all(cached_path.parent().unwrap())
        .await
        .unwrap();
    tokio::fs::write(&cached_path, b"hello").await.unwrap();

    let client = YadiskClient::with_base_url("http://127.0.0.1:9", "token").unwrap();
    let engine = SyncEngine::new(client, index, cache_dir.path().to_path_buf());
    materialize_sync_tree(&engine, sync_dir.path(), cache_dir.path(), "/")
        .await
        .unwrap();

    let cloud_meta = tokio::fs::metadata(local_docs.join("CloudOnly.txt"))
        .await
        .unwrap();
    assert_eq!(cloud_meta.len(), 0);
    assert_eq!(
        tokio::fs::read(local_docs.join("Cached.txt"))
            .await
            .unwrap(),
        b"hello"
    );
}
