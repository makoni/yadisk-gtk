use super::*;

#[test]
fn maps_state_to_emblem_and_actions() {
    assert_eq!(
        emblem_for_state(SyncUiState::CloudOnly),
        "cloud-outline-thin-symbolic"
    );
    assert_eq!(
        visible_actions_for_state(SyncUiState::CloudOnly),
        vec![NautilusAction::DownloadNow]
    );
    assert_eq!(
        visible_actions_for_state(SyncUiState::Partial),
        vec![
            NautilusAction::DownloadNow,
            NautilusAction::RemoveOfflineCopy
        ]
    );
    assert_eq!(
        visible_actions_for_state(SyncUiState::Cached),
        vec![NautilusAction::RemoveOfflineCopy, NautilusAction::RetrySync]
    );
    assert_eq!(
        menu_for_state(SyncUiState::CloudOnly)
            .first()
            .map(|item| item.label),
        Some("Download")
    );
    assert_eq!(
        emblem_for_state(SyncUiState::Partial),
        "cloud-outline-thin-symbolic"
    );
}

#[test]
fn maps_local_path_to_disk_and_legacy_remote_candidates() {
    let sync_root = PathBuf::from("/home/user/Yandex Disk");
    let local_path = PathBuf::from("/home/user/Yandex Disk/Docs/A.txt");
    let candidates = map_local_to_remote_candidates(&local_path, &sync_root).unwrap();
    assert_eq!(candidates[0], "disk:/Docs/A.txt");
    assert_eq!(candidates[1], "/Docs/A.txt");
}

#[test]
fn rejects_path_outside_sync_root() {
    let sync_root = PathBuf::from("/home/user/Yandex Disk");
    let local_path = PathBuf::from("/home/user/Other/file.txt");
    let err = map_local_to_remote_candidates(&local_path, &sync_root).unwrap_err();
    assert!(matches!(err, ExtensionError::OutsideSyncRoot));
}

#[test]
fn maps_remote_path_back_to_local_path() {
    let sync_root = PathBuf::from("/home/user/Yandex Disk");
    assert_eq!(
        map_remote_to_local_path("disk:/Docs/A.txt", &sync_root),
        PathBuf::from("/home/user/Yandex Disk/Docs/A.txt")
    );
    assert_eq!(
        map_remote_to_local_path("/Docs/B.txt", &sync_root),
        PathBuf::from("/home/user/Yandex Disk/Docs/B.txt")
    );
}

#[test]
fn parses_partial_state_from_dbus() {
    assert_eq!(SyncUiState::from_dbus("partial"), SyncUiState::Partial);
    assert_eq!(SyncUiState::Partial.as_dbus(), "partial");
    assert_eq!(
        SyncUiState::Partial.badge_label(),
        "Partially available offline"
    );
}
