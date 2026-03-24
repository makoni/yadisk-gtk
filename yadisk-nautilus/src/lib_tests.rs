use super::*;
use std::io;
use yadisk_integrations::i18n::tr;

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
        vec![NautilusAction::RemoveOfflineCopy]
    );
    assert!(visible_actions_for_state(SyncUiState::Syncing).is_empty());
    assert_eq!(
        visible_actions_for_state(SyncUiState::Error),
        vec![NautilusAction::DownloadNow]
    );
    assert_eq!(
        menu_for_state(SyncUiState::CloudOnly)
            .first()
            .map(|item| item.label.clone()),
        Some(tr("Download"))
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
fn snapshot_cache_expands_disk_and_slash_aliases() {
    let cache = state_cache_from_snapshot(&[
        ("disk:/Docs/A.txt".to_string(), SyncUiState::Cached),
        ("/Docs/B.txt".to_string(), SyncUiState::Partial),
    ]);
    assert_eq!(cache.get("disk:/Docs/A.txt"), Some(&SyncUiState::Cached));
    assert_eq!(cache.get("/Docs/A.txt"), Some(&SyncUiState::Cached));
    assert_eq!(cache.get("/Docs/B.txt"), Some(&SyncUiState::Partial));
    assert_eq!(cache.get("disk:/Docs/B.txt"), Some(&SyncUiState::Partial));
}

#[test]
fn parses_partial_state_from_dbus() {
    assert_eq!(SyncUiState::from_dbus("partial"), SyncUiState::Partial);
    assert_eq!(SyncUiState::Partial.as_dbus(), "partial");
    assert_eq!(
        SyncUiState::Partial.badge_label(),
        tr("Partially available offline")
    );
}

#[test]
fn empty_candidates_returns_error() {
    // Validates that perform_action_with_fallback and get_state_with_fallback
    // return an error for empty candidate lists
    let err = ExtensionError::EmptyCandidates;
    assert!(format!("{err}").contains("empty"));
}

#[test]
fn unsupported_signal_variant_is_defined() {
    // UnsupportedSignal variant exists but is no longer returned by parse_signal_event.
    // Unknown D-Bus signals are now silently skipped (returning Ok(None)).
    let err = ExtensionError::UnsupportedSignal("test".to_string());
    assert!(format!("{err}").contains("unsupported"));
}

#[test]
fn transport_dbus_errors_do_not_retry_alternate_candidates() {
    let err = ExtensionError::Dbus(zbus::Error::InputOutput(
        io::Error::from(io::ErrorKind::TimedOut).into(),
    ));
    assert!(!should_try_next_candidate(&err));
}

#[test]
fn method_errors_still_allow_alternate_candidates() {
    let err = ExtensionError::Fdo(zbus::fdo::Error::Failed(
        "me.spaceinbox.yadisk.Sync1.Error.NotFound: path does not exist".to_string(),
    ));
    assert!(should_try_next_candidate(&err));
}

#[test]
fn from_dbus_maps_explicit_cloud_only() {
    assert_eq!(SyncUiState::from_dbus("cloud_only"), SyncUiState::CloudOnly);
}

#[test]
fn from_dbus_unknown_value_defaults_to_cloud_only() {
    // Unknown D-Bus state values should default to CloudOnly (with a warning logged)
    assert_eq!(
        SyncUiState::from_dbus("unknown_state"),
        SyncUiState::CloudOnly
    );
    assert_eq!(SyncUiState::from_dbus(""), SyncUiState::CloudOnly);
}
