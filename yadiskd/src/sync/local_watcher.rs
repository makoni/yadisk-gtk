#![allow(dead_code)]

use std::path::{Path, PathBuf};

use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalEvent {
    Upload { path: String },
    Mkdir { path: String },
    Delete { path: String },
    Move { from: String, to: String },
}

pub fn start_notify_watcher(
    root: &Path,
) -> notify::Result<(RecommendedWatcher, mpsc::UnboundedReceiver<LocalEvent>)> {
    let (tx, rx) = mpsc::unbounded_channel();
    let root = root.to_path_buf();
    let watch_root = root.clone();
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<Event>| {
        if let Ok(event) = res {
            for local in map_event(&watch_root, event) {
                let _ = tx.send(local);
            }
        }
    })?;
    watcher.watch(root.as_path(), RecursiveMode::Recursive)?;
    Ok((watcher, rx))
}

fn map_event(root: &Path, event: Event) -> Vec<LocalEvent> {
    match event.kind {
        EventKind::Modify(notify::event::ModifyKind::Name(mode)) => {
            if event.paths.len() >= 2 {
                let from = to_remote_path(root, &event.paths[0]);
                let to = to_remote_path(root, &event.paths[1]);
                match (from, to) {
                    (Some(from), Some(to)) => return vec![LocalEvent::Move { from, to }],
                    (Some(path), None) => return vec![LocalEvent::Delete { path }],
                    (None, Some(_)) => {
                        if let Some(event) = map_created_path(root, &event.paths[1]) {
                            return vec![event];
                        }
                    }
                    (None, None) => {}
                }
            }
            if event.paths.len() == 1 {
                let path = &event.paths[0];
                match mode {
                    notify::event::RenameMode::From => {
                        if let Some(remote) = to_remote_path(root, path) {
                            return vec![LocalEvent::Delete { path: remote }];
                        }
                    }
                    notify::event::RenameMode::To => {
                        if let Some(created) = map_created_path(root, path) {
                            return vec![created];
                        }
                    }
                    _ => {
                        if let Some(created) = map_created_path(root, path) {
                            return vec![created];
                        }
                        if let Some(remote) = to_remote_path(root, path) {
                            return vec![LocalEvent::Delete { path: remote }];
                        }
                    }
                }
            }
            Vec::new()
        }
        EventKind::Create(_) => event
            .paths
            .into_iter()
            .filter_map(|path| map_created_path(root, &path))
            .collect(),
        EventKind::Modify(notify::event::ModifyKind::Data(_)) => event
            .paths
            .into_iter()
            .filter_map(|path| map_modified_path(root, &path))
            .collect(),
        EventKind::Access(notify::event::AccessKind::Close(notify::event::AccessMode::Write)) => {
            event
                .paths
                .into_iter()
                .filter_map(|path| map_modified_path(root, &path))
                .collect()
        }
        EventKind::Remove(_) => event
            .paths
            .into_iter()
            .filter_map(|path| to_remote_path(root, &path))
            .map(|path| LocalEvent::Delete { path })
            .collect(),
        _ => Vec::new(),
    }
}

fn is_ignored_temporary_name(name: &str) -> bool {
    name.starts_with(".goutputstream-")
        || name.starts_with(".~lock.") && name.ends_with('#')
        || name.starts_with(".#")
        || name.starts_with("~$")
        || name.starts_with(".nfs")
        || name.ends_with(".swp")
        || name.ends_with(".swo")
        || name.ends_with(".swx")
        || name.ends_with('~')
}

fn is_ignored_temp_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(is_ignored_temporary_name)
        .unwrap_or(false)
}

fn map_created_path(root: &Path, path: &Path) -> Option<LocalEvent> {
    let remote = to_remote_path(root, path)?;
    let meta = std::fs::symlink_metadata(path).ok()?;
    if meta.file_type().is_symlink() {
        return None;
    }
    if meta.is_dir() {
        Some(LocalEvent::Mkdir { path: remote })
    } else {
        Some(LocalEvent::Upload { path: remote })
    }
}

fn map_modified_path(root: &Path, path: &Path) -> Option<LocalEvent> {
    let remote = to_remote_path(root, path)?;
    let meta = std::fs::symlink_metadata(path).ok()?;
    if meta.file_type().is_symlink() || meta.is_dir() {
        return None;
    }
    Some(LocalEvent::Upload { path: remote })
}

fn to_remote_path(root: &Path, path: &Path) -> Option<String> {
    if is_ignored_temp_path(path) {
        return None;
    }
    let relative = path.strip_prefix(root).ok()?;
    let remote = PathBuf::from("/").join(relative);
    Some(remote.to_string_lossy().replace('\\', "/"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_modify_event_to_upload() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let file = root.join("Docs/A.txt");
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, b"x").unwrap();
        let event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Any,
            )),
            paths: vec![file],
            attrs: Default::default(),
        };
        let mapped = map_event(root, event);
        assert_eq!(
            mapped,
            vec![LocalEvent::Upload {
                path: "/Docs/A.txt".into()
            }]
        );
    }

    #[test]
    fn maps_create_dir_event_to_mkdir() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let dir = root.join("Docs");
        std::fs::create_dir_all(&dir).unwrap();
        let event = Event {
            kind: EventKind::Create(notify::event::CreateKind::Folder),
            paths: vec![dir],
            attrs: Default::default(),
        };
        let mapped = map_event(root, event);
        assert_eq!(
            mapped,
            vec![LocalEvent::Mkdir {
                path: "/Docs".into()
            }]
        );
    }

    #[test]
    fn maps_rename_event_to_move() {
        let root = Path::new("/tmp/root");
        let event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Name(
                notify::event::RenameMode::Both,
            )),
            paths: vec![
                PathBuf::from("/tmp/root/Docs/A.txt"),
                PathBuf::from("/tmp/root/Docs/B.txt"),
            ],
            attrs: Default::default(),
        };
        let mapped = map_event(root, event);
        assert_eq!(
            mapped,
            vec![LocalEvent::Move {
                from: "/Docs/A.txt".into(),
                to: "/Docs/B.txt".into()
            }]
        );
    }

    #[test]
    fn maps_rename_outside_root_to_delete() {
        let root = Path::new("/tmp/root");
        let event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Name(
                notify::event::RenameMode::Both,
            )),
            paths: vec![
                PathBuf::from("/tmp/root/Docs/A.txt"),
                PathBuf::from("/tmp/Trash/A.txt"),
            ],
            attrs: Default::default(),
        };
        let mapped = map_event(root, event);
        assert_eq!(
            mapped,
            vec![LocalEvent::Delete {
                path: "/Docs/A.txt".into()
            }]
        );
    }

    #[test]
    fn maps_rename_from_single_path_to_delete() {
        let root = Path::new("/tmp/root");
        let event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Name(
                notify::event::RenameMode::From,
            )),
            paths: vec![PathBuf::from("/tmp/root/Docs/A.txt")],
            attrs: Default::default(),
        };
        let mapped = map_event(root, event);
        assert_eq!(
            mapped,
            vec![LocalEvent::Delete {
                path: "/Docs/A.txt".into()
            }]
        );
    }

    #[test]
    fn maps_rename_to_single_path_to_upload() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let file = root.join("Docs/B.txt");
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, b"x").unwrap();
        let event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Name(
                notify::event::RenameMode::To,
            )),
            paths: vec![file],
            attrs: Default::default(),
        };
        let mapped = map_event(root, event);
        assert_eq!(
            mapped,
            vec![LocalEvent::Upload {
                path: "/Docs/B.txt".into()
            }]
        );
    }

    #[test]
    fn maps_close_write_event_to_upload() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let file = root.join("Docs/A.txt");
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, b"x").unwrap();
        let event = Event {
            kind: EventKind::Access(notify::event::AccessKind::Close(
                notify::event::AccessMode::Write,
            )),
            paths: vec![file],
            attrs: Default::default(),
        };
        let mapped = map_event(root, event);
        assert_eq!(
            mapped,
            vec![LocalEvent::Upload {
                path: "/Docs/A.txt".into()
            }]
        );
    }

    #[test]
    fn goutputstream_edit_sequence_ignores_temp_ops() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let target = root.join("test.txt");
        let tmp = root.join(".goutputstream-WTZ4K3");
        std::fs::write(&target, b"old").unwrap();
        std::fs::write(&tmp, b"new").unwrap();

        let events = vec![
            Event {
                kind: EventKind::Create(notify::event::CreateKind::File),
                paths: vec![tmp.clone()],
                attrs: Default::default(),
            },
            Event {
                kind: EventKind::Modify(notify::event::ModifyKind::Data(
                    notify::event::DataChange::Any,
                )),
                paths: vec![target.clone()],
                attrs: Default::default(),
            },
            Event {
                kind: EventKind::Remove(notify::event::RemoveKind::File),
                paths: vec![tmp.clone()],
                attrs: Default::default(),
            },
            Event {
                kind: EventKind::Modify(notify::event::ModifyKind::Name(
                    notify::event::RenameMode::Both,
                )),
                paths: vec![tmp, target],
                attrs: Default::default(),
            },
        ];

        let mapped: Vec<LocalEvent> = events
            .into_iter()
            .flat_map(|event| map_event(root, event))
            .collect();
        assert!(
            mapped
                .iter()
                .all(|event| !matches!(event, LocalEvent::Move { .. } | LocalEvent::Delete { .. }))
        );
        assert!(mapped.iter().all(|event| match event {
            LocalEvent::Upload { path } | LocalEvent::Mkdir { path } => {
                !path.contains(".goutputstream-")
            }
            LocalEvent::Delete { path } => !path.contains(".goutputstream-"),
            LocalEvent::Move { from, to } => {
                !from.contains(".goutputstream-") && !to.contains(".goutputstream-")
            }
        }));
        assert!(mapped.contains(&LocalEvent::Upload {
            path: "/test.txt".into()
        }));
    }

    #[test]
    fn ignores_known_temporary_name_patterns() {
        assert!(is_ignored_temporary_name(".goutputstream-ABC"));
        assert!(is_ignored_temporary_name(".~lock.file.txt#"));
        assert!(is_ignored_temporary_name(".#draft.md"));
        assert!(is_ignored_temporary_name("~$Report.docx"));
        assert!(is_ignored_temporary_name(".nfs000001"));
        assert!(is_ignored_temporary_name("edit.swp"));
        assert!(is_ignored_temporary_name("edit.swo"));
        assert!(is_ignored_temporary_name("edit.swx"));
        assert!(is_ignored_temporary_name("backup.txt~"));
        assert!(!is_ignored_temporary_name(".env"));
        assert!(!is_ignored_temporary_name("test.txt"));
    }

    #[test]
    fn ignores_metadata_only_modify_event() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let file = root.join("Docs/A.txt");
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, b"x").unwrap();
        let event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Metadata(
                notify::event::MetadataKind::Any,
            )),
            paths: vec![file],
            attrs: Default::default(),
        };
        let mapped = map_event(root, event);
        assert!(mapped.is_empty());
    }

    #[test]
    fn ignores_modify_any_event() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        let file = root.join("Docs/A.txt");
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(&file, b"x").unwrap();
        let event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Any),
            paths: vec![file],
            attrs: Default::default(),
        };
        let mapped = map_event(root, event);
        assert!(mapped.is_empty());
    }
}
