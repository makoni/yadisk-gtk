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
        EventKind::Modify(notify::event::ModifyKind::Name(_)) => {
            if event.paths.len() >= 2
                && let (Some(from), Some(to)) = (
                    to_remote_path(root, &event.paths[0]),
                    to_remote_path(root, &event.paths[1]),
                )
            {
                return vec![LocalEvent::Move { from, to }];
            }
            Vec::new()
        }
        EventKind::Create(_) => event
            .paths
            .into_iter()
            .filter_map(|path| map_created_path(root, &path))
            .collect(),
        EventKind::Modify(_) => event
            .paths
            .into_iter()
            .filter_map(|path| map_modified_path(root, &path))
            .collect(),
        EventKind::Remove(_) => event
            .paths
            .into_iter()
            .filter_map(|path| to_remote_path(root, &path))
            .map(|path| LocalEvent::Delete { path })
            .collect(),
        _ => Vec::new(),
    }
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
}
