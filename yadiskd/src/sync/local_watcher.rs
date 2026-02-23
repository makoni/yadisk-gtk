#![allow(dead_code)]

use std::path::{Path, PathBuf};

use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalEvent {
    Upload { path: String },
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
        EventKind::Create(_) | EventKind::Modify(_) => event
            .paths
            .into_iter()
            .filter_map(|path| to_remote_path(root, &path))
            .map(|path| LocalEvent::Upload { path })
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
        let root = Path::new("/tmp/root");
        let event = Event {
            kind: EventKind::Modify(notify::event::ModifyKind::Data(
                notify::event::DataChange::Any,
            )),
            paths: vec![PathBuf::from("/tmp/root/Docs/A.txt")],
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
