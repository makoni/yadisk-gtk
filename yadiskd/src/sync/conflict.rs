#![allow(dead_code)]

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMetadata {
    pub modified: i64,
    pub hash: Option<String>,
}

impl FileMetadata {
    pub fn is_same_as(&self, other: &Self) -> bool {
        match (&self.hash, &other.hash) {
            (Some(left), Some(right)) => left == right,
            _ => self.modified == other.modified,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictDecision {
    NoOp,
    UploadLocal,
    DownloadRemote,
    KeepBoth { renamed_local: String },
}

pub fn resolve_conflict(
    path: &str,
    base: Option<&FileMetadata>,
    local: &FileMetadata,
    remote: &FileMetadata,
) -> ConflictDecision {
    if let Some(base) = base {
        let local_changed = !local.is_same_as(base);
        let remote_changed = !remote.is_same_as(base);
        return match (local_changed, remote_changed) {
            (false, false) => ConflictDecision::NoOp,
            (true, false) => ConflictDecision::UploadLocal,
            (false, true) => ConflictDecision::DownloadRemote,
            (true, true) => ConflictDecision::KeepBoth {
                renamed_local: conflict_path(path, local.modified),
            },
        };
    }

    if local.is_same_as(remote) {
        ConflictDecision::NoOp
    } else {
        ConflictDecision::KeepBoth {
            renamed_local: conflict_path(path, local.modified),
        }
    }
}

fn conflict_path(path: &str, stamp: i64) -> String {
    use rand::Rng;
    let suffix: u16 = rand::thread_rng().r#gen();
    let (dir, name) = match path.rsplit_once('/') {
        Some((dir, name)) => (format!("{dir}/"), name),
        None => (String::new(), path),
    };

    if let Some((stem, ext)) = name.rsplit_once('.')
        && !stem.is_empty()
    {
        return format!("{dir}{stem} (conflict {stamp}-{suffix:04x}).{ext}");
    }

    format!("{dir}{name} (conflict {stamp}-{suffix:04x})")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(hash: &str, modified: i64) -> FileMetadata {
        FileMetadata {
            modified,
            hash: Some(hash.to_string()),
        }
    }

    #[test]
    fn no_op_when_unchanged() {
        let base = meta("a", 1);
        let local = meta("a", 1);
        let remote = meta("a", 1);
        assert_eq!(
            resolve_conflict("/Docs/A.txt", Some(&base), &local, &remote),
            ConflictDecision::NoOp
        );
    }

    #[test]
    fn upload_when_only_local_changed() {
        let base = meta("a", 1);
        let local = meta("b", 2);
        let remote = meta("a", 1);
        assert_eq!(
            resolve_conflict("/Docs/A.txt", Some(&base), &local, &remote),
            ConflictDecision::UploadLocal
        );
    }

    #[test]
    fn download_when_only_remote_changed() {
        let base = meta("a", 1);
        let local = meta("a", 1);
        let remote = meta("c", 3);
        assert_eq!(
            resolve_conflict("/Docs/A.txt", Some(&base), &local, &remote),
            ConflictDecision::DownloadRemote
        );
    }

    #[test]
    fn keep_both_when_both_changed() {
        let base = meta("a", 1);
        let local = meta("b", 2);
        let remote = meta("c", 3);
        let decision = resolve_conflict("/Docs/A.txt", Some(&base), &local, &remote);
        match &decision {
            ConflictDecision::KeepBoth { renamed_local } => {
                assert!(
                    renamed_local.starts_with("/Docs/A (conflict 2-"),
                    "unexpected renamed path: {renamed_local}"
                );
                assert!(renamed_local.ends_with(").txt"));
            }
            other => panic!("expected KeepBoth, got {other:?}"),
        }
    }

    #[test]
    fn keep_both_without_base() {
        let local = meta("b", 2);
        let remote = meta("c", 3);
        let decision = resolve_conflict("/Docs/A.txt", None, &local, &remote);
        match &decision {
            ConflictDecision::KeepBoth { renamed_local } => {
                assert!(
                    renamed_local.starts_with("/Docs/A (conflict 2-"),
                    "unexpected renamed path: {renamed_local}"
                );
                assert!(renamed_local.ends_with(").txt"));
            }
            other => panic!("expected KeepBoth, got {other:?}"),
        }
    }

    #[test]
    fn conflict_paths_are_unique_for_same_timestamp() {
        let path1 = conflict_path("/Docs/A.txt", 100);
        let path2 = conflict_path("/Docs/A.txt", 100);
        assert_ne!(
            path1, path2,
            "two conflict paths with same timestamp must differ"
        );
        assert!(path1.starts_with("/Docs/A (conflict 100-"));
        assert!(path2.starts_with("/Docs/A (conflict 100-"));
    }

    #[test]
    fn conflict_path_without_extension() {
        let path = conflict_path("/Docs/README", 42);
        assert!(path.starts_with("/Docs/README (conflict 42-"));
        assert!(!path.contains('.'));
    }
}
