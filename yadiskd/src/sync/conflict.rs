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
    let (dir, name) = match path.rsplit_once('/') {
        Some((dir, name)) => (format!("{dir}/"), name),
        None => (String::new(), path),
    };

    if let Some((stem, ext)) = name.rsplit_once('.')
        && !stem.is_empty()
    {
        return format!("{dir}{stem} (conflict {stamp}).{ext}");
    }

    format!("{dir}{name} (conflict {stamp})")
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
        assert_eq!(
            decision,
            ConflictDecision::KeepBoth {
                renamed_local: "/Docs/A (conflict 2).txt".to_string()
            }
        );
    }

    #[test]
    fn keep_both_without_base() {
        let local = meta("b", 2);
        let remote = meta("c", 3);
        let decision = resolve_conflict("/Docs/A.txt", None, &local, &remote);
        assert_eq!(
            decision,
            ConflictDecision::KeepBoth {
                renamed_local: "/Docs/A (conflict 2).txt".to_string()
            }
        );
    }
}
