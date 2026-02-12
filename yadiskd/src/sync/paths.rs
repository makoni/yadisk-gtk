use std::path::{Component, Path, PathBuf};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PathError {
    #[error("remote path is empty")]
    Empty,
    #[error("remote path contains unsupported component")]
    UnsupportedComponent,
}

pub fn cache_path_for(cache_root: &Path, remote_path: &str) -> Result<PathBuf, PathError> {
    if remote_path.is_empty() {
        return Err(PathError::Empty);
    }

    // Remote paths are POSIX-like ("/Docs/A.txt"); map them under cache_root.
    let mut out = cache_root.to_path_buf();
    for component in Path::new(remote_path).components() {
        match component {
            Component::Normal(part) => out.push(part),
            Component::RootDir => continue,
            Component::CurDir => continue,
            Component::ParentDir | Component::Prefix(_) => {
                return Err(PathError::UnsupportedComponent);
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_remote_path_under_cache_root() {
        let root = PathBuf::from("/cache");
        let mapped = cache_path_for(&root, "/Docs/A.txt").unwrap();
        assert_eq!(mapped, PathBuf::from("/cache/Docs/A.txt"));
    }

    #[test]
    fn rejects_parent_dir() {
        let root = PathBuf::from("/cache");
        assert!(matches!(
            cache_path_for(&root, "../secret"),
            Err(PathError::UnsupportedComponent)
        ));
    }
}
