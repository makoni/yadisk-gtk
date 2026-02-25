#[cfg(not(feature = "fuse-mount"))]
fn main() {
    eprintln!("yadisk-fuse binary requires --features fuse-mount");
    std::process::exit(1);
}

#[cfg(feature = "fuse-mount")]
mod app {
    use std::collections::HashMap;
    use std::ffi::OsStr;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use fuser::{
        FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyData, ReplyDirectory,
        ReplyEntry, ReplyOpen, Request,
    };
    use libc::{EIO, ENOENT};
    use tokio::runtime::Runtime;
    use yadisk_integrations::ids::{DBUS_INTERFACE_SYNC, DBUS_NAME_SYNC, DBUS_OBJECT_PATH_SYNC};
    use yadiskd::sync::index::{FileState, IndexStore, ItemType};
    use yadiskd::sync::paths::cache_path_for;
    use zbus::blocking::{Connection, Proxy};

    const TTL: Duration = Duration::from_secs(1);

    struct InodeMap {
        next: u64,
        path_to_ino: HashMap<String, u64>,
        ino_to_path: HashMap<u64, String>,
    }

    impl InodeMap {
        fn new() -> Self {
            let mut path_to_ino = HashMap::new();
            let mut ino_to_path = HashMap::new();
            path_to_ino.insert("/".to_string(), 1);
            ino_to_path.insert(1, "/".to_string());
            Self {
                next: 2,
                path_to_ino,
                ino_to_path,
            }
        }

        fn inode_for(&mut self, path: &str) -> u64 {
            if let Some(existing) = self.path_to_ino.get(path) {
                return *existing;
            }
            let ino = self.next;
            self.next += 1;
            self.path_to_ino.insert(path.to_string(), ino);
            self.ino_to_path.insert(ino, path.to_string());
            ino
        }

        fn path_for(&self, ino: u64) -> Option<String> {
            self.ino_to_path.get(&ino).cloned()
        }
    }

    struct DbusDownloader {
        connection: Option<Connection>,
    }

    impl DbusDownloader {
        fn new() -> Self {
            Self {
                connection: Connection::session().ok(),
            }
        }

        fn download(&self, path: &str) -> bool {
            let Some(connection) = &self.connection else {
                return false;
            };
            let Ok(proxy) = Proxy::new(
                connection,
                DBUS_NAME_SYNC,
                DBUS_OBJECT_PATH_SYNC,
                DBUS_INTERFACE_SYNC,
            ) else {
                return false;
            };
            proxy.call_method("Download", &(path)).is_ok()
        }
    }

    struct YadiskFuseFs {
        rt: Runtime,
        index: Arc<IndexStore>,
        cache_root: PathBuf,
        inodes: Mutex<InodeMap>,
        downloader: DbusDownloader,
    }

    impl YadiskFuseFs {
        fn new(index: IndexStore, cache_root: PathBuf) -> anyhow::Result<Self> {
            Ok(Self {
                rt: Runtime::new()?,
                index: Arc::new(index),
                cache_root,
                inodes: Mutex::new(InodeMap::new()),
                downloader: DbusDownloader::new(),
            })
        }

        fn path_from_ino(&self, ino: u64) -> Option<String> {
            self.inodes.lock().ok()?.path_for(ino)
        }

        fn normalize_remote(path: &str) -> String {
            if path == "/" {
                "/".to_string()
            } else {
                let trimmed = path.trim_end_matches('/');
                if trimmed.is_empty() {
                    "/".to_string()
                } else {
                    trimmed.to_string()
                }
            }
        }

        fn child_path(parent: &str, name: &OsStr) -> String {
            let name = name.to_string_lossy();
            if parent == "/" {
                format!("/{}", name)
            } else {
                format!("{}/{}", parent.trim_end_matches('/'), name)
            }
        }

        fn item_attr(&self, path: &str) -> Option<FileAttr> {
            let path = Self::normalize_remote(path);
            if path == "/" {
                return Some(FileAttr {
                    ino: 1,
                    size: 0,
                    blocks: 0,
                    atime: SystemTime::now(),
                    mtime: SystemTime::now(),
                    ctime: SystemTime::now(),
                    crtime: SystemTime::now(),
                    kind: FileType::Directory,
                    perm: 0o755,
                    nlink: 2,
                    uid: unsafe { libc::geteuid() },
                    gid: unsafe { libc::getegid() },
                    rdev: 0,
                    flags: 0,
                    blksize: 512,
                });
            }
            let item = self
                .rt
                .block_on(self.index.get_item_by_path(&path))
                .ok()
                .flatten()?;
            let mut inodes = self.inodes.lock().ok()?;
            let ino = inodes.inode_for(&item.path);
            Some(attr_for_item(ino, &item))
        }

        fn list_children(&self, path: &str) -> Vec<(String, ItemType)> {
            let prefix = Self::normalize_remote(path);
            let Ok(items) = self.rt.block_on(self.index.list_items_by_prefix(&prefix)) else {
                return Vec::new();
            };
            let mut children = HashMap::<String, ItemType>::new();
            for item in items {
                if item.path == prefix {
                    continue;
                }
                let rest = if prefix == "/" {
                    item.path.trim_start_matches('/').to_string()
                } else {
                    item.path
                        .trim_start_matches(prefix.as_str())
                        .trim_start_matches('/')
                        .to_string()
                };
                let first = rest.split('/').next().unwrap_or_default();
                if first.is_empty() {
                    continue;
                }
                let child_path = if prefix == "/" {
                    format!("/{}", first)
                } else {
                    format!("{}/{}", prefix.trim_end_matches('/'), first)
                };
                if let std::collections::hash_map::Entry::Vacant(slot) = children.entry(child_path)
                {
                    let kind = if rest.contains('/') {
                        ItemType::Dir
                    } else {
                        item.item_type
                    };
                    slot.insert(kind);
                }
            }
            let mut out: Vec<_> = children.into_iter().collect();
            out.sort_by(|a, b| a.0.cmp(&b.0));
            out
        }

        fn ensure_downloaded(&self, path: &str) {
            let remote = Self::normalize_remote(path);
            let cache_path = match cache_path_for(&self.cache_root, &remote) {
                Ok(path) => path,
                Err(_) => return,
            };
            let mut state = self.current_state_for_remote_path(&remote);
            if matches!(state, Some(FileState::Cached)) && std::fs::metadata(&cache_path).is_ok() {
                return;
            }
            eprintln!("[yadisk-fuse] on-demand download requested: {remote}");
            let _ = self.downloader.download(&remote);
            for _ in 0..150 {
                state = self.current_state_for_remote_path(&remote);
                let cache_exists = std::fs::metadata(&cache_path).is_ok();
                if matches!(state, Some(FileState::Cached)) && cache_exists {
                    eprintln!("[yadisk-fuse] on-demand download completed: {remote}");
                    return;
                }
                if matches!(state, Some(FileState::Error)) {
                    eprintln!("[yadisk-fuse] on-demand download failed: {remote}");
                    return;
                }
                std::thread::sleep(Duration::from_millis(200));
            }
            eprintln!("[yadisk-fuse] on-demand download timeout: {remote}");
        }

        fn current_state_for_remote_path(&self, remote: &str) -> Option<FileState> {
            let mut candidates = vec![remote.to_string()];
            if let Some(stripped) = remote.strip_prefix("disk:") {
                candidates.push(stripped.to_string());
            } else if remote.starts_with('/') {
                candidates.push(format!("disk:{remote}"));
            }

            for candidate in candidates {
                let state = self
                    .rt
                    .block_on(async {
                        let item = self.index.get_item_by_path(&candidate).await?;
                        if let Some(item) = item {
                            let state = self.index.get_state(item.id).await?;
                            Ok::<Option<FileState>, yadiskd::sync::index::IndexError>(
                                state.map(|s| s.state),
                            )
                        } else {
                            Ok(None)
                        }
                    })
                    .ok()
                    .flatten();
                if state.is_some() {
                    return state;
                }
            }

            None
        }
    }

    impl Filesystem for YadiskFuseFs {
        fn lookup(&mut self, _req: &Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
            let Some(parent_path) = self.path_from_ino(parent) else {
                reply.error(ENOENT);
                return;
            };
            let path = Self::child_path(&parent_path, name);
            if let Some(attr) = self.item_attr(&path) {
                reply.entry(&TTL, &attr, 0);
            } else {
                reply.error(ENOENT);
            }
        }

        fn getattr(&mut self, _req: &Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
            let Some(path) = self.path_from_ino(ino) else {
                reply.error(ENOENT);
                return;
            };
            if let Some(attr) = self.item_attr(&path) {
                reply.attr(&TTL, &attr);
            } else {
                reply.error(ENOENT);
            }
        }

        fn readdir(
            &mut self,
            _req: &Request<'_>,
            ino: u64,
            _fh: u64,
            offset: i64,
            mut reply: ReplyDirectory,
        ) {
            let Some(path) = self.path_from_ino(ino) else {
                reply.error(ENOENT);
                return;
            };
            let mut entries: Vec<(u64, FileType, String)> = Vec::new();
            entries.push((ino, FileType::Directory, ".".to_string()));
            let parent_path = if path == "/" {
                "/".to_string()
            } else {
                Path::new(&path)
                    .parent()
                    .and_then(|p| p.to_str())
                    .filter(|p| !p.is_empty())
                    .unwrap_or("/")
                    .to_string()
            };
            let parent_ino = match self.inodes.lock() {
                Ok(mut inodes) => inodes.inode_for(&parent_path),
                Err(_) => {
                    reply.error(EIO);
                    return;
                }
            };
            entries.push((parent_ino, FileType::Directory, "..".to_string()));
            for (child_path, kind) in self.list_children(&path) {
                let child_ino = match self.inodes.lock() {
                    Ok(mut inodes) => inodes.inode_for(&child_path),
                    Err(_) => {
                        reply.error(EIO);
                        return;
                    }
                };
                entries.push((child_ino, to_fuse_kind(kind), leaf_name(&child_path)));
            }

            for (idx, (entry_ino, entry_type, name)) in
                entries.iter().enumerate().skip(offset as usize)
            {
                let next = (idx + 1) as i64;
                if reply.add(*entry_ino, next, *entry_type, name) {
                    break;
                }
            }
            reply.ok();
        }

        fn open(&mut self, _req: &Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
            if self.path_from_ino(ino).is_none() {
                reply.error(ENOENT);
                return;
            }
            reply.opened(0, 0);
        }

        fn read(
            &mut self,
            _req: &Request<'_>,
            ino: u64,
            _fh: u64,
            offset: i64,
            size: u32,
            _flags: i32,
            _lock_owner: Option<u64>,
            reply: ReplyData,
        ) {
            let Some(path) = self.path_from_ino(ino) else {
                reply.error(ENOENT);
                return;
            };
            self.ensure_downloaded(&path);
            let cache_path = match cache_path_for(&self.cache_root, &path) {
                Ok(path) => path,
                Err(_) => {
                    reply.error(EIO);
                    return;
                }
            };
            let Ok(mut file) = std::fs::File::open(cache_path) else {
                reply.error(ENOENT);
                return;
            };
            use std::io::{Read, Seek, SeekFrom};
            if file.seek(SeekFrom::Start(offset.max(0) as u64)).is_err() {
                reply.error(EIO);
                return;
            }
            let mut buf = vec![0u8; size as usize];
            match file.read(&mut buf) {
                Ok(read) => reply.data(&buf[..read]),
                Err(_) => reply.error(EIO),
            }
        }
    }

    pub fn run() -> anyhow::Result<()> {
        let mountpoint = parse_mountpoint()?;
        let rt = Runtime::new()?;
        let index = rt.block_on(async {
            let idx = IndexStore::new_default().await?;
            idx.init().await?;
            Ok::<IndexStore, yadiskd::sync::index::IndexError>(idx)
        })?;
        let cache_root = std::env::var("YADISK_CACHE_DIR")
            .ok()
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                dirs::cache_dir()
                    .unwrap_or_else(std::env::temp_dir)
                    .join("yadisk-gtk")
            });
        std::fs::create_dir_all(&mountpoint)?;
        let fs = YadiskFuseFs::new(index, cache_root)?;
        let options = vec![
            MountOption::FSName("yadisk-fuse".to_string()),
            MountOption::DefaultPermissions,
        ];
        fuser::mount2(fs, &mountpoint, &options)?;
        Ok(())
    }

    fn parse_mountpoint() -> anyhow::Result<PathBuf> {
        let mut args = std::env::args().skip(1);
        while let Some(arg) = args.next() {
            if arg == "--mount" {
                if let Some(path) = args.next() {
                    return Ok(PathBuf::from(path));
                }
            }
        }
        anyhow::bail!("usage: yadisk-fuse --mount <path>")
    }

    fn to_fuse_kind(kind: ItemType) -> FileType {
        match kind {
            ItemType::Dir => FileType::Directory,
            ItemType::File => FileType::RegularFile,
        }
    }

    fn leaf_name(path: &str) -> String {
        Path::new(path)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or(path)
            .to_string()
    }

    fn attr_for_item(ino: u64, item: &yadiskd::sync::index::ItemRecord) -> FileAttr {
        let kind = to_fuse_kind(item.item_type.clone());
        let size = item.size.unwrap_or(0).max(0) as u64;
        let mtime = item
            .modified
            .map(unix_to_system_time)
            .unwrap_or_else(SystemTime::now);
        FileAttr {
            ino,
            size,
            blocks: size.div_ceil(512),
            atime: mtime,
            mtime,
            ctime: mtime,
            crtime: mtime,
            kind,
            perm: if matches!(kind, FileType::Directory) {
                0o755
            } else {
                0o644
            },
            nlink: if matches!(kind, FileType::Directory) {
                2
            } else {
                1
            },
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getegid() },
            rdev: 0,
            flags: 0,
            blksize: 512,
        }
    }

    fn unix_to_system_time(ts: i64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(ts.max(0) as u64)
    }
}

#[cfg(feature = "fuse-mount")]
fn main() -> anyhow::Result<()> {
    app::run()
}
