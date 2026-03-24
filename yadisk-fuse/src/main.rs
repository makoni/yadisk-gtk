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
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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
    const MAX_INODE_CACHE_ENTRIES: usize = 100_000;

    struct InodeMap {
        next: u64,
        path_to_ino: HashMap<String, u64>,
        ino_to_path: HashMap<u64, String>,
        refcounts: HashMap<u64, u64>,
        last_touched: HashMap<u64, u64>,
        touch_clock: u64,
    }

    impl InodeMap {
        fn new() -> Self {
            let mut path_to_ino = HashMap::new();
            let mut ino_to_path = HashMap::new();
            let mut refcounts = HashMap::new();
            let mut last_touched = HashMap::new();
            path_to_ino.insert("/".to_string(), 1);
            ino_to_path.insert(1, "/".to_string());
            refcounts.insert(1, u64::MAX); // root inode is never forgotten
            last_touched.insert(1, 1);
            Self {
                next: 2,
                path_to_ino,
                ino_to_path,
                refcounts,
                last_touched,
                touch_clock: 1,
            }
        }

        fn touch(&mut self, ino: u64) {
            self.touch_clock = self.touch_clock.saturating_add(1);
            self.last_touched.insert(ino, self.touch_clock);
        }

        fn inode_for(&mut self, path: &str) -> u64 {
            if let Some(existing) = self.path_to_ino.get(path).copied() {
                self.touch(existing);
                return existing;
            }
            let ino = self.next;
            self.next += 1;
            self.path_to_ino.insert(path.to_string(), ino);
            self.ino_to_path.insert(ino, path.to_string());
            self.touch(ino);
            let _ = self.gc_unreferenced(MAX_INODE_CACHE_ENTRIES);
            ino
        }

        fn inc_ref(&mut self, ino: u64) {
            *self.refcounts.entry(ino).or_insert(0) += 1;
            self.touch(ino);
        }

        fn forget(&mut self, ino: u64, nlookup: u64) {
            if ino == 1 {
                return; // never forget root
            }
            let count = self.refcounts.entry(ino).or_insert(0);
            *count = count.saturating_sub(nlookup);
            if *count == 0 {
                self.refcounts.remove(&ino);
                if let Some(path) = self.ino_to_path.remove(&ino) {
                    self.path_to_ino.remove(&path);
                }
                self.last_touched.remove(&ino);
            }
        }

        fn prune_unreferenced(&mut self) -> usize {
            let stale: Vec<(u64, String)> = self
                .ino_to_path
                .iter()
                .filter_map(|(ino, path)| {
                    if *ino == 1 || self.refcounts.get(ino).copied().unwrap_or(0) != 0 {
                        None
                    } else {
                        Some((*ino, path.clone()))
                    }
                })
                .collect();
            for (ino, path) in &stale {
                self.ino_to_path.remove(ino);
                self.path_to_ino.remove(path);
                self.last_touched.remove(ino);
            }
            stale.len()
        }

        fn gc_unreferenced(&mut self, limit: usize) -> usize {
            if self.path_to_ino.len() <= limit {
                return 0;
            }
            let mut stale: Vec<(u64, String, u64)> = self
                .ino_to_path
                .iter()
                .filter_map(|(ino, path)| {
                    if *ino == 1 || self.refcounts.get(ino).copied().unwrap_or(0) != 0 {
                        None
                    } else {
                        Some((
                            *ino,
                            path.clone(),
                            self.last_touched.get(ino).copied().unwrap_or(0),
                        ))
                    }
                })
                .collect();
            stale.sort_by_key(|(_, _, last_touched)| *last_touched);
            let to_remove = self.path_to_ino.len().saturating_sub(limit);
            let mut removed = 0usize;
            for (ino, path, _) in stale.into_iter().take(to_remove) {
                self.ino_to_path.remove(&ino);
                self.path_to_ino.remove(&path);
                self.last_touched.remove(&ino);
                removed += 1;
            }
            removed
        }

        fn path_for(&mut self, ino: u64) -> Option<String> {
            let path = self.ino_to_path.get(&ino).cloned();
            if path.is_some() {
                self.touch(ino);
            }
            path
        }
    }

    fn refresh_after_reconnect(
        inodes: &Arc<Mutex<InodeMap>>,
        state_notify: &Arc<(Mutex<()>, Condvar)>,
    ) {
        let pruned = inodes
            .lock()
            .map(|mut inodes| inodes.prune_unreferenced())
            .unwrap_or(0);
        if pruned != 0 {
            eprintln!("[yadisk-fuse] pruned {pruned} stale inode mappings after D-Bus reconnect");
        }
        state_notify.1.notify_all();
    }

    struct DbusDownloader;

    impl DbusDownloader {
        fn new() -> Self {
            Self
        }

        fn download(&self, path: &str) -> bool {
            let Ok(connection) = Connection::session() else {
                return false;
            };
            let Ok(proxy) = Proxy::new(
                &connection,
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
        inodes: Arc<Mutex<InodeMap>>,
        downloader: DbusDownloader,
        state_notify: Arc<(Mutex<()>, Condvar)>,
    }

    impl YadiskFuseFs {
        fn new(rt: Runtime, index: IndexStore, cache_root: PathBuf) -> Self {
            let state_notify = Arc::new((Mutex::new(()), Condvar::new()));
            let inodes = Arc::new(Mutex::new(InodeMap::new()));

            // Background thread: listen for D-Bus state_changed signals and
            // wake any FUSE threads waiting in ensure_downloaded.
            let notify = Arc::clone(&state_notify);
            let inodes_for_signals = Arc::clone(&inodes);
            std::thread::spawn(move || {
                loop {
                    let Ok(connection) = Connection::session() else {
                        std::thread::sleep(Duration::from_secs(5));
                        continue;
                    };
                    let Ok(proxy) = Proxy::new(
                        &connection,
                        DBUS_NAME_SYNC,
                        DBUS_OBJECT_PATH_SYNC,
                        DBUS_INTERFACE_SYNC,
                    ) else {
                        std::thread::sleep(Duration::from_secs(5));
                        continue;
                    };
                    let Ok(signals) = proxy.receive_signal("state_changed") else {
                        std::thread::sleep(Duration::from_secs(5));
                        continue;
                    };
                    refresh_after_reconnect(&inodes_for_signals, &notify);
                    for _signal in signals {
                        notify.1.notify_all();
                    }
                    // Iterator ended (D-Bus disconnected) — reconnect after a short pause.
                    std::thread::sleep(Duration::from_secs(1));
                }
            });

            Self {
                rt,
                index: Arc::new(index),
                cache_root,
                inodes,
                downloader: DbusDownloader::new(),
                state_notify,
            }
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
            if let Some(item) = self
                .rt
                .block_on(self.index.get_item_by_path(&path))
                .ok()
                .flatten()
            {
                let mut inodes = self.inodes.lock().ok()?;
                let ino = inodes.inode_for(&item.path);
                return Some(attr_for_item(ino, &item));
            }
            // Synthetic directory: readdir may report directories synthesized from
            // nested paths (e.g. /Docs/Sub inferred from /Docs/Sub/file.txt).
            // Check if any children exist under this path.
            let children = self.list_children(&path);
            if !children.is_empty() {
                let mut inodes = self.inodes.lock().ok()?;
                let ino = inodes.inode_for(&path);
                let now = SystemTime::now();
                return Some(FileAttr {
                    ino,
                    size: 0,
                    blocks: 0,
                    atime: now,
                    mtime: now,
                    ctime: now,
                    crtime: now,
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
            None
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
            if !self.downloader.download(&remote) {
                eprintln!(
                    "[yadisk-fuse] on-demand download could not start because Sync1 is unavailable: {remote}"
                );
                return;
            }
            let (lock, cvar) = &*self.state_notify;
            let deadline = Instant::now() + Duration::from_secs(30);
            loop {
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
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                // Wait for a state_changed D-Bus signal or poll every 1s as fallback.
                let guard = lock.lock().unwrap();
                let _ = cvar.wait_timeout(guard, remaining.min(Duration::from_secs(1)));
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
                if let Ok(mut inodes) = self.inodes.lock() {
                    inodes.inc_ref(attr.ino);
                }
                reply.entry(&TTL, &attr, 0);
            } else {
                reply.error(ENOENT);
            }
        }

        fn forget(&mut self, _req: &Request<'_>, ino: u64, nlookup: u64) {
            if let Ok(mut inodes) = self.inodes.lock() {
                inodes.forget(ino, nlookup);
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
                Ok(read) => {
                    if read > 0 {
                        let _ = self
                            .rt
                            .block_on(self.index.touch_accessed_by_path(&path, now_unix()));
                    }
                    reply.data(&buf[..read]);
                }
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
        let fs = YadiskFuseFs::new(rt, index, cache_root);
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
            if arg == "--mount"
                && let Some(path) = args.next()
            {
                return Ok(PathBuf::from(path));
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

    fn now_unix() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs() as i64)
            .unwrap_or(0)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn prune_unreferenced_removes_only_inactive_inodes() {
            let mut inodes = InodeMap::new();
            let active = inodes.inode_for("/Docs/A.txt");
            inodes.inc_ref(active);
            let stale = inodes.inode_for("/Docs/B.txt");

            assert_eq!(inodes.prune_unreferenced(), 1);
            assert_eq!(inodes.path_for(1).as_deref(), Some("/"));
            assert_eq!(inodes.path_for(active).as_deref(), Some("/Docs/A.txt"));
            assert_eq!(inodes.path_for(stale), None);
        }

        #[test]
        fn refresh_after_reconnect_prunes_stale_inode_cache() {
            let inodes = Arc::new(Mutex::new(InodeMap::new()));
            let state_notify = Arc::new((Mutex::new(()), Condvar::new()));

            let (active, stale) = {
                let mut guard = inodes.lock().unwrap();
                let active = guard.inode_for("/Docs/A.txt");
                guard.inc_ref(active);
                let stale = guard.inode_for("/Docs/B.txt");
                (active, stale)
            };

            refresh_after_reconnect(&inodes, &state_notify);

            let mut guard = inodes.lock().unwrap();
            assert_eq!(guard.path_for(1).as_deref(), Some("/"));
            assert_eq!(guard.path_for(active).as_deref(), Some("/Docs/A.txt"));
            assert_eq!(guard.path_for(stale), None);
        }

        #[test]
        fn gc_unreferenced_evicts_oldest_unused_inodes_first() {
            let mut inodes = InodeMap::new();
            let oldest = inodes.inode_for("/Docs/A.txt");
            let _middle = inodes.inode_for("/Docs/B.txt");
            let newest = inodes.inode_for("/Docs/C.txt");

            assert_eq!(inodes.gc_unreferenced(3), 1);
            assert_eq!(inodes.path_for(oldest), None);
            assert_eq!(inodes.path_for(newest).as_deref(), Some("/Docs/C.txt"));
        }

        #[test]
        fn gc_unreferenced_keeps_referenced_inodes() {
            let mut inodes = InodeMap::new();
            let active = inodes.inode_for("/Docs/A.txt");
            inodes.inc_ref(active);
            let oldest_unused = inodes.inode_for("/Docs/B.txt");
            let newest_unused = inodes.inode_for("/Docs/C.txt");

            assert_eq!(inodes.gc_unreferenced(3), 1);
            assert_eq!(inodes.path_for(active).as_deref(), Some("/Docs/A.txt"));
            assert_eq!(inodes.path_for(oldest_unused), None);
            assert_eq!(
                inodes.path_for(newest_unused).as_deref(),
                Some("/Docs/C.txt")
            );
        }
    }
}

#[cfg(feature = "fuse-mount")]
fn main() -> anyhow::Result<()> {
    app::run()
}
