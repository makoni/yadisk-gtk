use std::path::{Path, PathBuf};
use std::process::Command;

use serde::Serialize;

const DEFAULT_SYNC_DIR_NAME: &str = "Yandex Disk";
const DEFAULT_REMOTE_ROOT: &str = "disk:/";
const DEFAULT_CLOUD_POLL_SECS: u64 = 15;
const DEFAULT_WORKER_LOOP_MS: u64 = 500;
const DEFAULT_ENABLE_LOCAL_WATCHER: bool = true;

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct SettingsSnapshot {
    pub sync_root: String,
    pub cache_root: String,
    pub remote_root: String,
    pub cloud_poll_secs: u64,
    pub worker_loop_ms: u64,
    pub local_watcher_enabled: bool,
    pub autostart: String,
}

pub fn read_settings_snapshot() -> SettingsSnapshot {
    let home = dirs::home_dir().unwrap_or_else(std::env::temp_dir);
    let sync_root = std::env::var("YADISK_SYNC_DIR")
        .ok()
        .map(|value| expand_with_home(&value, &home))
        .unwrap_or_else(|| home.join(DEFAULT_SYNC_DIR_NAME));
    let cache_root = std::env::var("YADISK_CACHE_DIR")
        .ok()
        .map(|value| expand_with_home(&value, &home))
        .unwrap_or_else(default_cache_root);
    let remote_root =
        std::env::var("YADISK_REMOTE_ROOT").unwrap_or_else(|_| DEFAULT_REMOTE_ROOT.to_string());
    let cloud_poll_secs = read_u64_env("YADISK_CLOUD_POLL_SECS", DEFAULT_CLOUD_POLL_SECS);
    let worker_loop_ms = read_u64_env("YADISK_WORKER_LOOP_MS", DEFAULT_WORKER_LOOP_MS);
    let local_watcher_enabled =
        read_bool_env("YADISK_ENABLE_LOCAL_WATCHER", DEFAULT_ENABLE_LOCAL_WATCHER);
    let autostart = detect_autostart_state();

    SettingsSnapshot {
        sync_root: sync_root.display().to_string(),
        cache_root: cache_root.display().to_string(),
        remote_root,
        cloud_poll_secs,
        worker_loop_ms,
        local_watcher_enabled,
        autostart,
    }
}

fn detect_autostart_state() -> String {
    let output = Command::new("systemctl")
        .args(["--user", "is-enabled", "yadiskd.service"])
        .output();
    let Ok(output) = output else {
        return "unknown".to_string();
    };
    let state = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if state.is_empty() {
        return "unknown".to_string();
    }
    state
}

fn default_cache_root() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(std::env::temp_dir)
        .join("yadisk-gtk")
}

fn expand_with_home(value: &str, home: &Path) -> PathBuf {
    if value == "~" {
        return home.to_path_buf();
    }
    if let Some(rest) = value.strip_prefix("~/") {
        return home.join(rest);
    }
    PathBuf::from(value)
}

fn read_u64_env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn read_bool_env(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_parsers_follow_daemon_rules() {
        assert_eq!(read_u64_env("DOES_NOT_EXIST_123", 10), 10);
        assert!(read_bool_env("DOES_NOT_EXIST_456", true));
        assert!(!read_bool_env("DOES_NOT_EXIST_789", false));
    }
}
