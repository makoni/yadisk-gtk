use std::path::PathBuf;

use anyhow::Context;
use tokio::sync::mpsc;
use yadisk_integrations::i18n::{product_name, sync_with_saved_language, tr};
use yadisk_integrations::ids::DBUS_NAME_SYNC;

use crate::daemon::resolve_sync_root_from_env;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraySyncState {
    Normal,
    Syncing,
    Error,
}

impl TraySyncState {
    fn icon_name(self) -> &'static str {
        match self {
            Self::Normal => "normal",
            Self::Syncing => "syncing",
            Self::Error => "error",
        }
    }
}

pub struct TrayController {
    tx: std::sync::mpsc::Sender<TraySyncState>,
}

impl TrayController {
    pub fn update(&self, state: TraySyncState) {
        let _ = self.tx.send(state);
    }
}

pub fn start_status_tray(
    quit_tx: mpsc::UnboundedSender<()>,
) -> anyhow::Result<Option<TrayController>> {
    if read_bool_env("YADISK_DISABLE_STATUS_TRAY", false) {
        return Ok(None);
    }

    let icon_dir = status_icon_dir().context("failed to resolve tray icon directory")?;
    if !icon_dir.exists() {
        anyhow::bail!("tray icon directory does not exist: {}", icon_dir.display());
    }
    let icon_theme_path = icon_dir.to_string_lossy().to_string();

    let tray = YadiskTray {
        state: TraySyncState::Normal,
        icon_theme_path,
        quit_tx,
    };
    let service = ksni::TrayService::new(tray);
    let handle = service.handle();
    std::thread::Builder::new()
        .name("yadisk-status-tray".to_string())
        .spawn(move || {
            service.spawn();
        })?;

    let (tx, rx) = std::sync::mpsc::channel::<TraySyncState>();
    std::thread::Builder::new()
        .name("yadisk-status-tray-updates".to_string())
        .spawn(move || {
            while let Ok(state) = rx.recv() {
                handle.update(|tray| {
                    tray.state = state;
                });
            }
        })?;

    Ok(Some(TrayController { tx }))
}

fn status_icon_dir() -> anyhow::Result<PathBuf> {
    if let Ok(path) = std::env::var("YADISK_STATUS_ICON_DIR")
        && !path.trim().is_empty()
    {
        return Ok(PathBuf::from(path));
    }

    if let Ok(exe) = std::env::current_exe()
        && let Some(exe_dir) = exe.parent()
    {
        let candidate = exe_dir
            .join("../share/yadiskd/icons/status")
            .canonicalize()
            .unwrap_or_else(|_| exe_dir.join("../share/yadiskd/icons/status"));
        if candidate.exists() {
            return Ok(candidate);
        }
    }

    Ok(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("assets/status"))
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

struct YadiskTray {
    state: TraySyncState,
    icon_theme_path: String,
    quit_tx: mpsc::UnboundedSender<()>,
}

impl ksni::Tray for YadiskTray {
    fn id(&self) -> String {
        DBUS_NAME_SYNC.to_string()
    }

    fn title(&self) -> String {
        sync_with_saved_language();
        product_name().to_string()
    }

    fn icon_name(&self) -> String {
        self.state.icon_name().to_string()
    }

    fn icon_theme_path(&self) -> String {
        self.icon_theme_path.clone()
    }

    fn menu(&self) -> Vec<ksni::menu::MenuItem<Self>> {
        use ksni::menu::StandardItem;
        sync_with_saved_language();
        vec![
            StandardItem {
                label: tr("Open Yandex Disk"),
                activate: Box::new(|_tray: &mut Self| {
                    launch_ui_from_tray();
                }),
                ..Default::default()
            }
            .into(),
            StandardItem {
                label: tr("Open Yandex Disk Folder"),
                activate: Box::new(|_tray: &mut Self| {
                    open_sync_root_from_tray();
                }),
                ..Default::default()
            }
            .into(),
            StandardItem {
                label: tr("Quit"),
                activate: Box::new(|tray: &mut Self| {
                    let _ = tray.quit_tx.send(());
                }),
                ..Default::default()
            }
            .into(),
        ]
    }
}

fn launch_ui_from_tray() {
    let mut last_error: Option<String> = None;
    for candidate in ui_launch_candidates() {
        match std::process::Command::new(&candidate).spawn() {
            Ok(_) => return,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                last_error = Some(format!("launch target not found: {}", candidate.display()));
            }
            Err(err) => {
                eprintln!(
                    "[yadiskd] warning: failed to launch UI from tray via {}: {err}",
                    candidate.display()
                );
                return;
            }
        }
    }

    if let Some(err) = last_error {
        eprintln!("[yadiskd] warning: failed to launch UI from tray: {err}");
    }
}

fn ui_launch_candidates() -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    let mut push_candidate = |candidate: PathBuf| {
        if !candidate.as_os_str().is_empty()
            && !candidates.iter().any(|existing| existing == &candidate)
        {
            candidates.push(candidate);
        }
    };

    if let Ok(path) = std::env::var("YADISK_UI_BIN")
        && !path.trim().is_empty()
    {
        push_candidate(PathBuf::from(path));
    }
    if let Ok(exe) = std::env::current_exe()
        && let Some(exe_dir) = exe.parent()
    {
        push_candidate(exe_dir.join("yadisk-ui"));
    }
    push_candidate(PathBuf::from("yadisk-ui"));
    candidates
}

fn open_sync_root_from_tray() {
    let path = match resolve_sync_root_from_env() {
        Ok(path) => path,
        Err(err) => {
            eprintln!("[yadiskd] warning: failed to resolve sync root for tray: {err}");
            return;
        }
    };
    if !path.exists() {
        eprintln!(
            "[yadiskd] warning: tray sync root does not exist: {}",
            path.display()
        );
        return;
    }
    if let Err(err) = std::process::Command::new("xdg-open").arg(&path).spawn() {
        eprintln!(
            "[yadiskd] warning: failed to open sync root from tray ({}): {err}",
            path.display()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_state_to_icon_name() {
        assert_eq!(TraySyncState::Normal.icon_name(), "normal");
        assert_eq!(TraySyncState::Syncing.icon_name(), "syncing");
        assert_eq!(TraySyncState::Error.icon_name(), "error");
    }

    #[test]
    fn ui_launch_candidates_include_path_fallback() {
        assert!(
            ui_launch_candidates()
                .iter()
                .any(|candidate| candidate == &PathBuf::from("yadisk-ui"))
        );
    }
}
