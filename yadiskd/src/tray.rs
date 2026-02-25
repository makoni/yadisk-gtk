use std::path::PathBuf;

use anyhow::Context;
use tokio::sync::mpsc;
use yadisk_integrations::ids::DBUS_NAME_SYNC;

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
        .map(|value| match value.as_str() {
            "1" | "true" | "TRUE" | "yes" | "on" => true,
            "0" | "false" | "FALSE" | "no" | "off" => false,
            _ => default,
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
        "Yandex Disk".to_string()
    }

    fn icon_name(&self) -> String {
        self.state.icon_name().to_string()
    }

    fn icon_theme_path(&self) -> String {
        self.icon_theme_path.clone()
    }

    fn menu(&self) -> Vec<ksni::menu::MenuItem<Self>> {
        use ksni::menu::StandardItem;
        vec![
            StandardItem {
                label: "Quit".to_string(),
                activate: Box::new(|tray: &mut Self| {
                    let _ = tray.quit_tx.send(());
                }),
                ..Default::default()
            }
            .into(),
        ]
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
}
