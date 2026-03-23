use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use yadisk_integrations::i18n::tr;

const NAUTILUS_LIB_NAME: &str = "libyadisk_nautilus.so";
const FUSE_BIN_NAME: &str = "yadisk-fuse";
const ICON_NAME: &str = "cloud-outline-thin-symbolic.svg";
const ICON_NAMES: [&str; 3] = [
    "check-round-outline-symbolic.svg",
    "cloud-outline-thin-symbolic.svg",
    "update-symbolic.svg",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IntegrationStatus {
    pub nautilus_extension_installed: bool,
    pub fuse_helper_installed: bool,
    pub emblems_installed: bool,
}

impl IntegrationStatus {
    pub fn summary_state(&self) -> &'static str {
        if self.nautilus_extension_installed && self.fuse_helper_installed && self.emblems_installed
        {
            "ok"
        } else {
            "needs_setup"
        }
    }

    pub fn summary_message(&self) -> String {
        let mut missing = Vec::new();
        if !self.nautilus_extension_installed {
            missing.push(component_label("nautilus_extension"));
        }
        if !self.fuse_helper_installed {
            missing.push(component_label("fuse_helper"));
        }
        if !self.emblems_installed {
            missing.push(component_label("emblems"));
        }
        if missing.is_empty() {
            return tr("All integration components are present");
        }
        format!("{}: {}", tr("Missing components"), missing.join(", "))
    }
}

pub fn detect_integration_status() -> IntegrationStatus {
    let nautilus_extension_installed = nautilus_candidate_paths()
        .into_iter()
        .map(|base| base.join(NAUTILUS_LIB_NAME))
        .any(|path| path.is_file());
    let fuse_helper_installed = std::env::var_os("HOME")
        .map(PathBuf::from)
        .map(|home| home.join(".local/bin").join(FUSE_BIN_NAME))
        .is_some_and(|path| path.is_file());
    let emblems_installed = std::env::var_os("HOME")
        .map(PathBuf::from)
        .map(|home| {
            home.join(".local/share/icons/hicolor/scalable/emblems")
                .join(ICON_NAME)
        })
        .is_some_and(|path| path.is_file());
    IntegrationStatus {
        nautilus_extension_installed,
        fuse_helper_installed,
        emblems_installed,
    }
}

pub fn guided_install_instructions() -> Vec<String> {
    let commands = guided_install_commands();
    vec![
        "Guided integration setup:".to_string(),
        format!("1) Install/update Nautilus extension: {}", commands[0]),
        format!("2) Install/update FUSE helper: {}", commands[1]),
        format!("3) Restart Files: {}", commands[2]),
        format!("4) Re-check status: {}", commands[3]),
    ]
}

pub fn guided_install_commands() -> Vec<String> {
    let root = repo_root();
    vec![
        format!(
            "bash {}/packaging/host/install-nautilus-extension.sh",
            root.display()
        ),
        format!(
            "bash {}/packaging/host/install-yadisk-fuse.sh",
            root.display()
        ),
        "nautilus -q".to_string(),
        "yadisk-ui --check-integrations".to_string(),
    ]
}

pub fn run_auto_install() -> Result<()> {
    let root = repo_root();
    run_script(root.join("packaging/host/install-nautilus-extension.sh"))?;
    run_script(root.join("packaging/host/install-yadisk-fuse.sh"))?;
    Ok(())
}

pub fn ensure_auto_install_permissions() -> Result<()> {
    let ext_dir = install_nautilus_extension_dir();
    if is_dir_writable(&ext_dir) {
        return Ok(());
    }
    if command_exists("pkexec") || command_exists("sudo") {
        return Ok(());
    }
    anyhow::bail!(
        "not enough permissions for {} and no pkexec/sudo available",
        ext_dir.display()
    );
}

pub fn run_auto_uninstall() -> Result<()> {
    for base in nautilus_candidate_paths() {
        remove_if_exists(base.join(NAUTILUS_LIB_NAME))?;
    }
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        remove_if_exists(home.join(".local/bin").join(FUSE_BIN_NAME))?;
        let emblem_dir = home.join(".local/share/icons/hicolor/scalable/emblems");
        for icon in ICON_NAMES {
            remove_if_exists(emblem_dir.join(icon))?;
        }
    }
    Ok(())
}

fn run_script(path: PathBuf) -> Result<()> {
    let status = Command::new("bash")
        .arg(&path)
        .status()
        .with_context(|| format!("failed to run {}", path.display()))?;
    if status.success() {
        return Ok(());
    }
    anyhow::bail!("script failed: {} (status {status})", path.display());
}

fn remove_if_exists(path: PathBuf) -> Result<()> {
    if path.is_file() {
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                if !remove_with_privileges(&path)? {
                    return Err(err)
                        .with_context(|| format!("failed to remove {}", path.display()));
                }
            }
            Err(err) => {
                return Err(err).with_context(|| format!("failed to remove {}", path.display()));
            }
        }
    }
    Ok(())
}

fn remove_with_privileges(path: &Path) -> Result<bool> {
    if command_exists("pkexec")
        && (std::env::var_os("DISPLAY").is_some() || std::env::var_os("WAYLAND_DISPLAY").is_some())
    {
        let status = Command::new("pkexec")
            .arg("rm")
            .arg("-f")
            .arg(path)
            .status()
            .with_context(|| format!("failed to run pkexec rm for {}", path.display()))?;
        if status.success() {
            return Ok(true);
        }
    }
    if command_exists("sudo")
        && let Ok(status) = Command::new("sudo")
            .arg("-n")
            .arg("rm")
            .arg("-f")
            .arg(path)
            .status()
        && status.success()
    {
        return Ok(true);
    }
    Ok(false)
}

fn command_exists(name: &str) -> bool {
    std::env::var_os("PATH")
        .is_some_and(|paths| std::env::split_paths(&paths).any(|path| path.join(name).is_file()))
}

fn component_label(name: &str) -> String {
    match name {
        "nautilus_extension" => tr("Nautilus extension"),
        "fuse_helper" => tr("FUSE helper"),
        "emblems" => tr("Emblems"),
        _ => name.to_string(),
    }
}

fn install_nautilus_extension_dir() -> PathBuf {
    if let Some(path) = std::env::var_os("YADISK_NAUTILUS_EXT_DIR") {
        return PathBuf::from(path);
    }
    if let Some(path) = pkg_config_extension_dir() {
        return path;
    }
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .map(|home| home.join(".local/lib/nautilus/extensions-4"))
        .unwrap_or_else(|| PathBuf::from(".local/lib/nautilus/extensions-4"))
}

fn pkg_config_extension_dir() -> Option<PathBuf> {
    let output = Command::new("pkg-config")
        .arg("--variable=extensiondir")
        .arg("libnautilus-extension-4")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let value = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if value.is_empty() {
        return None;
    }
    Some(PathBuf::from(value))
}

fn is_dir_writable(path: &Path) -> bool {
    if path.exists() && path.is_dir() {
        return can_create_probe_file(path);
    }
    path.parent().is_some_and(can_create_probe_file)
}

fn can_create_probe_file(dir: &Path) -> bool {
    let probe = dir.join(format!(".yadisk-write-check-{}", std::process::id()));
    match std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&probe)
    {
        Ok(_) => {
            let _ = std::fs::remove_file(probe);
            true
        }
        Err(_) => false,
    }
}

fn nautilus_candidate_paths() -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(path) = std::env::var_os("YADISK_NAUTILUS_EXT_DIR") {
        paths.push(PathBuf::from(path));
    }
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        paths.push(home.join(".local/lib/nautilus/extensions-4"));
    }
    paths.push(PathBuf::from("/usr/lib/nautilus/extensions-4"));
    paths.push(PathBuf::from(
        "/usr/lib/x86_64-linux-gnu/nautilus/extensions-4",
    ));
    paths
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root exists")
        .to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_summary_reports_missing_components() {
        let status = IntegrationStatus {
            nautilus_extension_installed: false,
            fuse_helper_installed: true,
            emblems_installed: false,
        };
        assert_eq!(status.summary_state(), "needs_setup");
        assert!(status.summary_message().contains("Nautilus extension"));
        assert!(status.summary_message().contains("Emblems"));
    }

    #[test]
    fn status_summary_reports_complete_state() {
        let status = IntegrationStatus {
            nautilus_extension_installed: true,
            fuse_helper_installed: true,
            emblems_installed: true,
        };
        assert_eq!(status.summary_state(), "ok");
    }
}
