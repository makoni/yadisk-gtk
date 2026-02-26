use std::path::PathBuf;
use std::process::Command;

use anyhow::{Context, Result};

const NAUTILUS_LIB_NAME: &str = "libyadisk_nautilus.so";
const FUSE_BIN_NAME: &str = "yadisk-fuse";
const ICON_NAME: &str = "cloud-outline-thin-symbolic.svg";

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
            missing.push("nautilus_extension");
        }
        if !self.fuse_helper_installed {
            missing.push("fuse_helper");
        }
        if !self.emblems_installed {
            missing.push("emblems");
        }
        if missing.is_empty() {
            return "all integration components are present".to_string();
        }
        format!("missing components: {}", missing.join(", "))
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
    let root = repo_root();
    vec![
        "Guided integration setup:".to_string(),
        format!(
            "1) Install/update Nautilus extension: bash {}/packaging/host/install-nautilus-extension.sh",
            root.display()
        ),
        format!(
            "2) Install/update FUSE helper: bash {}/packaging/host/install-yadisk-fuse.sh",
            root.display()
        ),
        "3) Restart Files: nautilus -q".to_string(),
        "4) Re-check status: yadisk-ui --check-integrations".to_string(),
    ]
}

pub fn run_auto_install() -> Result<()> {
    let root = repo_root();
    run_script(root.join("packaging/host/install-nautilus-extension.sh"))?;
    run_script(root.join("packaging/host/install-yadisk-fuse.sh"))?;
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
        assert!(status.summary_message().contains("nautilus_extension"));
        assert!(status.summary_message().contains("emblems"));
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
