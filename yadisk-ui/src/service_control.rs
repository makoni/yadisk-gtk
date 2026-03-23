use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use anyhow::{Context, Result};

const USER_SERVICE_NAME: &str = "yadiskd.service";
const USER_SERVICE_DIR: &str = "systemd/user";
const SERVICE_TEMPLATE_REL: &str = "packaging/systemd/yadiskd.service";
const LOCAL_BIN_NAME: &str = "yadiskd";
const DAEMON_BIN_ENV: &str = "YADISK_DAEMON_BIN";
const APP_CONFIG_DIR: &str = "yadisk-gtk";
const OAUTH_ENV_FILENAME: &str = "oauth.env";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceAction {
    Start,
    Stop,
    Restart,
    EnableAutostart,
    DisableAutostart,
}

impl ServiceAction {
    fn as_systemctl_action(self) -> &'static str {
        match self {
            ServiceAction::Start => "start",
            ServiceAction::Stop => "stop",
            ServiceAction::Restart => "restart",
            ServiceAction::EnableAutostart => "enable",
            ServiceAction::DisableAutostart => "disable",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceStatus {
    pub state: String,
}

impl ServiceStatus {
    pub fn normalized(&self) -> &str {
        self.state.as_str()
    }
}

pub fn query_daemon_service_status() -> Result<ServiceStatus> {
    let output = Command::new("systemctl")
        .args(["--user", "is-active", USER_SERVICE_NAME])
        .output()
        .context("failed to run systemctl --user is-active")?;
    let state = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if state.is_empty() {
        return Ok(ServiceStatus {
            state: "unknown".to_string(),
        });
    }
    Ok(ServiceStatus { state })
}

pub fn run_service_action(action: ServiceAction) -> Result<()> {
    sync_daemon_binary_for_action(action)?;
    sync_user_service_unit_for_action(action)?;
    let output = run_systemctl_action(action)?;
    if output.status.success() {
        return Ok(());
    }

    let details = command_output(&output);
    if is_unit_not_found(&details) {
        bootstrap_user_service()?;
        let retry = run_systemctl_action(action)?;
        if retry.status.success() {
            return Ok(());
        }
        anyhow::bail!(
            "systemctl --user {} {} failed after bootstrap: {}",
            action.as_systemctl_action(),
            USER_SERVICE_NAME,
            command_output(&retry)
        );
    }

    anyhow::bail!(
        "systemctl --user {} {} failed: {}",
        action.as_systemctl_action(),
        USER_SERVICE_NAME,
        details
    );
}

pub fn configure_oauth_credentials(client_id: &str, client_secret: &str) -> Result<()> {
    configure_oauth_credentials_with_redirect(client_id, client_secret, None)
}

pub fn configure_oauth_credentials_with_redirect(
    client_id: &str,
    client_secret: &str,
    redirect_uri: Option<&str>,
) -> Result<()> {
    let client_id = client_id.trim();
    let client_secret = client_secret.trim();
    if client_id.is_empty() || client_secret.is_empty() {
        anyhow::bail!("OAuth client_id and client_secret must not be empty");
    }
    let redirect_uri = redirect_uri
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let env_file = oauth_env_path()?;
    let config_dir = env_file
        .parent()
        .ok_or_else(|| anyhow::anyhow!("failed to resolve oauth env parent directory"))?
        .to_path_buf();
    std::fs::create_dir_all(&config_dir)
        .with_context(|| format!("failed to create {}", config_dir.display()))?;
    let mut payload = format!(
        "YADISK_CLIENT_ID={}\nYADISK_CLIENT_SECRET={}\n",
        quote_env_value(client_id),
        quote_env_value(client_secret)
    );
    if let Some(redirect_uri) = redirect_uri {
        payload.push_str(&format!(
            "YADISK_REDIRECT_URI={}\n",
            quote_env_value(redirect_uri)
        ));
    }
    std::fs::write(&env_file, payload)
        .with_context(|| format!("failed to write {}", env_file.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&env_file, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to chmod {}", env_file.display()))?;
    }
    sync_user_service_unit_for_action(ServiceAction::Start)?;
    if run_service_action(ServiceAction::Restart).is_err() {
        run_service_action(ServiceAction::Start)?;
    }
    Ok(())
}

pub fn oauth_credentials_configured() -> bool {
    let Ok(path) = oauth_env_path() else {
        return false;
    };
    let Ok(content) = std::fs::read_to_string(path) else {
        return false;
    };
    content.contains("YADISK_CLIENT_ID=") && content.contains("YADISK_CLIENT_SECRET=")
}

pub fn auto_import_oauth_credentials() -> Result<bool> {
    if oauth_credentials_configured() {
        return Ok(false);
    }
    if let Some((client_id, client_secret, redirect_uri)) = oauth_from_process_env() {
        configure_oauth_credentials_with_redirect(
            &client_id,
            &client_secret,
            redirect_uri.as_deref(),
        )?;
        return Ok(true);
    }
    for candidate in dotenv_candidate_paths() {
        if let Some((client_id, client_secret, redirect_uri)) =
            read_oauth_from_env_file(&candidate)?
        {
            configure_oauth_credentials_with_redirect(
                &client_id,
                &client_secret,
                redirect_uri.as_deref(),
            )?;
            return Ok(true);
        }
    }
    Ok(false)
}

fn sync_daemon_binary_for_action(action: ServiceAction) -> Result<()> {
    if !matches!(
        action,
        ServiceAction::Start | ServiceAction::Restart | ServiceAction::EnableAutostart
    ) {
        return Ok(());
    }
    if let Ok(path) = std::env::var(DAEMON_BIN_ENV) {
        let candidate = PathBuf::from(path);
        if candidate.is_file() {
            return Ok(());
        }
        anyhow::bail!("{DAEMON_BIN_ENV} points to a missing file");
    }
    let local_bin = home_local_bin_path()?;
    if let Some(candidate) = find_repo_daemon_binary() {
        install_binary(&candidate, &local_bin)?;
        return Ok(());
    }
    if local_bin.is_file() {
        return Ok(());
    }

    let build_status = Command::new("cargo")
        .current_dir(repo_root())
        .args(["build", "-p", "yadiskd"])
        .status()
        .context("failed to run cargo build -p yadiskd")?;
    if !build_status.success() {
        anyhow::bail!("cargo build -p yadiskd failed with status {build_status}");
    }
    if let Some(candidate) = find_repo_daemon_binary() {
        install_binary(&candidate, &local_bin)?;
        return Ok(());
    }
    anyhow::bail!("failed to locate built yadiskd binary")
}

fn sync_user_service_unit_for_action(action: ServiceAction) -> Result<()> {
    if !matches!(
        action,
        ServiceAction::Start | ServiceAction::Restart | ServiceAction::EnableAutostart
    ) {
        return Ok(());
    }
    let binary = service_binary_path()?;
    let unit_dir = dirs::config_dir()
        .map(|base| base.join(USER_SERVICE_DIR))
        .ok_or_else(|| anyhow::anyhow!("failed to resolve user config directory"))?;
    std::fs::create_dir_all(&unit_dir)
        .with_context(|| format!("failed to create {}", unit_dir.display()))?;
    let unit_path = unit_dir.join(USER_SERVICE_NAME);
    let template_path = repo_root().join(SERVICE_TEMPLATE_REL);
    let unit_template = std::fs::read_to_string(&template_path)
        .with_context(|| format!("failed to read {}", template_path.display()))?;
    let unit_content = rewrite_exec_start(&unit_template, &binary);
    let existing = std::fs::read_to_string(&unit_path).ok();
    if existing.as_deref() == Some(unit_content.as_str()) {
        return Ok(());
    }
    std::fs::write(&unit_path, unit_content)
        .with_context(|| format!("failed to write {}", unit_path.display()))?;
    let reload = Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .output()
        .context("failed to run systemctl --user daemon-reload")?;
    if !reload.status.success() {
        anyhow::bail!(
            "systemctl --user daemon-reload failed: {}",
            command_output(&reload)
        );
    }
    Ok(())
}

fn run_systemctl_action(action: ServiceAction) -> Result<Output> {
    Command::new("systemctl")
        .args(["--user", action.as_systemctl_action(), USER_SERVICE_NAME])
        .output()
        .with_context(|| {
            format!(
                "failed to run systemctl --user {} {}",
                action.as_systemctl_action(),
                USER_SERVICE_NAME
            )
        })
}

fn bootstrap_user_service() -> Result<()> {
    let binary = ensure_daemon_binary()?;
    let unit_dir = dirs::config_dir()
        .map(|base| base.join(USER_SERVICE_DIR))
        .ok_or_else(|| anyhow::anyhow!("failed to resolve user config directory"))?;
    std::fs::create_dir_all(&unit_dir)
        .with_context(|| format!("failed to create {}", unit_dir.display()))?;
    let unit_path = unit_dir.join(USER_SERVICE_NAME);

    let template_path = repo_root().join(SERVICE_TEMPLATE_REL);
    let unit_template = std::fs::read_to_string(&template_path)
        .with_context(|| format!("failed to read {}", template_path.display()))?;
    let unit_content = rewrite_exec_start(&unit_template, &binary);
    std::fs::write(&unit_path, unit_content)
        .with_context(|| format!("failed to write {}", unit_path.display()))?;

    let reload = Command::new("systemctl")
        .args(["--user", "daemon-reload"])
        .output()
        .context("failed to run systemctl --user daemon-reload")?;
    if !reload.status.success() {
        anyhow::bail!(
            "systemctl --user daemon-reload failed: {}",
            command_output(&reload)
        );
    }
    Ok(())
}

fn ensure_daemon_binary() -> Result<PathBuf> {
    if let Ok(path) = std::env::var(DAEMON_BIN_ENV) {
        let candidate = PathBuf::from(path);
        if candidate.is_file() {
            return Ok(candidate);
        }
        anyhow::bail!("{DAEMON_BIN_ENV} points to a missing file");
    }

    let local_bin = home_local_bin_path()?;
    if local_bin.is_file() {
        return Ok(local_bin);
    }

    if let Some(candidate) = find_repo_daemon_binary() {
        install_binary(&candidate, &local_bin)?;
        return Ok(local_bin);
    }

    let build_status = Command::new("cargo")
        .current_dir(repo_root())
        .args(["build", "-p", "yadiskd"])
        .status()
        .context("failed to run cargo build -p yadiskd")?;
    if !build_status.success() {
        anyhow::bail!("cargo build -p yadiskd failed with status {build_status}");
    }

    if let Some(candidate) = find_repo_daemon_binary() {
        install_binary(&candidate, &local_bin)?;
        return Ok(local_bin);
    }

    anyhow::bail!("failed to locate built yadiskd binary")
}

fn find_repo_daemon_binary() -> Option<PathBuf> {
    let root = repo_root();
    [
        root.join("target/debug/yadiskd"),
        root.join("target/release/yadiskd"),
    ]
    .into_iter()
    .find(|path| path.is_file())
}

fn install_binary(source: &Path, destination: &Path) -> Result<()> {
    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    let temp_destination = destination.with_extension(format!("{}.tmp", std::process::id()));
    std::fs::copy(source, &temp_destination).with_context(|| {
        format!(
            "failed to copy daemon binary from {} to {}",
            source.display(),
            temp_destination.display()
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&temp_destination, std::fs::Permissions::from_mode(0o755))
            .with_context(|| format!("failed to chmod {}", temp_destination.display()))?;
    }
    std::fs::rename(&temp_destination, destination).with_context(|| {
        format!(
            "failed to atomically replace daemon binary {} with {}",
            destination.display(),
            temp_destination.display()
        )
    })?;
    Ok(())
}

fn rewrite_exec_start(template: &str, binary: &Path) -> String {
    let mut replaced = false;
    let mut lines = Vec::new();
    for line in template.lines() {
        if line.trim_start().starts_with("ExecStart=") {
            lines.push(format!("ExecStart={}", binary.display()));
            replaced = true;
        } else {
            lines.push(line.to_string());
        }
    }
    if !replaced {
        lines.push(format!("ExecStart={}", binary.display()));
    }
    lines.join("\n") + "\n"
}

fn service_binary_path() -> Result<PathBuf> {
    if let Ok(path) = std::env::var(DAEMON_BIN_ENV) {
        let candidate = PathBuf::from(path);
        if candidate.is_file() {
            return Ok(candidate);
        }
        anyhow::bail!("{DAEMON_BIN_ENV} points to a missing file");
    }
    home_local_bin_path()
}

fn oauth_env_path() -> Result<PathBuf> {
    let base = dirs::config_dir()
        .ok_or_else(|| anyhow::anyhow!("failed to resolve user config directory"))?;
    Ok(base.join(APP_CONFIG_DIR).join(OAUTH_ENV_FILENAME))
}

fn oauth_from_process_env() -> Option<(String, String, Option<String>)> {
    let client_id = std::env::var("YADISK_CLIENT_ID").ok()?;
    let client_secret = std::env::var("YADISK_CLIENT_SECRET").ok()?;
    let client_id = client_id.trim().to_string();
    let client_secret = client_secret.trim().to_string();
    if client_id.is_empty() || client_secret.is_empty() {
        return None;
    }
    let redirect_uri = std::env::var("YADISK_REDIRECT_URI")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    Some((client_id, client_secret, redirect_uri))
}

fn dotenv_candidate_paths() -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Ok(cwd) = std::env::current_dir() {
        paths.push(cwd.join(".env"));
    }
    let repo_env = repo_root().join(".env");
    if !paths.contains(&repo_env) {
        paths.push(repo_env);
    }
    paths
}

fn read_oauth_from_env_file(path: &Path) -> Result<Option<(String, String, Option<String>)>> {
    if !path.is_file() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {}", path.display()))?;
    let mut client_id = None::<String>;
    let mut client_secret = None::<String>;
    let mut redirect_uri = None::<String>;
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let trimmed = trimmed.strip_prefix("export ").unwrap_or(trimmed);
        let Some((key, value)) = trimmed.split_once('=') else {
            continue;
        };
        let key = key.trim();
        let value = parse_env_value(value.trim());
        if value.is_empty() {
            continue;
        }
        match key {
            "YADISK_CLIENT_ID" => client_id = Some(value),
            "YADISK_CLIENT_SECRET" => client_secret = Some(value),
            "YADISK_REDIRECT_URI" => redirect_uri = Some(value),
            _ => {}
        }
    }
    match (client_id, client_secret) {
        (Some(client_id), Some(client_secret)) => {
            Ok(Some((client_id, client_secret, redirect_uri)))
        }
        _ => Ok(None),
    }
}

fn parse_env_value(raw: &str) -> String {
    if raw.len() >= 2 && raw.starts_with('"') && raw.ends_with('"') {
        let inner = &raw[1..raw.len() - 1];
        let mut result = String::with_capacity(inner.len());
        let mut chars = inner.chars();
        while let Some(ch) = chars.next() {
            if ch == '\\' {
                match chars.next() {
                    Some('\\') => result.push('\\'),
                    Some('"') => result.push('"'),
                    Some(other) => {
                        result.push('\\');
                        result.push(other);
                    }
                    None => result.push('\\'),
                }
            } else {
                result.push(ch);
            }
        }
        return result;
    }
    if raw.len() >= 2 && raw.starts_with('\'') && raw.ends_with('\'') {
        return raw[1..raw.len() - 1].to_string();
    }
    raw.split('#').next().unwrap_or_default().trim().to_string()
}

fn quote_env_value(value: &str) -> String {
    format!(
        "\"{}\"",
        value
            .replace('\\', "\\\\")
            .replace('"', "\\\"")
            .replace(['\n', '\r'], "")
    )
}

fn home_local_bin_path() -> Result<PathBuf> {
    let home =
        dirs::home_dir().ok_or_else(|| anyhow::anyhow!("failed to resolve home directory"))?;
    Ok(home.join(".local/bin").join(LOCAL_BIN_NAME))
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("workspace root exists")
        .to_path_buf()
}

fn is_unit_not_found(output: &str) -> bool {
    output.contains("Unit yadiskd.service not found")
        || output.contains("unit yadiskd.service not found")
}

fn command_output(output: &Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    match (stdout.is_empty(), stderr.is_empty()) {
        (true, true) => format!("exit status {}", output.status),
        (false, true) => stdout,
        (true, false) => stderr,
        (false, false) => format!("{stderr}; {stdout}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn action_to_systemctl_name_matches_expected_values() {
        assert_eq!(ServiceAction::Start.as_systemctl_action(), "start");
        assert_eq!(ServiceAction::Stop.as_systemctl_action(), "stop");
        assert_eq!(ServiceAction::Restart.as_systemctl_action(), "restart");
        assert_eq!(
            ServiceAction::EnableAutostart.as_systemctl_action(),
            "enable"
        );
        assert_eq!(
            ServiceAction::DisableAutostart.as_systemctl_action(),
            "disable"
        );
    }

    #[test]
    fn rewrite_exec_start_replaces_existing_line() {
        let template = "[Service]\nExecStart=%h/.local/bin/yadiskd\nRestart=on-failure\n";
        let rewritten = rewrite_exec_start(template, Path::new("/tmp/yadiskd"));
        assert!(rewritten.contains("ExecStart=/tmp/yadiskd"));
        assert!(!rewritten.contains("%h/.local/bin/yadiskd"));
    }

    #[test]
    fn quote_env_value_escapes_special_chars() {
        let value = quote_env_value("ab\"c\\d");
        assert_eq!(value, "\"ab\\\"c\\\\d\"");
    }

    #[test]
    fn parse_env_value_handles_quotes_and_comments() {
        assert_eq!(parse_env_value("\"abc\""), "abc");
        assert_eq!(parse_env_value("'xyz'"), "xyz");
        assert_eq!(parse_env_value("abc # comment"), "abc");
    }

    #[test]
    fn parse_env_value_round_trips_with_quote_env_value() {
        let original = r#"ab"c\d"#;
        let quoted = quote_env_value(original);
        let parsed = parse_env_value(&quoted);
        assert_eq!(
            parsed, original,
            "round-trip through quote/parse must preserve value"
        );
    }

    #[test]
    fn reads_oauth_credentials_from_env_file() {
        let path = std::env::temp_dir().join(format!("yadisk-ui-oauth-{}.env", std::process::id()));
        std::fs::write(
            &path,
            "FOO=bar\nYADISK_CLIENT_ID=\"client-id\"\nYADISK_CLIENT_SECRET='secret'\n",
        )
        .unwrap();
        let creds = read_oauth_from_env_file(&path).unwrap();
        assert_eq!(
            creds,
            Some(("client-id".to_string(), "secret".to_string(), None))
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn detects_missing_unit_message() {
        assert!(is_unit_not_found(
            "Failed to start yadiskd.service: Unit yadiskd.service not found."
        ));
    }
}
