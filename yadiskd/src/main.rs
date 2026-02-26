use yadiskd::daemon::{DaemonConfig, DaemonRuntime};
use yadiskd::storage::TokenStorage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CliMode {
    Run,
    Logout,
    Help,
}

fn parse_cli_mode<I>(args: I) -> anyhow::Result<CliMode>
where
    I: IntoIterator<Item = String>,
{
    let mut mode = CliMode::Run;
    for arg in args.into_iter().skip(1) {
        match arg.as_str() {
            "--logout" => mode = CliMode::Logout,
            "--help" | "-h" => mode = CliMode::Help,
            other => anyhow::bail!("unknown argument: {other}"),
        }
    }
    Ok(mode)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    match parse_cli_mode(std::env::args())? {
        CliMode::Logout => {
            let storage = TokenStorage::new().await?;
            storage.delete_token()?;
            eprintln!("[yadiskd] saved token removed");
            return Ok(());
        }
        CliMode::Help => {
            println!("Usage: yadiskd [--logout]");
            println!("  --logout   Remove saved OAuth token and exit");
            return Ok(());
        }
        CliMode::Run => {}
    }
    let config = DaemonConfig::from_env()?;
    let daemon = DaemonRuntime::bootstrap(config).await?;
    daemon.run().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cli_mode_defaults_to_run() {
        let mode = parse_cli_mode(vec!["yadiskd".to_string()]).unwrap();
        assert_eq!(mode, CliMode::Run);
    }

    #[test]
    fn parse_cli_mode_supports_logout() {
        let mode = parse_cli_mode(vec!["yadiskd".to_string(), "--logout".to_string()]).unwrap();
        assert_eq!(mode, CliMode::Logout);
    }

    #[test]
    fn parse_cli_mode_supports_help() {
        let mode = parse_cli_mode(vec!["yadiskd".to_string(), "--help".to_string()]).unwrap();
        assert_eq!(mode, CliMode::Help);
    }
}
