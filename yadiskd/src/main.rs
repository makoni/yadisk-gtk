use yadiskd::daemon::{DaemonConfig, DaemonRuntime};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let config = DaemonConfig::from_env()?;
    let daemon = DaemonRuntime::bootstrap(config).await?;
    daemon.run().await
}
