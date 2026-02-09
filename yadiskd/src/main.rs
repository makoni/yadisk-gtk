mod oauth_flow;
mod storage;
mod sync;

use anyhow::Context;
use oauth_flow::OAuthFlow;
use storage::TokenStorage;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let token = match std::env::var("YADISK_TOKEN") {
        Ok(token) => token,
        Err(_) => {
            let storage = TokenStorage::new()
                .await
                .context("failed to initialize token storage")?;
            match storage.get_token() {
                Ok(token) => token,
                Err(_) => {
                    let client_id =
                        std::env::var("YADISK_CLIENT_ID").context("YADISK_CLIENT_ID is not set")?;
                    let client_secret = std::env::var("YADISK_CLIENT_SECRET")
                        .context("YADISK_CLIENT_SECRET is not set")?;
                    let flow = OAuthFlow::new(client_id, client_secret);
                    let token = flow.authenticate().await?;
                    storage
                        .save_token(&token.access_token)
                        .context("failed to save token")?;
                    token.access_token
                }
            }
        }
    };

    let client = yadisk_core::YadiskClient::new(token)?;
    let info = client.get_disk_info().await?;

    println!("{}", serde_json::to_string_pretty(&info)?);
    Ok(())
}
