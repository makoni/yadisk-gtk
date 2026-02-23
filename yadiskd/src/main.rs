use anyhow::Context;
use yadisk_core::{ApiErrorClass, DiskInfo, OAuthClient, YadiskClient};
use yadiskd::oauth_flow::OAuthFlow;
use yadiskd::storage::{OAuthState, TokenStorage};
use yadiskd::token_provider::TokenProvider;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let info = match std::env::var("YADISK_TOKEN") {
        Ok(token) => {
            let client = YadiskClient::new(token)?;
            client.get_disk_info().await?
        }
        Err(_) => {
            let storage = TokenStorage::new()
                .await
                .context("failed to initialize token storage")?;
            let state = match storage.get_oauth_state() {
                Ok(state) => state,
                Err(_) => authenticate_and_store(&storage).await?,
            };
            let oauth_client = oauth_client_from_env()?;

            let mut provider = TokenProvider::new(state, oauth_client);
            let info = fetch_disk_info_with_retry(&mut provider, None)
                .await
                .context("failed to fetch disk info")?;
            storage
                .save_oauth_state(provider.state())
                .context("failed to persist oauth state")?;
            info
        }
    };

    println!("{}", serde_json::to_string_pretty(&info)?);
    Ok(())
}

async fn authenticate_and_store(storage: &TokenStorage) -> anyhow::Result<OAuthState> {
    let client_id = std::env::var("YADISK_CLIENT_ID").context("YADISK_CLIENT_ID is not set")?;
    let client_secret =
        std::env::var("YADISK_CLIENT_SECRET").context("YADISK_CLIENT_SECRET is not set")?;
    let flow = OAuthFlow::new(client_id, client_secret);
    let token = flow.authenticate().await?;
    let state = OAuthState::from_oauth_token(&token);
    storage
        .save_oauth_state(&state)
        .context("failed to save token")?;
    Ok(state)
}

fn oauth_client_from_env() -> anyhow::Result<Option<OAuthClient>> {
    match (
        std::env::var("YADISK_CLIENT_ID"),
        std::env::var("YADISK_CLIENT_SECRET"),
    ) {
        (Ok(client_id), Ok(client_secret)) => Ok(Some(
            OAuthClient::new(client_id, client_secret).context("invalid oauth config")?,
        )),
        _ => Ok(None),
    }
}

async fn fetch_disk_info_with_retry(
    provider: &mut TokenProvider,
    base_url: Option<&str>,
) -> anyhow::Result<DiskInfo> {
    let token = provider
        .valid_access_token()
        .await
        .context("failed to resolve valid access token")?;
    let client = build_client(base_url, &token)?;
    match client.get_disk_info().await {
        Ok(info) => Ok(info),
        Err(err) if matches!(err.classification(), Some(ApiErrorClass::Auth)) => {
            let refreshed = provider
                .refresh_now()
                .await
                .context("failed to refresh token after 401")?;
            let retry_client = build_client(base_url, &refreshed)?;
            Ok(retry_client.get_disk_info().await?)
        }
        Err(err) => Err(err.into()),
    }
}

fn build_client(
    base_url: Option<&str>,
    token: &str,
) -> Result<YadiskClient, yadisk_core::YadiskError> {
    match base_url {
        Some(url) => YadiskClient::with_base_url(url, token.to_string()),
        None => YadiskClient::new(token.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_string_contains, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn retries_once_after_unauthorized_with_refreshed_token() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/v1/disk"))
            .and(header("authorization", "OAuth old-token"))
            .respond_with(ResponseTemplate::new(401).set_body_string("unauthorized"))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .and(body_string_contains("grant_type=refresh_token"))
            .and(body_string_contains("refresh_token=refresh-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": "new-token",
                "token_type": "bearer",
                "expires_in": 3600,
                "refresh_token": "refresh-2",
                "scope": "disk:read"
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/v1/disk"))
            .and(header("authorization", "OAuth new-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "total_space": 1000,
                "used_space": 100,
                "trash_size": 0,
                "is_paid": false
            })))
            .mount(&server)
            .await;

        let oauth_client =
            OAuthClient::with_base_url(&server.uri(), "client-id", "secret").expect("oauth client");
        let mut provider = TokenProvider::new(
            OAuthState {
                access_token: "old-token".into(),
                refresh_token: Some("refresh-1".into()),
                expires_at: Some(i64::MAX),
                scope: Some("disk:read".into()),
                token_type: Some("bearer".into()),
            },
            Some(oauth_client),
        );

        let info = fetch_disk_info_with_retry(&mut provider, Some(&server.uri()))
            .await
            .expect("retry should succeed");
        assert_eq!(info.total_space, 1000);
        assert_eq!(provider.state().access_token, "new-token");
    }
}
