use thiserror::Error;
use yadisk_core::OAuthClient;

use crate::storage::OAuthState;

#[derive(Debug, Error)]
pub enum TokenProviderError {
    #[error("oauth client is required to refresh expired token")]
    MissingOAuthClient,
    #[error("refresh token is missing")]
    MissingRefreshToken,
    #[error("oauth refresh failed: {0}")]
    OAuth(#[from] yadisk_core::OAuthError),
}

pub struct TokenProvider {
    state: OAuthState,
    oauth_client: Option<OAuthClient>,
    refresh_skew_secs: i64,
}

impl TokenProvider {
    pub fn new(state: OAuthState, oauth_client: Option<OAuthClient>) -> Self {
        Self {
            state,
            oauth_client,
            refresh_skew_secs: 60,
        }
    }

    pub async fn valid_access_token(&mut self) -> Result<String, TokenProviderError> {
        if self.should_refresh() {
            self.refresh().await?;
        }
        Ok(self.state.access_token.clone())
    }

    pub fn state(&self) -> &OAuthState {
        &self.state
    }

    pub async fn refresh_now(&mut self) -> Result<String, TokenProviderError> {
        self.refresh().await?;
        Ok(self.state.access_token.clone())
    }

    fn should_refresh(&self) -> bool {
        let Some(expires_at) = self.state.expires_at else {
            return false;
        };
        expires_at <= now_unix().saturating_add(self.refresh_skew_secs)
    }

    async fn refresh(&mut self) -> Result<(), TokenProviderError> {
        let refresh_token = self
            .state
            .refresh_token
            .clone()
            .ok_or(TokenProviderError::MissingRefreshToken)?;
        let client = self
            .oauth_client
            .as_ref()
            .ok_or(TokenProviderError::MissingOAuthClient)?;
        let token = client
            .refresh_token(&refresh_token, self.state.scope.as_deref())
            .await?;
        let mut refreshed = OAuthState::from_oauth_token(&token);
        if refreshed.refresh_token.is_none() {
            refreshed.refresh_token = Some(refresh_token);
        }
        if refreshed.scope.is_none() {
            refreshed.scope = self.state.scope.clone();
        }
        if refreshed.token_type.is_none() {
            refreshed.token_type = self.state.token_type.clone();
        }
        self.state = refreshed;
        Ok(())
    }
}

fn now_unix() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{body_string_contains, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn returns_current_token_when_not_expired() {
        let mut provider = TokenProvider::new(
            OAuthState {
                access_token: "token-1".into(),
                refresh_token: Some("refresh-1".into()),
                expires_at: Some(i64::MAX),
                scope: Some("disk:read".into()),
                token_type: Some("bearer".into()),
            },
            None,
        );

        let token = provider
            .valid_access_token()
            .await
            .expect("token should be valid");
        assert_eq!(token, "token-1");
    }

    #[tokio::test]
    async fn refreshes_token_when_expired() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/token"))
            .and(body_string_contains("grant_type=refresh_token"))
            .and(body_string_contains("refresh_token=refresh-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "access_token": "new-token",
                "token_type": "bearer",
                "expires_in": 3600,
                "refresh_token": "refresh-2",
                "scope": "disk:read"
            })))
            .mount(&server)
            .await;
        let oauth_client = OAuthClient::with_base_url(&server.uri(), "client-id", "secret")
            .expect("oauth client should be built");
        let mut provider = TokenProvider::new(
            OAuthState {
                access_token: "old-token".into(),
                refresh_token: Some("refresh-1".into()),
                expires_at: Some(0),
                scope: Some("disk:read".into()),
                token_type: Some("bearer".into()),
            },
            Some(oauth_client),
        );

        let token = provider
            .valid_access_token()
            .await
            .expect("token should refresh");
        assert_eq!(token, "new-token");
        assert_eq!(provider.state().refresh_token.as_deref(), Some("refresh-2"));
    }

    #[tokio::test]
    async fn returns_error_when_expired_and_no_refresh_token() {
        let mut provider = TokenProvider::new(
            OAuthState {
                access_token: "old-token".into(),
                refresh_token: None,
                expires_at: Some(0),
                scope: None,
                token_type: Some("bearer".into()),
            },
            None,
        );

        let err = provider
            .valid_access_token()
            .await
            .expect_err("expected missing refresh token error");
        assert!(matches!(err, TokenProviderError::MissingRefreshToken));
    }

    #[tokio::test]
    async fn returns_error_when_expired_without_oauth_client() {
        let mut provider = TokenProvider::new(
            OAuthState {
                access_token: "old-token".into(),
                refresh_token: Some("refresh-1".into()),
                expires_at: Some(0),
                scope: None,
                token_type: Some("bearer".into()),
            },
            None,
        );

        let err = provider
            .valid_access_token()
            .await
            .expect_err("expected missing client error");
        assert!(matches!(err, TokenProviderError::MissingOAuthClient));
    }
}
