use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

const DEFAULT_BASE_URL: &str = "https://oauth.yandex.ru";

#[derive(Debug, Error)]
pub enum OAuthError {
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("invalid base url: {0}")]
    Url(#[from] url::ParseError),
    #[error("api returned {status}: {body}")]
    Api { status: StatusCode, body: String },
}

#[derive(Clone)]
pub struct OAuthClient {
    http: Client,
    base_url: Url,
    client_id: String,
    client_secret: String,
}

impl OAuthClient {
    pub fn new(
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> Result<Self, OAuthError> {
        Self::with_base_url(DEFAULT_BASE_URL, client_id, client_secret)
    }

    pub fn with_base_url(
        base_url: &str,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
    ) -> Result<Self, OAuthError> {
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .connect_timeout(std::time::Duration::from_secs(15))
            .build()?;
        Ok(Self {
            http,
            base_url: Url::parse(base_url)?,
            client_id: client_id.into(),
            client_secret: client_secret.into(),
        })
    }

    fn endpoint(&self, path: &str) -> Result<Url, OAuthError> {
        let mut base = self.base_url.clone();
        if !base.path().ends_with('/') {
            base.set_path(&format!("{}/", base.path()));
        }
        Ok(base.join(path)?)
    }

    pub fn authorize_url(
        &self,
        redirect_uri: &str,
        scope: Option<&str>,
        state: Option<&str>,
    ) -> Result<Url, OAuthError> {
        let mut url = self.endpoint("authorize")?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("response_type", "code");
            query.append_pair("client_id", &self.client_id);
            query.append_pair("redirect_uri", redirect_uri);
            if let Some(scope) = scope {
                query.append_pair("scope", scope);
            }
            if let Some(state) = state {
                query.append_pair("state", state);
            }
        }
        Ok(url)
    }

    pub async fn exchange_code(
        &self,
        code: &str,
        redirect_uri: Option<&str>,
    ) -> Result<OAuthToken, OAuthError> {
        let url = self.endpoint("token")?;
        let mut form = vec![
            ("grant_type", "authorization_code"),
            ("code", code),
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
        ];
        if let Some(redirect_uri) = redirect_uri {
            form.push(("redirect_uri", redirect_uri));
        }

        let response = self.http.post(url).form(&form).send().await?;
        if response.status().is_success() {
            Ok(response.json::<OAuthToken>().await?)
        } else {
            let status = response.status();
            let body = match response.text().await {
                Ok(text) => text,
                Err(err) => format!("<failed to read response body: {err}>"),
            };
            Err(OAuthError::Api { status, body })
        }
    }

    pub async fn refresh_token(
        &self,
        refresh_token: &str,
        scope: Option<&str>,
    ) -> Result<OAuthToken, OAuthError> {
        let url = self.endpoint("token")?;
        let mut form = vec![
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
        ];
        if let Some(scope) = scope {
            form.push(("scope", scope));
        }

        let response = self.http.post(url).form(&form).send().await?;
        if response.status().is_success() {
            Ok(response.json::<OAuthToken>().await?)
        } else {
            let status = response.status();
            let body = match response.text().await {
                Ok(text) => text,
                Err(err) => format!("<failed to read response body: {err}>"),
            };
            Err(OAuthError::Api { status, body })
        }
    }
}

#[derive(Deserialize, Serialize)]
pub struct OAuthToken {
    pub access_token: String,
    pub token_type: String,
    #[serde(default)]
    pub expires_in: Option<u64>,
    #[serde(default)]
    pub refresh_token: Option<String>,
    #[serde(default)]
    pub scope: Option<String>,
}

impl std::fmt::Debug for OAuthToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OAuthToken")
            .field("access_token", &"[REDACTED]")
            .field("token_type", &self.token_type)
            .field("expires_in", &self.expires_in)
            .field("refresh_token", &self.refresh_token.as_ref().map(|_| "[REDACTED]"))
            .field("scope", &self.scope)
            .finish()
    }
}
