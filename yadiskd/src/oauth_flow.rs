use std::io::Write;
use thiserror::Error;
use yadisk_core::{OAuthClient, OAuthToken};

#[derive(Debug, Error)]
pub enum OAuthFlowError {
    #[error("oauth error: {0}")]
    OAuth(#[from] yadisk_core::OAuthError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("portal open-uri failed: {0}")]
    Portal(#[from] ashpd::Error),
    #[error("authorization code missing in redirect")]
    MissingCode,
}

pub struct OAuthFlow {
    client_id: String,
    client_secret: String,
}

impl OAuthFlow {
    pub fn new(client_id: impl Into<String>, client_secret: impl Into<String>) -> Self {
        Self {
            client_id: client_id.into(),
            client_secret: client_secret.into(),
        }
    }

    pub async fn authenticate(&self) -> Result<OAuthToken, OAuthFlowError> {
        let client = OAuthClient::new(&self.client_id, &self.client_secret)?;
        let code = wait_for_verification_code(&self.client_id).await?;
        let token = client.exchange_code(&code, None).await?;
        Ok(token)
    }
}

async fn wait_for_verification_code(client_id: &str) -> Result<String, OAuthFlowError> {
    println!(
        "Open this URL in your browser:\n{}",
        authorize_url(client_id)
    );
    print!("Enter the verification code: ");
    std::io::stdout().flush()?;
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    Ok(input.trim().to_string())
}

fn authorize_url(client_id: &str) -> String {
    format!(
        "https://oauth.yandex.ru/authorize?response_type=code&client_id={}",
        client_id
    )
}
