use std::net::SocketAddr;

use ashpd::desktop::open_uri::OpenFileRequest;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use url::Url;
use yadisk_core::{OAuthClient, OAuthToken};

const DEFAULT_SCOPE: &str = "disk:read disk:write";

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
    scope: String,
}

impl OAuthFlow {
    pub fn new(client_id: impl Into<String>, client_secret: impl Into<String>) -> Self {
        Self {
            client_id: client_id.into(),
            client_secret: client_secret.into(),
            scope: DEFAULT_SCOPE.to_string(),
        }
    }

    pub async fn authenticate(&self) -> Result<OAuthToken, OAuthFlowError> {
        let client = OAuthClient::new(&self.client_id, &self.client_secret)?;
        let (listener, redirect_uri) = bind_loopback().await?;
        let authorize_url = client.authorize_url(&redirect_uri, Some(&self.scope), None)?;

        open_in_browser(&authorize_url).await?;
        let code = wait_for_code(listener).await?;
        let token = client.exchange_code(&code, Some(&redirect_uri)).await?;
        Ok(token)
    }
}

async fn bind_loopback() -> Result<(TcpListener, String), OAuthFlowError> {
    let listener = TcpListener::bind(("127.0.0.1", 0)).await?;
    let addr = listener.local_addr()?;
    Ok((listener, redirect_uri(addr)))
}

fn redirect_uri(addr: SocketAddr) -> String {
    format!("http://{}:{}/callback", addr.ip(), addr.port())
}

async fn open_in_browser(url: &Url) -> Result<(), OAuthFlowError> {
    let request = OpenFileRequest::default().ask(true).send_uri(url).await?;
    request.response()?;
    Ok(())
}

async fn wait_for_code(listener: TcpListener) -> Result<String, OAuthFlowError> {
    let (mut socket, _) = listener.accept().await?;
    let mut buffer = [0u8; 4096];
    let read = socket.read(&mut buffer).await?;
    let request = String::from_utf8_lossy(&buffer[..read]);

    let code = extract_code_from_request(&request).ok_or(OAuthFlowError::MissingCode)?;
    let response = "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\nYou can close this window.";
    socket.write_all(response.as_bytes()).await?;
    socket.shutdown().await?;

    Ok(code)
}

fn extract_code_from_request(request: &str) -> Option<String> {
    let line = request.lines().next()?;
    let mut parts = line.split_whitespace();
    let method = parts.next()?;
    if method != "GET" {
        return None;
    }
    let path = parts.next()?;
    let url = Url::parse(&format!("http://localhost{path}")).ok()?;
    url.query_pairs()
        .find(|(key, _)| key == "code")
        .map(|(_, value)| value.to_string())
}

#[cfg(test)]
mod tests {
    use super::extract_code_from_request;

    #[test]
    fn extracts_code_from_request_line() {
        let request = "GET /callback?code=abc123&state=xyz HTTP/1.1\r\nHost: localhost\r\n\r\n";
        let code = extract_code_from_request(request).expect("code should be parsed");
        assert_eq!(code, "abc123");
    }

    #[test]
    fn returns_none_when_code_missing() {
        let request = "GET /callback?state=xyz HTTP/1.1\r\nHost: localhost\r\n\r\n";
        assert!(extract_code_from_request(request).is_none());
    }
}
