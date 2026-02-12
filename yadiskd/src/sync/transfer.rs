use std::{
    fs, io,
    path::{Path, PathBuf},
};

use reqwest::Client;
use thiserror::Error;
use url::Url;

#[derive(Debug, Error)]
pub enum TransferError {
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("invalid url: {0}")]
    Url(#[from] url::ParseError),
}

#[derive(Clone)]
pub struct TransferClient {
    http: Client,
}

impl TransferClient {
    pub fn new() -> Self {
        Self {
            http: Client::new(),
        }
    }

    pub async fn download_to_path(&self, href: &str, target: &Path) -> Result<(), TransferError> {
        let url = Url::parse(href)?;
        let response = self.http.get(url).send().await?.error_for_status()?;
        let bytes = response.bytes().await?;

        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(target, &bytes)?;
        Ok(())
    }

    pub async fn upload_from_path(&self, href: &str, source: &Path) -> Result<(), TransferError> {
        let url = Url::parse(href)?;
        let bytes = fs::read(source)?;
        self.http
            .put(url)
            .body(bytes)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn with_http(http: Client) -> Self {
        Self { http }
    }

    #[allow(dead_code)]
    pub fn download_target_path(&self, root: &Path, remote_path: &str) -> PathBuf {
        root.join(remote_path.trim_start_matches('/'))
    }
}

impl Default for TransferClient {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use wiremock::matchers::{body_bytes, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn downloads_file_to_target_path() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/file"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello"))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let target = dir.path().join("nested/out.txt");
        let client = TransferClient::new();

        client
            .download_to_path(&format!("{}/file", server.uri()), &target)
            .await
            .unwrap();

        assert_eq!(fs::read(target).unwrap(), b"hello");
    }

    #[tokio::test]
    async fn uploads_file_contents() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/upload"))
            .and(body_bytes(b"payload"))
            .respond_with(ResponseTemplate::new(201))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let source = dir.path().join("in.bin");
        fs::write(&source, b"payload").unwrap();

        let client = TransferClient::new();
        client
            .upload_from_path(&format!("{}/upload", server.uri()), &source)
            .await
            .unwrap();
    }
}
