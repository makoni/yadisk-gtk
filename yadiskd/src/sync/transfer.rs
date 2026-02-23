use std::{
    env, io,
    path::{Path, PathBuf},
    sync::Arc,
};

use futures_util::StreamExt;
use md5::Context;
use reqwest::Client;
use thiserror::Error;
use tokio::io::AsyncWriteExt;
use tokio::sync::Semaphore;
use tokio_util::io::ReaderStream;
use url::Url;

#[derive(Debug, Error)]
pub enum TransferError {
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("invalid url: {0}")]
    Url(#[from] url::ParseError),
    #[error("concurrency limiter is closed")]
    ConcurrencyClosed,
    #[error("download integrity check failed: expected {expected_md5}, got {actual_md5}")]
    IntegrityMismatch {
        expected_md5: String,
        actual_md5: String,
    },
}

#[derive(Clone)]
pub struct TransferClient {
    http: Client,
    download_limit: Arc<Semaphore>,
    upload_limit: Arc<Semaphore>,
}

#[derive(Debug, Clone, Copy)]
pub struct TransferConfig {
    pub download_concurrency: usize,
    pub upload_concurrency: usize,
}

impl Default for TransferConfig {
    fn default() -> Self {
        Self {
            download_concurrency: read_limit("YADISK_DOWNLOAD_CONCURRENCY", 4),
            upload_concurrency: read_limit("YADISK_UPLOAD_CONCURRENCY", 2),
        }
    }
}

impl TransferClient {
    pub fn new() -> Self {
        Self::with_config(TransferConfig::default())
    }

    pub fn with_config(config: TransferConfig) -> Self {
        Self {
            http: Client::new(),
            download_limit: Arc::new(Semaphore::new(config.download_concurrency.max(1))),
            upload_limit: Arc::new(Semaphore::new(config.upload_concurrency.max(1))),
        }
    }

    #[allow(dead_code)]
    pub async fn download_to_path(&self, href: &str, target: &Path) -> Result<(), TransferError> {
        self.download_to_path_checked(href, target, None).await
    }

    pub async fn download_to_path_checked(
        &self,
        href: &str,
        target: &Path,
        expected_md5: Option<&str>,
    ) -> Result<(), TransferError> {
        let _permit = self
            .download_limit
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| TransferError::ConcurrencyClosed)?;
        let url = Url::parse(href)?;
        let response = self.http.get(url).send().await?.error_for_status()?;

        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let partial = partial_path(target);
        let mut file = tokio::fs::File::create(&partial).await?;
        let mut stream = response.bytes_stream();
        let mut md5 = expected_md5.map(|_| Context::new());

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
            if let Some(ctx) = md5.as_mut() {
                ctx.consume(&chunk);
            }
        }

        file.flush().await?;
        file.sync_all().await?;

        if let Some(expected_md5) = expected_md5 {
            let actual_md5 = format!("{:x}", md5.expect("md5 initialized").compute());
            if actual_md5 != expected_md5.to_ascii_lowercase() {
                let _ = tokio::fs::remove_file(&partial).await;
                return Err(TransferError::IntegrityMismatch {
                    expected_md5: expected_md5.to_ascii_lowercase(),
                    actual_md5,
                });
            }
        }

        tokio::fs::rename(partial, target).await?;
        Ok(())
    }

    pub async fn upload_from_path(&self, href: &str, source: &Path) -> Result<(), TransferError> {
        let _permit = self
            .upload_limit
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| TransferError::ConcurrencyClosed)?;
        let url = Url::parse(href)?;
        let file = tokio::fs::File::open(source).await?;
        let stream = ReaderStream::new(file);
        let body = reqwest::Body::wrap_stream(stream);
        self.http
            .put(url)
            .body(body)
            .send()
            .await?
            .error_for_status()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn with_http(http: Client) -> Self {
        let config = TransferConfig::default();
        Self {
            http,
            download_limit: Arc::new(Semaphore::new(config.download_concurrency.max(1))),
            upload_limit: Arc::new(Semaphore::new(config.upload_concurrency.max(1))),
        }
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

fn partial_path(target: &Path) -> PathBuf {
    target.with_extension(format!(
        "{}partial",
        target
            .extension()
            .map(|ext| format!("{}.", ext.to_string_lossy()))
            .unwrap_or_default()
    ))
}

fn read_limit(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default)
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

        assert_eq!(std::fs::read(target).unwrap(), b"hello");
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
        std::fs::write(&source, b"payload").unwrap();

        let client = TransferClient::new();
        client
            .upload_from_path(&format!("{}/upload", server.uri()), &source)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn fails_when_md5_does_not_match() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/file"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello"))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let target = dir.path().join("bad.txt");
        let client = TransferClient::new();

        let err = client
            .download_to_path_checked(&format!("{}/file", server.uri()), &target, Some("deadbeef"))
            .await
            .expect_err("expected md5 mismatch");

        assert!(matches!(err, TransferError::IntegrityMismatch { .. }));
        assert!(!target.exists());
    }
}
