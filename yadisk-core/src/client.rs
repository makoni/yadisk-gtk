use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

const DEFAULT_BASE_URL: &str = "https://cloud-api.yandex.net";

#[derive(Debug, Error)]
pub enum YadiskError {
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("invalid base url: {0}")]
    Url(#[from] url::ParseError),
    #[error("api returned {status}: {body}")]
    Api { status: StatusCode, body: String },
    #[error("api response missing embedded items")]
    MissingEmbedded,
}

#[derive(Clone)]
pub struct YadiskClient {
    http: Client,
    base_url: Url,
    token: String,
}

impl YadiskClient {
    pub fn new(token: impl Into<String>) -> Result<Self, YadiskError> {
        Self::with_base_url(DEFAULT_BASE_URL, token)
    }

    pub fn with_base_url(base_url: &str, token: impl Into<String>) -> Result<Self, YadiskError> {
        Ok(Self {
            http: Client::new(),
            base_url: Url::parse(base_url)?,
            token: token.into(),
        })
    }

    pub async fn get_disk_info(&self) -> Result<DiskInfo, YadiskError> {
        let url = self.endpoint("/v1/disk")?;
        let response = self
            .http
            .get(url)
            .header("Authorization", self.auth_header_value())
            .send()
            .await?;
        Self::handle_response(response).await
    }

    pub async fn get_resource(&self, path: &str) -> Result<Resource, YadiskError> {
        let mut url = self.endpoint("/v1/disk/resources")?;
        url.query_pairs_mut().append_pair("path", path);
        let response = self
            .http
            .get(url)
            .header("Authorization", self.auth_header_value())
            .send()
            .await?;
        Self::handle_response(response).await
    }

    pub async fn list_directory(
        &self,
        path: &str,
        limit: Option<u32>,
        offset: Option<u32>,
    ) -> Result<ResourceList, YadiskError> {
        let mut url = self.endpoint("/v1/disk/resources")?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("path", path);
            if let Some(limit) = limit {
                query.append_pair("limit", &limit.to_string());
            }
            if let Some(offset) = offset {
                query.append_pair("offset", &offset.to_string());
            }
        }
        let response = self
            .http
            .get(url)
            .header("Authorization", self.auth_header_value())
            .send()
            .await?;
        let payload: ResourceListResponse = Self::handle_response(response).await?;
        payload.embedded.ok_or(YadiskError::MissingEmbedded)
    }

    pub async fn get_download_link(&self, path: &str) -> Result<TransferLink, YadiskError> {
        let mut url = self.endpoint("/v1/disk/resources/download")?;
        url.query_pairs_mut().append_pair("path", path);
        let response = self
            .http
            .get(url)
            .header("Authorization", self.auth_header_value())
            .send()
            .await?;
        Self::handle_response(response).await
    }

    pub async fn get_upload_link(
        &self,
        path: &str,
        overwrite: bool,
    ) -> Result<TransferLink, YadiskError> {
        let mut url = self.endpoint("/v1/disk/resources/upload")?;
        url.query_pairs_mut()
            .append_pair("path", path)
            .append_pair("overwrite", if overwrite { "true" } else { "false" });
        let response = self
            .http
            .get(url)
            .header("Authorization", self.auth_header_value())
            .send()
            .await?;
        Self::handle_response(response).await
    }

    fn auth_header_value(&self) -> String {
        format!("OAuth {}", self.token)
    }

    fn endpoint(&self, path: &str) -> Result<Url, YadiskError> {
        Ok(self.base_url.join(path)?)
    }

    async fn handle_response<T: serde::de::DeserializeOwned>(
        response: reqwest::Response,
    ) -> Result<T, YadiskError> {
        if response.status().is_success() {
            Ok(response.json::<T>().await?)
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            Err(YadiskError::Api { status, body })
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DiskInfo {
    pub total_space: u64,
    pub used_space: u64,
    #[serde(default)]
    pub trash_size: u64,
    #[serde(default)]
    pub is_paid: bool,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Resource {
    pub path: String,
    pub name: String,
    #[serde(rename = "type")]
    pub resource_type: ResourceType,
    #[serde(default)]
    pub size: Option<u64>,
    #[serde(default)]
    pub modified: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ResourceType {
    File,
    Dir,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ResourceList {
    pub items: Vec<Resource>,
    pub limit: u32,
    pub offset: u32,
    pub total: u32,
}

#[derive(Debug, Deserialize, Serialize)]
struct ResourceListResponse {
    #[serde(rename = "_embedded")]
    embedded: Option<ResourceList>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct TransferLink {
    pub href: Url,
    pub method: String,
    #[serde(default)]
    pub templated: bool,
}
