use serde_json::json;
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};
use yadisk_core::{OperationStatus, ResourceType, YadiskClient};

#[tokio::test]
async fn get_disk_info_includes_oauth_header() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/disk"))
        .and(header("authorization", "OAuth test-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "total_space": 1024,
            "used_space": 256,
            "trash_size": 0,
            "is_paid": false
        })))
        .mount(&server)
        .await;

    let client = YadiskClient::with_base_url(&server.uri(), "test-token").unwrap();
    let info = client.get_disk_info().await.unwrap();

    assert_eq!(info.total_space, 1024);
    assert_eq!(info.used_space, 256);
    assert!(!info.is_paid);
}

#[tokio::test]
async fn get_resource_encodes_path() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/disk/resources"))
        .and(query_param("path", "/Docs/Hello World.txt"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "path": "/Docs/Hello World.txt",
            "name": "Hello World.txt",
            "type": "file",
            "size": 12,
            "modified": "2024-01-01T00:00:00Z"
        })))
        .mount(&server)
        .await;

    let client = YadiskClient::with_base_url(&server.uri(), "test-token").unwrap();
    let resource = client.get_resource("/Docs/Hello World.txt").await.unwrap();

    assert_eq!(resource.resource_type, ResourceType::File);
    assert_eq!(resource.size, Some(12));
}

#[tokio::test]
async fn get_download_link_returns_href() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/disk/resources/download"))
        .and(query_param("path", "/Docs/Hello.txt"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "href": "https://download.example/hello.txt",
            "method": "GET",
            "templated": false
        })))
        .mount(&server)
        .await;

    let client = YadiskClient::with_base_url(&server.uri(), "test-token").unwrap();
    let link = client.get_download_link("/Docs/Hello.txt").await.unwrap();

    assert_eq!(link.href.as_str(), "https://download.example/hello.txt");
    assert_eq!(link.method, "GET");
}

#[tokio::test]
async fn get_upload_link_sends_overwrite_flag() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/disk/resources/upload"))
        .and(query_param("path", "/Docs/Hello.txt"))
        .and(query_param("overwrite", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "href": "https://upload.example/hello.txt",
            "method": "PUT",
            "templated": false
        })))
        .mount(&server)
        .await;

    let client = YadiskClient::with_base_url(&server.uri(), "test-token").unwrap();
    let link = client
        .get_upload_link("/Docs/Hello.txt", true)
        .await
        .unwrap();

    assert_eq!(link.href.as_str(), "https://upload.example/hello.txt");
    assert_eq!(link.method, "PUT");
}

#[tokio::test]
async fn list_directory_returns_embedded_items() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/disk/resources"))
        .and(query_param("path", "/Docs"))
        .and(query_param("limit", "2"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "_embedded": {
                "limit": 2,
                "offset": 0,
                "total": 4,
                "items": [
                    {
                        "path": "/Docs/A.txt",
                        "name": "A.txt",
                        "type": "file",
                        "size": 1
                    },
                    {
                        "path": "/Docs/B",
                        "name": "B",
                        "type": "dir"
                    }
                ]
            }
        })))
        .mount(&server)
        .await;

    let client = YadiskClient::with_base_url(&server.uri(), "test-token").unwrap();
    let list = client
        .list_directory("/Docs", Some(2), Some(0))
        .await
        .unwrap();

    assert_eq!(list.limit, 2);
    assert_eq!(list.total, 4);
    assert_eq!(list.items.len(), 2);
    assert_eq!(list.items[0].name, "A.txt");
    assert_eq!(list.items[1].resource_type, ResourceType::Dir);
}

#[tokio::test]
async fn create_folder_uses_put() {
    let server = MockServer::start().await;

    Mock::given(method("PUT"))
        .and(path("/v1/disk/resources"))
        .and(query_param("path", "/Docs/NewFolder"))
        .and(header("authorization", "OAuth test-token"))
        .respond_with(ResponseTemplate::new(201).set_body_json(json!({
            "path": "/Docs/NewFolder",
            "name": "NewFolder",
            "type": "dir"
        })))
        .mount(&server)
        .await;

    let client = YadiskClient::with_base_url(&server.uri(), "test-token").unwrap();
    let resource = client.create_folder("/Docs/NewFolder").await.unwrap();

    assert_eq!(resource.resource_type, ResourceType::Dir);
    assert_eq!(resource.name, "NewFolder");
}

#[tokio::test]
async fn move_resource_returns_operation_link() {
    let server = MockServer::start().await;

    Mock::given(method("PUT"))
        .and(path("/v1/disk/resources/move"))
        .and(query_param("from", "/Docs/A.txt"))
        .and(query_param("path", "/Docs/B.txt"))
        .and(query_param("overwrite", "true"))
        .and(header("authorization", "OAuth test-token"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "href": "https://cloud-api.yandex.net/v1/disk/operations/1",
            "method": "GET",
            "templated": false
        })))
        .mount(&server)
        .await;

    let client = YadiskClient::with_base_url(&server.uri(), "test-token").unwrap();
    let link = client
        .move_resource("/Docs/A.txt", "/Docs/B.txt", true)
        .await
        .unwrap();

    assert_eq!(
        link.href.as_str(),
        "https://cloud-api.yandex.net/v1/disk/operations/1"
    );
}

#[tokio::test]
async fn copy_resource_returns_operation_link() {
    let server = MockServer::start().await;

    Mock::given(method("PUT"))
        .and(path("/v1/disk/resources/copy"))
        .and(query_param("from", "/Docs/A.txt"))
        .and(query_param("path", "/Docs/C.txt"))
        .and(query_param("overwrite", "false"))
        .and(header("authorization", "OAuth test-token"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "href": "https://cloud-api.yandex.net/v1/disk/operations/2",
            "method": "GET",
            "templated": false
        })))
        .mount(&server)
        .await;

    let client = YadiskClient::with_base_url(&server.uri(), "test-token").unwrap();
    let link = client
        .copy_resource("/Docs/A.txt", "/Docs/C.txt", false)
        .await
        .unwrap();

    assert_eq!(
        link.href.as_str(),
        "https://cloud-api.yandex.net/v1/disk/operations/2"
    );
}

#[tokio::test]
async fn delete_resource_returns_none_on_no_content() {
    let server = MockServer::start().await;

    Mock::given(method("DELETE"))
        .and(path("/v1/disk/resources"))
        .and(query_param("path", "/Docs/Delete.txt"))
        .and(header("authorization", "OAuth test-token"))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;

    let client = YadiskClient::with_base_url(&server.uri(), "test-token").unwrap();
    let response = client
        .delete_resource("/Docs/Delete.txt", false)
        .await
        .unwrap();

    assert!(response.is_none());
}

#[tokio::test]
async fn delete_resource_returns_operation_link() {
    let server = MockServer::start().await;

    Mock::given(method("DELETE"))
        .and(path("/v1/disk/resources"))
        .and(query_param("path", "/Docs/Delete.txt"))
        .and(query_param("permanently", "true"))
        .and(header("authorization", "OAuth test-token"))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({
            "href": "https://cloud-api.yandex.net/v1/disk/operations/3",
            "method": "GET",
            "templated": false
        })))
        .mount(&server)
        .await;

    let client = YadiskClient::with_base_url(&server.uri(), "test-token").unwrap();
    let response = client
        .delete_resource("/Docs/Delete.txt", true)
        .await
        .unwrap();

    let link = response.expect("expected operation link");
    assert_eq!(
        link.href.as_str(),
        "https://cloud-api.yandex.net/v1/disk/operations/3"
    );
}

#[tokio::test]
async fn get_operation_status_parses_response() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/disk/operations/123"))
        .and(header("authorization", "OAuth test-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "status": "success"
        })))
        .mount(&server)
        .await;

    let client = YadiskClient::with_base_url(&server.uri(), "test-token").unwrap();
    let status = client
        .get_operation_status(&format!("{}/v1/disk/operations/123", server.uri()))
        .await
        .unwrap();

    assert_eq!(status, OperationStatus::Success);
}
