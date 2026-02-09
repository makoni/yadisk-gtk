use serde_json::json;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};
use yadisk_core::OAuthClient;

#[test]
fn authorize_url_includes_required_params() {
    let client = OAuthClient::with_base_url("https://oauth.example", "client-id", "secret")
        .expect("client should build");
    let url = client
        .authorize_url(
            "http://localhost/callback",
            Some("disk:read"),
            Some("state-1"),
        )
        .expect("url should build");

    let query = url.query().unwrap_or_default();
    assert!(query.contains("response_type=code"));
    assert!(query.contains("client_id=client-id"));
    assert!(query.contains("redirect_uri=http%3A%2F%2Flocalhost%2Fcallback"));
    assert!(query.contains("scope=disk%3Aread"));
    assert!(query.contains("state=state-1"));
}

#[tokio::test]
async fn exchange_code_posts_form_data() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/token"))
        .and(body_string_contains("grant_type=authorization_code"))
        .and(body_string_contains("code=auth-code"))
        .and(body_string_contains("client_id=client-id"))
        .and(body_string_contains("client_secret=secret"))
        .and(body_string_contains(
            "redirect_uri=http%3A%2F%2Flocalhost%2Fcallback",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "access_token": "token",
            "token_type": "bearer",
            "expires_in": 3600,
            "refresh_token": "refresh",
            "scope": "disk:read"
        })))
        .mount(&server)
        .await;

    let client = OAuthClient::with_base_url(&server.uri(), "client-id", "secret").unwrap();
    let token = client
        .exchange_code("auth-code", Some("http://localhost/callback"))
        .await
        .unwrap();

    assert_eq!(token.access_token, "token");
    assert_eq!(token.token_type, "bearer");
    assert_eq!(token.expires_in, Some(3600));
}
