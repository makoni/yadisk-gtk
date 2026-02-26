use std::io::Write;
use std::process::Command;
use std::time::Duration;

use ashpd::desktop::open_uri::OpenFileRequest;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use url::Url;
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
    #[error("authorization timed out")]
    Timeout,
    #[error("authorization cancelled by user")]
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthUiState {
    Intro,
    AwaitingBrowser,
    ManualCodePrompt,
    Success,
    Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthUiAction {
    Continue,
    Retry,
    UseManualCode,
    Cancel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthUiBackend {
    Terminal,
    Zenity,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct AuthUiErrorMessage {
    title: &'static str,
    body: &'static str,
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
    let force_manual = env_flag("YADISK_OAUTH_FORCE_MANUAL");
    let backend = select_ui_backend(
        force_manual,
        has_graphical_session(),
        has_zenity_available(),
    );

    if !force_manual && prefers_portal_loopback_flow() {
        if matches!(backend, AuthUiBackend::Zenity) {
            match prompt_ui(backend, AuthUiState::Intro, client_id, None)? {
                AuthUiAction::Continue => {}
                AuthUiAction::Cancel => return Err(OAuthFlowError::Cancelled),
                _ => {}
            }
        }

        loop {
            let _ = prompt_ui(backend, AuthUiState::AwaitingBrowser, client_id, None);
            match wait_for_verification_code_via_loopback(client_id).await {
                Ok(code) => {
                    let _ = prompt_ui(backend, AuthUiState::Success, client_id, None);
                    return Ok(code);
                }
                Err(err) => {
                    if matches!(backend, AuthUiBackend::Terminal) {
                        eprintln!(
                            "[yadiskd] oauth auto-flow unavailable ({err}), falling back to manual code entry"
                        );
                        return wait_for_verification_code_manual(client_id, backend);
                    }
                    match prompt_ui(backend, AuthUiState::Error, client_id, Some(&err))? {
                        AuthUiAction::Retry => continue,
                        AuthUiAction::UseManualCode => {
                            return wait_for_verification_code_manual(client_id, backend);
                        }
                        AuthUiAction::Cancel => return Err(OAuthFlowError::Cancelled),
                        AuthUiAction::Continue => return Err(err),
                    }
                }
            }
        }
    }

    wait_for_verification_code_manual(client_id, backend)
}

fn wait_for_verification_code_manual(
    client_id: &str,
    backend: AuthUiBackend,
) -> Result<String, OAuthFlowError> {
    let url = authorize_url(client_id, None);
    let code = if matches!(backend, AuthUiBackend::Zenity) {
        prompt_ui(backend, AuthUiState::ManualCodePrompt, client_id, None)?;
        zenity_entry(
            "Yandex Disk: код подтверждения",
            &format!(
                "Откройте URL и вставьте код:\n{}\n\nВведите verification code:",
                url
            ),
        )?
    } else {
        println!("Open this URL in your browser:\n{}", url);
        print!("Enter the verification code: ");
        std::io::stdout().flush()?;
        let mut input = String::new();
        std::io::stdin().read_line(&mut input)?;
        input.trim().to_string()
    };
    if code.is_empty() {
        return Err(OAuthFlowError::MissingCode);
    }
    Ok(code)
}

async fn wait_for_verification_code_via_loopback(
    client_id: &str,
) -> Result<String, OAuthFlowError> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let redirect_uri = format!("http://127.0.0.1:{}/callback", addr.port());
    let url = authorize_url(client_id, Some(&redirect_uri));

    OpenFileRequest::default().ask(true).send_uri(&url).await?;

    let (mut stream, _) = tokio::time::timeout(oauth_timeout(), listener.accept())
        .await
        .map_err(|_| OAuthFlowError::Timeout)??;

    let mut request = vec![0u8; 8192];
    let read = tokio::time::timeout(Duration::from_secs(10), stream.read(&mut request))
        .await
        .map_err(|_| OAuthFlowError::Timeout)??;
    let request_text = String::from_utf8_lossy(&request[..read]);
    let code = extract_code_from_http_request(&request_text).ok_or(OAuthFlowError::MissingCode)?;

    let _ = stream
        .write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nConnection: close\r\n\r\n\
            <html><body><h2>Yandex Disk connected</h2><p>You can return to the app.</p></body></html>",
        )
        .await;
    let _ = stream.shutdown().await;

    Ok(code)
}

fn authorize_url(client_id: &str, redirect_uri: Option<&str>) -> Url {
    let mut url =
        Url::parse("https://oauth.yandex.ru/authorize").expect("hardcoded OAuth URL is valid");
    {
        let mut qp = url.query_pairs_mut();
        qp.append_pair("response_type", "code");
        qp.append_pair("client_id", client_id);
        if let Some(redirect_uri) = redirect_uri {
            qp.append_pair("redirect_uri", redirect_uri);
        }
    }
    url
}

fn extract_code_from_http_request(request: &str) -> Option<String> {
    let request_line = request.lines().next()?;
    let target = request_line.split_whitespace().nth(1)?;
    let request_url = if target.starts_with("http://") || target.starts_with("https://") {
        Url::parse(target).ok()?
    } else {
        Url::parse(&format!("http://127.0.0.1{target}")).ok()?
    };
    request_url
        .query_pairs()
        .find_map(|(key, value)| (key == "code" && !value.is_empty()).then(|| value.into_owned()))
}

fn prefers_portal_loopback_flow() -> bool {
    if env_flag("YADISK_OAUTH_FORCE_MANUAL") {
        return false;
    }
    has_graphical_session()
}

fn has_graphical_session() -> bool {
    has_non_empty_env("WAYLAND_DISPLAY") || has_non_empty_env("DISPLAY")
}

fn has_non_empty_env(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .is_some()
}

fn oauth_timeout() -> Duration {
    let secs = std::env::var("YADISK_OAUTH_TIMEOUT_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(180);
    Duration::from_secs(secs)
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn select_ui_backend(
    force_manual: bool,
    graphical_session: bool,
    zenity_available: bool,
) -> AuthUiBackend {
    if !force_manual && graphical_session && zenity_available {
        AuthUiBackend::Zenity
    } else {
        AuthUiBackend::Terminal
    }
}

fn has_zenity_available() -> bool {
    Command::new("sh")
        .arg("-c")
        .arg("command -v zenity >/dev/null 2>&1")
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn map_error_for_ui(err: &OAuthFlowError) -> AuthUiErrorMessage {
    match err {
        OAuthFlowError::Timeout => AuthUiErrorMessage {
            title: "Время ожидания истекло",
            body: "Не получен callback от браузера. Проверьте, что вход подтверждён, и повторите попытку.",
        },
        OAuthFlowError::Portal(_) => AuthUiErrorMessage {
            title: "Не удалось открыть браузер",
            body: "Портал OpenURI недоступен. Можно повторить попытку или ввести код вручную.",
        },
        OAuthFlowError::MissingCode => AuthUiErrorMessage {
            title: "Код подтверждения не получен",
            body: "Сервис не вернул параметр code. Повторите вход или введите код вручную.",
        },
        OAuthFlowError::OAuth(_) => AuthUiErrorMessage {
            title: "Ошибка обмена OAuth-кода",
            body: "Не удалось обменять код на токен. Проверьте клиентские ключи и сеть.",
        },
        OAuthFlowError::Io(_) => AuthUiErrorMessage {
            title: "Ошибка ввода-вывода",
            body: "Локальная операция завершилась ошибкой. Повторите попытку.",
        },
        OAuthFlowError::Cancelled => AuthUiErrorMessage {
            title: "Авторизация отменена",
            body: "Пользователь отменил процесс авторизации.",
        },
    }
}

fn prompt_ui(
    backend: AuthUiBackend,
    state: AuthUiState,
    client_id: &str,
    error: Option<&OAuthFlowError>,
) -> Result<AuthUiAction, OAuthFlowError> {
    if !matches!(backend, AuthUiBackend::Zenity) {
        return Ok(AuthUiAction::Continue);
    }
    let auth_url = authorize_url(client_id, None).to_string();
    match state {
        AuthUiState::Intro => {
            if zenity_question(
                "Подключение Яндекс Диска",
                &format!(
                    "Сейчас откроется системный браузер для входа.\n\nURL: {}\n\nПродолжить?",
                    auth_url
                ),
                "Продолжить",
                "Отмена",
            )? {
                Ok(AuthUiAction::Continue)
            } else {
                Ok(AuthUiAction::Cancel)
            }
        }
        AuthUiState::AwaitingBrowser => Ok(AuthUiAction::Continue),
        AuthUiState::ManualCodePrompt => Ok(AuthUiAction::UseManualCode),
        AuthUiState::Success => {
            zenity_info(
                "Яндекс Диск подключён",
                "Авторизация выполнена успешно. Можно вернуться к работе.",
            )?;
            Ok(AuthUiAction::Continue)
        }
        AuthUiState::Error => {
            let message = error.map(map_error_for_ui).unwrap_or(AuthUiErrorMessage {
                title: "Ошибка авторизации",
                body: "Авторизация завершилась ошибкой.",
            });
            let retry = zenity_question(
                message.title,
                message.body,
                "Повторить",
                "Ввести код вручную",
            )?;
            if retry {
                Ok(AuthUiAction::Retry)
            } else {
                let manual = zenity_question(
                    "Ручной ввод кода",
                    "Перейти к ручному вводу verification code?",
                    "Да",
                    "Отмена",
                )?;
                if manual {
                    Ok(AuthUiAction::UseManualCode)
                } else {
                    Ok(AuthUiAction::Cancel)
                }
            }
        }
    }
}

fn zenity_info(title: &str, text: &str) -> Result<(), OAuthFlowError> {
    let status = Command::new("zenity")
        .args(["--info", "--title", title, "--text", text])
        .status()?;
    if status.success() {
        Ok(())
    } else {
        Err(OAuthFlowError::Cancelled)
    }
}

fn zenity_question(
    title: &str,
    text: &str,
    ok_label: &str,
    cancel_label: &str,
) -> Result<bool, OAuthFlowError> {
    let status = Command::new("zenity")
        .args([
            "--question",
            "--title",
            title,
            "--text",
            text,
            "--ok-label",
            ok_label,
            "--cancel-label",
            cancel_label,
        ])
        .status()?;
    match status.code() {
        Some(0) => Ok(true),
        Some(1) => Ok(false),
        _ => Err(OAuthFlowError::Cancelled),
    }
}

fn zenity_entry(title: &str, text: &str) -> Result<String, OAuthFlowError> {
    let output = Command::new("zenity")
        .args(["--entry", "--title", title, "--text", text])
        .output()?;
    if !output.status.success() {
        return Err(OAuthFlowError::Cancelled);
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_code_from_request_line() {
        let req = "GET /callback?code=abc123&state=xyz HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n";
        assert_eq!(
            extract_code_from_http_request(req).as_deref(),
            Some("abc123")
        );
    }

    #[test]
    fn returns_none_when_code_missing() {
        let req = "GET /callback?state=xyz HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n";
        assert!(extract_code_from_http_request(req).is_none());
    }

    #[test]
    fn authorize_url_includes_redirect_uri() {
        let redirect = "http://127.0.0.1:9876/callback";
        let url = authorize_url("client-id", Some(redirect));
        let query: std::collections::HashMap<_, _> = url.query_pairs().into_owned().collect();
        assert_eq!(query.get("response_type"), Some(&"code".to_string()));
        assert_eq!(query.get("client_id"), Some(&"client-id".to_string()));
        assert_eq!(query.get("redirect_uri"), Some(&redirect.to_string()));
    }

    #[test]
    fn select_ui_backend_prefers_zenity_only_in_graphical_non_forced_mode() {
        assert_eq!(select_ui_backend(false, true, true), AuthUiBackend::Zenity);
        assert_eq!(select_ui_backend(true, true, true), AuthUiBackend::Terminal);
        assert_eq!(
            select_ui_backend(false, false, true),
            AuthUiBackend::Terminal
        );
        assert_eq!(
            select_ui_backend(false, true, false),
            AuthUiBackend::Terminal
        );
    }

    #[test]
    fn maps_timeout_error_to_user_facing_text() {
        let msg = map_error_for_ui(&OAuthFlowError::Timeout);
        assert!(msg.title.contains("ожидания"));
    }

    #[test]
    fn maps_missing_code_error_to_user_facing_text() {
        let err = OAuthFlowError::MissingCode;
        let msg = map_error_for_ui(&err);
        assert!(!msg.body.is_empty());
    }
}
