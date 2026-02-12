# Copilot instructions (yadisk-gtk)

## Build / run / format / lint

- Build all crates:
  - `cargo build --workspace`
- Run the daemon (prints `/v1/disk` info as JSON):
  - `cargo run -p yadiskd`
  - Auth inputs (checked in this order):
    - `YADISK_TOKEN` (raw OAuth token, bypasses storage + OAuth flow)
    - stored token via `yadiskd::storage::TokenStorage`
    - OAuth code flow via portal browser open:
      - `YADISK_CLIENT_ID`, `YADISK_CLIENT_SECRET`
- Format:
  - `cargo fmt --all`
- Lint (repo convention: no warnings):
  - `cargo clippy --workspace --all-targets -- -D warnings`

## Tests

- Run all tests:
  - `cargo test --workspace`
- Run a single test by name (Rust test filter):
  - `cargo test -p yadisk-core get_disk_info_includes_oauth_header`
  - `cargo test -p yadiskd extracts_code_from_request_line`
- Run a specific integration test target:
  - `cargo test -p yadisk-core --test client create_folder_uses_put`

## High-level architecture

This repo is a Rust workspace with two main crates:

- `yadisk-core/`: pure REST + OAuth client library (no GNOME specifics)
  - `YadiskClient` (`yadisk-core/src/client.rs`): wraps Yandex Disk REST endpoints.
    - Uses `Authorization: OAuth <token>` header everywhere.
    - Endpoints implemented so far:
      - disk info: `GET /v1/disk`
      - resource metadata + listing: `GET /v1/disk/resources` (`list_directory` expects `_embedded.items`)
      - transfer links: `GET /v1/disk/resources/download|upload`
      - mutations: `PUT /v1/disk/resources` (mkdir), `PUT /resources/move|copy`, `DELETE /resources`
      - async op polling: `GET /v1/disk/operations/{id}` via `get_operation_status()`
  - `OAuthClient` (`yadisk-core/src/oauth.rs`): builds `/authorize` URL and exchanges an auth `code` via `POST /token`.
  - HTTP tests use `wiremock` (see `yadisk-core/tests/*.rs`).

- `yadiskd/`: daemon/sync prototype + portal-friendly auth/token handling
  - OAuth UX (`yadiskd/src/oauth_flow.rs`):
    - Starts a loopback listener on `127.0.0.1:0`, opens the browser via the **OpenURI portal** (`ashpd::desktop::open_uri::OpenFileRequest`),
      then extracts `?code=` from the redirect request and exchanges it via `yadisk-core::OAuthClient`.
  - Token storage (`yadiskd/src/storage/`):
    - **Portal-first**: uses `org.freedesktop.portal.Secret` via `ashpd` to derive a per-app secret and encrypt the token to disk
      (`PortalTokenStore`, file under `${XDG_CONFIG_HOME}/yadisk-gtk/secret-portal/yadisk_token.portal`).
    - Falls back to system keyring via the `keyring` crate when portals are disabled/allowed to fallback.
    - Portal selection is controlled by:
      - `YADISK_ENABLE_SECRET_PORTAL=1` / `YADISK_DISABLE_SECRET_PORTAL=1`
      - sandbox hints: `FLATPAK_ID`, `CONTAINER=flatpak`
  - Sync building blocks (`yadiskd/src/sync/`):
    - `queue.rs`: in-memory FIFO operation queue (TDD covered)
    - `index.rs`: SQLite-backed index for items + states + sync cursor + persisted ops queue (`sqlx`, in-memory DB in tests)
    - `backoff.rs`: exponential backoff helper with optional jitter
    - `conflict.rs`: conflict resolution helper (three-way compare; keep-both rename strategy)

## Key conventions and patterns

- **Edition/toolchain:** crates use Rust **edition 2024**; keep code compatible with the workspace toolchain (`cargo --version` / `rustc --version`).
- **Portal-first (Flatpak-friendly):**
  - Anything dealing with secrets should integrate through `TokenStorage` (portal-backed where possible) rather than calling the keyring directly.
  - Anything that needs to open external URLs (OAuth) should go through the OpenURI portal (`ashpd`) instead of spawning a browser.
- **API error handling:**
  - `yadisk-core::YadiskError::Api { status, body }` preserves the HTTP status + body; keep this behavior when adding new endpoints.
- **Test style:**
  - REST client behavior is verified via `wiremock` matchers (method/path/query/header) rather than hitting the real API.
  - SQLite logic is tested against `sqlite::memory:` and must call `IndexStore::init()` before queries.
- **Clippy gate:** the repo runs clippy with `-D warnings`; avoid introducing unused items/imports unless they are intentionally behind `#[cfg(test)]` or actively used.

