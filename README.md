# yadisk-gtk

Native GNOME client for Yandex Disk (Rust, REST API, Flatpak)

## Overview

This project implements a two-way sync client for Yandex Disk with deep GNOME integration:

- **FUSE filesystem** (`~/YandexDisk`) — browse cloud files in Files (Nautilus)
- **On-demand download** — files downloaded on access
- **D-Bus daemon** — background sync, queue management, retry logic
- **Nautilus extension** — file state emblems and context menu actions

## Architecture

```
┌─────────────────┐     D-Bus      ┌──────────────────┐
│   yadiskd       │◀──────────────▶│  Nautilus        │
│ (sync daemon)   │                │  (extension)     │
└────────┬────────┘                └──────────────────┘
         │
         ▼
┌─────────────────┐
│  FUSE FS        │
│ (yadisk-fuse)   │
└─────────────────┘
```

### Crates

| Crate | Description |
|-------|-------------|
| `yadisk-core` | REST API client + OAuth flow (no GNOME specifics) |
| `yadiskd` | Daemon: D-Bus API, SQLite index, sync engine |
| `yadisk-fuse` | FUSE bridge for Files integration |
| `yadisk-integrations` | Nautilus extension + libcloudproviders scaffolds |
| `yadisk-nautilus` | Native Nautilus extension (cdylib) + D-Bus action client |

## Requirements

- Rust 1.80+ (edition 2024)
- SQLite
- D-Bus (zbus)
- GTK 4 (for UI)

## Build & Run

```bash
# Build all crates
cargo build --workspace

# Run tests
cargo test --workspace

# Lint (no warnings allowed)
cargo clippy --workspace --all-targets -- -D warnings

# Format
cargo fmt --all

# Run daemon with token
YADISK_TOKEN=<your_token> cargo run -p yadiskd

# Run daemon via OAuth flow
# 1. Register app at https://oauth.yandex.ru/
# 2. Set env vars:
export YADISK_CLIENT_ID=<client_id>
export YADISK_CLIENT_SECRET=<client_secret>
cargo run -p yadiskd
# By default yadiskd opens system browser via portal and captures code automatically.
# In graphical GNOME session with `zenity` installed it also shows native auth dialogs.
# Fallback (headless/manual): export YADISK_OAUTH_FORCE_MANUAL=1
# Optional timeout for browser callback: export YADISK_OAUTH_TIMEOUT_SECS=180

# Logout (remove saved OAuth token and force new auth on next start)
cargo run -p yadiskd -- --logout
```

### `yadisk-ui` orchestration CLI (MVP)

```bash
# auth + daemon control
cargo run -p yadisk-ui -- --start-auth
cargo run -p yadisk-ui -- --start-daemon
cargo run -p yadisk-ui -- --restart-daemon
cargo run -p yadisk-ui -- --enable-autostart

# integrations
cargo run -p yadisk-ui -- --check-integrations
cargo run -p yadisk-ui -- --install-integrations-guided

# diagnostics/settings snapshots (JSON)
cargo run -p yadisk-ui -- --show-settings
cargo run -p yadisk-ui -- --diagnostics

# GTK/libadwaita shell (MVP)
cargo run -p yadisk-ui        # default: launches GTK window
cargo run -p yadisk-ui -- --gtk

# force CLI status mode
cargo run -p yadisk-ui -- --status

# open specific tab directly
cargo run -p yadisk-ui -- --tab-welcome
cargo run -p yadisk-ui -- --tab-sync
cargo run -p yadisk-ui -- --tab-integrations
cargo run -p yadisk-ui -- --tab-settings
cargo run -p yadisk-ui -- --tab-diagnostics
```

### GNOME status tray icon (indicator)

`yadiskd` now starts a status tray indicator (StatusNotifier/AppIndicator) with one menu action: `Quit`.

- states:
  - `normal` → everything synchronized
  - `syncing` → sync in progress
  - `error` → sync failed
- icons are loaded from `yadiskd/assets/status/` (`normal.svg`, `syncing.svg`, `error.svg`)

Optional env:

```bash
# disable tray icon
export YADISK_DISABLE_STATUS_TRAY=1

# override icon directory
export YADISK_STATUS_ICON_DIR=/path/to/status/icons
```

## Native Nautilus extension (Rust, GNOME 49 baseline)

Install host extension:

```bash
bash packaging/host/install-nautilus-extension.sh
nautilus -q
```

`install-nautilus-extension.sh` автоматически определяет `extensiondir` через `pkg-config`.
Если каталог системный и не writable, скрипт сначала пробует `pkexec` (GUI-диалог пароля), затем fallback на `sudo`.

What it provides in Files (Nautilus):
- state emblems (`cloud_only` / `cached` / `syncing` / `error`)
- state-aware context menu:
  - `Save Offline`
  - `Download Now`
  - `Remove Offline Copy`
- D-Bus actions via `me.spaceinbox.yadisk.Sync1` (`Pin`, `Download`, `Evict`)
- live status refresh from daemon signals

Optional smoke check:

```bash
bash packaging/host/nautilus-extension-smoke.sh
```

Requirements:
- GNOME Files 49 + `libnautilus-extension` development/runtime package on host
- running `yadiskd` daemon in user session

For custom/non-standard extension path, set:

```bash
export YADISK_NAUTILUS_EXT_DIR=/path/to/nautilus/extensions-4
```

## On-demand open by double click (FUSE mode)

Чтобы double click сразу открывал реальный контент (а не 0-byte placeholder), используйте FUSE mount:

```bash
# 1) host deps
sudo apt install -y fuse3 libfuse3-dev

# 2) install fuse helper
bash packaging/host/install-yadisk-fuse.sh

# 3) run mount
~/.local/bin/yadisk-fuse --mount "$HOME/Yandex Disk"
```

В этом режиме чтение файла из Nautilus автоматически триггерит `Download(...)` через D-Bus и ждёт появления файла в кэше.
Если при сборке видите `fuse3.pc ... not found`, установите `libfuse3-dev` (и `pkg-config`), затем повторите скрипт.

## Локальное e2e тестирование (daemon + Nautilus + FUSE)

```bash
# 0) зависимости для host integration
sudo apt install -y libnautilus-extension-dev fuse3 libfuse3-dev pkg-config

# 1) проверка workspace
cargo fmt --all
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings

# 2) запуск демона (выберите один вариант auth)
YADISK_TOKEN=<token> cargo run -p yadiskd
# или:
export YADISK_CLIENT_ID=<client_id>
export YADISK_CLIENT_SECRET=<client_secret>
cargo run -p yadiskd
```

В отдельном терминале:

```bash
# 3) установка/обновление Nautilus extension
bash packaging/host/install-nautilus-extension.sh

# 4) перезагрузка Nautilus для подхвата новой .so
nautilus -q
nautilus "$HOME/Yandex Disk"

# 5) smoke-проверка расширения и D-Bus
bash packaging/host/nautilus-extension-smoke.sh
```

Для double click on-demand download через FUSE:

```bash
# 6) установка/обновление FUSE helper
bash packaging/host/install-yadisk-fuse.sh

# 7) запуск mount (держите процесс запущенным)
~/.local/bin/yadisk-fuse --mount "$HOME/Yandex Disk"
```

Проверки в Nautilus:
- контекстное меню: `Save Offline`, `Download Now`, `Remove Offline Copy`
- эмблемы состояния (монохромные symbolic)
- после `Save Offline` размер файла становится реальным
- в FUSE-режиме double click открывает реальный контент (а не 0-byte placeholder)

Быстрый dev-цикл после изменений extension:

```bash
bash packaging/host/install-nautilus-extension.sh && nautilus -q
```

## Flatpak Integration

The project follows Flatpak-first approach:

- **Portal storage**: Uses `org.freedesktop.portal.Secret` for token encryption
- **OpenURI portal**: OAuth flow opens browser via portal
- **Host-helper model**: UI in sandbox, daemon/FUSE on host (for Flathub compatibility)

Flatpak manifest: `packaging/flatpak/me.spaceinbox.yadisk.Gtk.json`

## Status

All TODO items completed — project is in MVP state.

See `TODO.md` for detailed implementation status.
