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
# 1. Register app at https://oauth.yandex.com/
# 2. Set env vars:
export YADISK_CLIENT_ID=<client_id>
export YADISK_CLIENT_SECRET=<client_secret>
cargo run -p yadiskd
```

## Flatpak Integration

The project follows Flatpak-first approach:

- **Portal storage**: Uses `org.freedesktop.portal.Secret` for token encryption
- **OpenURI portal**: OAuth flow opens browser via portal
- **Host-helper model**: UI in sandbox, daemon/FUSE on host (for Flathub compatibility)

Flatpak manifest: `packaging/flatpak/com.yadisk.Gtk.json`

## Status

All TODO items completed — project is in MVP state.

See `TODO.md` for detailed implementation status.
