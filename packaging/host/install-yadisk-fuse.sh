#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

if ! command -v pkg-config >/dev/null 2>&1; then
  if command -v sudo >/dev/null 2>&1 && command -v apt-get >/dev/null 2>&1; then
    echo "[install] pkg-config is missing; installing it"
    sudo apt-get update
    sudo apt-get install -y pkg-config
  else
    echo "[install] missing dependency: pkg-config" >&2
    exit 1
  fi
fi

if ! pkg-config --exists fuse3; then
  if command -v sudo >/dev/null 2>&1 && command -v apt-get >/dev/null 2>&1; then
    echo "[install] fuse3 development files are missing; installing libfuse3-dev"
    sudo apt-get update
    sudo apt-get install -y libfuse3-dev
  else
    echo "[install] missing dependency: fuse3.pc (install libfuse3-dev)" >&2
    echo "[install] hint: set PKG_CONFIG_PATH if fuse3.pc is in a non-standard location" >&2
    exit 1
  fi
fi

echo "[install] building yadisk-fuse binary"
cargo build -p yadisk-fuse --release --features fuse-mount

install_dir="${HOME}/.local/bin"
mkdir -p "${install_dir}"
install -m 0755 "target/release/yadisk-fuse" "${install_dir}/yadisk-fuse"

echo "[install] installed: ${install_dir}/yadisk-fuse"
echo "[install] run: ${install_dir}/yadisk-fuse --mount \"${HOME}/Yandex Disk\""
