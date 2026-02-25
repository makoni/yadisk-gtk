#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${repo_root}"

target_so="target/release/libyadisk_nautilus.so"
icon_src_dir="${repo_root}/packaging/icons/hicolor/scalable/emblems"
icon_dst_dir="${XDG_DATA_HOME:-$HOME/.local/share}/icons/hicolor/scalable/emblems"
if [[ -n "${YADISK_NAUTILUS_EXT_DIR:-}" ]]; then
  ext_dir="${YADISK_NAUTILUS_EXT_DIR}"
elif command -v pkg-config >/dev/null 2>&1 && pkg-config --exists libnautilus-extension-4; then
  ext_dir="$(pkg-config --variable=extensiondir libnautilus-extension-4)"
else
  ext_dir="$HOME/.local/lib/nautilus/extensions-4"
fi

echo "[install] building Rust Nautilus extension"
cargo build -p yadisk-nautilus --release --features nautilus-plugin

echo "[install] installing emblem icons to ${icon_dst_dir}"
mkdir -p "${icon_dst_dir}"
install -m 0644 "${icon_src_dir}/check-round-outline-symbolic.svg" "${icon_dst_dir}/check-round-outline-symbolic.svg"
install -m 0644 "${icon_src_dir}/cloud-outline-thin-symbolic.svg" "${icon_dst_dir}/cloud-outline-thin-symbolic.svg"
install -m 0644 "${icon_src_dir}/update-symbolic.svg" "${icon_dst_dir}/update-symbolic.svg"
if [[ ! -d "${ext_dir}" ]]; then
  mkdir -p "${ext_dir}" 2>/dev/null || true
fi

if [[ ! -w "${ext_dir}" ]]; then
  if command -v sudo >/dev/null 2>&1; then
    echo "[install] extension dir requires root permissions: ${ext_dir}"
    sudo install -D -m 0755 "${repo_root}/${target_so}" "${ext_dir}/libyadisk_nautilus.so"
    echo "[install] installed with sudo: ${ext_dir}/libyadisk_nautilus.so"
    echo "[install] dependency note: install libnautilus-extension(-dev) for GNOME Files 49"
    echo "[install] restart Nautilus: nautilus -q"
    exit 0
  fi
  echo "[install] extension dir is not writable and sudo is unavailable: ${ext_dir}" >&2
  exit 1
fi

install -m 0755 "${target_so}" "${ext_dir}/libyadisk_nautilus.so"

echo "[install] installed: ${ext_dir}/libyadisk_nautilus.so"
echo "[install] dependency note: install libnautilus-extension(-dev) for GNOME Files 49"
echo "[install] restart Nautilus: nautilus -q"
