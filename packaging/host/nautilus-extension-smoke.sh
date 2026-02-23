#!/usr/bin/env bash
set -euo pipefail

if [[ -n "${YADISK_NAUTILUS_EXT_DIR:-}" ]]; then
  ext_dir="${YADISK_NAUTILUS_EXT_DIR}"
elif command -v pkg-config >/dev/null 2>&1 && pkg-config --exists libnautilus-extension-4; then
  ext_dir="$(pkg-config --variable=extensiondir libnautilus-extension-4)"
else
  ext_dir="$HOME/.local/lib/nautilus/extensions-4"
fi
so_path="${ext_dir}/libyadisk_nautilus.so"

if [[ ! -f "${so_path}" ]]; then
  echo "[smoke] extension not found: ${so_path}" >&2
  exit 1
fi

echo "[smoke] checking exported Nautilus entry points"
nm -D "${so_path}" | grep -q "nautilus_module_initialize"
nm -D "${so_path}" | grep -q "nautilus_module_shutdown"
nm -D "${so_path}" | grep -q "nautilus_module_list_types"

echo "[smoke] checking daemon D-Bus is reachable"
gdbus introspect --session --dest com.yadisk.Sync1 --object-path /com/yadisk/Sync1 >/dev/null

echo "[smoke] extension + D-Bus checks passed"
