#!/usr/bin/env bash
set -euo pipefail

SERVICE="me.spaceinbox.yadisk.Sync1"
OBJECT="/me/spaceinbox/yadisk/Sync1"
IFACE="me.spaceinbox.yadisk.Sync1"
SYNC_ROOT="${YADISK_SYNC_DIR:-$HOME/Yandex Disk}"

map_remote_path() {
  local local_path="$1"
  local clean_sync="${SYNC_ROOT%/}"
  local clean_local="${local_path%/}"
  if [[ "${clean_local}" != "${clean_sync}"* ]]; then
    return 1
  fi
  local rel="${clean_local#${clean_sync}}"
  rel="${rel#/}"
  printf 'disk:/%s' "${rel}"
}

pin_and_download() {
  local remote_path="$1"
  gdbus call --session \
    --dest "${SERVICE}" \
    --object-path "${OBJECT}" \
    --method "${IFACE}.Pin" \
    "${remote_path}" true >/dev/null
  gdbus call --session \
    --dest "${SERVICE}" \
    --object-path "${OBJECT}" \
    --method "${IFACE}.Download" \
    "${remote_path}" >/dev/null
}

process_one() {
  local local_path="$1"
  local remote_path
  if ! remote_path="$(map_remote_path "${local_path}")"; then
    echo "[yadisk] skipped (outside sync root): ${local_path}" >&2
    return 1
  fi
  if ! pin_and_download "${remote_path}"; then
    local fallback="/${remote_path#disk:/}"
    pin_and_download "${fallback}"
  fi
}

if [[ -n "${NAUTILUS_SCRIPT_SELECTED_FILE_PATHS:-}" ]]; then
  while IFS= read -r selected; do
    [[ -z "${selected}" ]] && continue
    process_one "${selected}"
  done <<< "${NAUTILUS_SCRIPT_SELECTED_FILE_PATHS}"
else
  for arg in "$@"; do
    process_one "${arg}"
  done
fi
