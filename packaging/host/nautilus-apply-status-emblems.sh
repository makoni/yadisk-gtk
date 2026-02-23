#!/usr/bin/env bash
set -euo pipefail

SERVICE="com.yadisk.Sync1"
OBJECT="/com/yadisk/Sync1"
IFACE="com.yadisk.Sync1"
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

fetch_state() {
  local remote_path="$1"
  local raw
  raw="$(gdbus call --session \
    --dest "${SERVICE}" \
    --object-path "${OBJECT}" \
    --method "${IFACE}.GetState" \
    "${remote_path}")"
  sed -n "s/.*'\\([^']*\\)'.*/\\1/p" <<< "${raw}"
}

state_to_emblem() {
  case "$1" in
    cloud_only) printf '%s' "emblem-default-symbolic" ;;
    cached) printf '%s' "emblem-ok-symbolic" ;;
    syncing) printf '%s' "emblem-synchronizing-symbolic" ;;
    error) printf '%s' "emblem-important-symbolic" ;;
    *) printf '%s' "" ;;
  esac
}

apply_emblem() {
  local local_path="$1"
  local emblem="$2"
  if [[ -z "${emblem}" ]]; then
    gio set "${local_path}" metadata::emblems ""
  else
    gio set "${local_path}" metadata::emblems "${emblem}"
  fi
}

process_one() {
  local local_path="$1"
  local remote_path
  if ! remote_path="$(map_remote_path "${local_path}")"; then
    echo "[yadisk] skipped (outside sync root): ${local_path}" >&2
    return 1
  fi
  local state
  if ! state="$(fetch_state "${remote_path}")"; then
    local fallback="/${remote_path#disk:/}"
    state="$(fetch_state "${fallback}")"
  fi
  apply_emblem "${local_path}" "$(state_to_emblem "${state}")"
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
