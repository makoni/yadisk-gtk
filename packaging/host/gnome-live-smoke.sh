#!/usr/bin/env bash
set -euo pipefail

SERVICE="com.yadisk.Sync1"
OBJECT="/com/yadisk/Sync1"
IFACE="com.yadisk.Sync1"
TEST_PATH="${1:-/Docs/smoke.txt}"

echo "[smoke] restarting user daemon"
systemctl --user restart yadiskd.service
sleep 2

echo "[smoke] waiting for ${SERVICE}"
for _ in $(seq 1 20); do
  if gdbus introspect --session --dest "${SERVICE}" --object-path "${OBJECT}" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
gdbus introspect --session --dest "${SERVICE}" --object-path "${OBJECT}" >/dev/null

echo "[smoke] verifying DBus methods"
gdbus call --session --dest "${SERVICE}" --object-path "${OBJECT}" --method "${IFACE}.ListConflicts" >/dev/null

set +e
download_out="$(gdbus call --session --dest "${SERVICE}" --object-path "${OBJECT}" --method "${IFACE}.Download" "${TEST_PATH}" 2>&1)"
download_code=$?
set -e
if [[ ${download_code} -ne 0 ]] && [[ "${download_out}" != *"com.yadisk.Sync1.Error.NotFound"* ]]; then
  echo "[smoke] unexpected Download error: ${download_out}" >&2
  exit 1
fi

echo "[smoke] DBus smoke passed"
