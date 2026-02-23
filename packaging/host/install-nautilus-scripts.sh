#!/usr/bin/env bash
set -euo pipefail

target_dir="${HOME}/.local/share/nautilus/scripts"
mkdir -p "${target_dir}"

install -m 0755 packaging/host/nautilus-save-offline.sh \
  "${target_dir}/Yandex Disk - Save Offline"
install -m 0755 packaging/host/nautilus-apply-status-emblems.sh \
  "${target_dir}/Yandex Disk - Apply Status Emblems"

echo "Installed Nautilus scripts to: ${target_dir}"
echo "Restart Files (Nautilus) and use right-click -> Scripts."
