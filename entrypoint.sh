#!/bin/bash
set -e

check_mount() {
  local path=$1
  local max_wait=${2:-30}
  local count=0

  if [ "${SKIP_MOUNT_CHECK:-false}" = "true" ]; then
    echo "Skipping mount check for: $path"
    return 0
  fi

  echo "Waiting for EFS mount: $path"
  until [ -r "$path" ] || [ $count -ge $max_wait ]; do
    sleep 1
    count=$((count + 1))
  done

  if [ ! -r "$path" ]; then
    echo "ERROR: EFS mount not ready at $path after ${max_wait}s" >&2
    exit 1
  fi
  echo "EFS mount ready: $path"
}

# Check all required mounts before starting the application to avoid NFS issues
check_mount "/mnt/data/sword/na_sword_v17.nc"

echo "All mounts ready, starting application..."
exec /app/env/bin/python3 /app/run_input.py "$@"