#!/usr/bin/env bash
# Use existing Bridge dependencies and registered service access, from any working directory.
set -euo pipefail
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
repo_dir="$(cd -- "$script_dir/../.." && pwd)"
workspace_dir="$(cd -- "$repo_dir/../.." && pwd)"
bridge_dir="${PLATFORM_BRIDGE_ROOT:-$workspace_dir/platform-bridge}"
if [[ ! -x "$bridge_dir/.venv/bin/python" ]]; then
  echo "Bridge Python environment not found: $bridge_dir/.venv/bin/python" >&2
  exit 1
fi
export PLATFORM_BRIDGE_ROOT="$bridge_dir"
exec "$bridge_dir/.venv/bin/python" "$script_dir/reset_demo_to_step1.py" "$@"
