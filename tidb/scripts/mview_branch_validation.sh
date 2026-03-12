#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/mview_common.sh
source "$SCRIPT_DIR/mview_common.sh"

mview_init_root "$SCRIPT_DIR"

TARBALL_URL="${1:-${TARBALL_URL:-}}"
BINARY_URLS="${2:-${BINARY_URLS:-}}"
mview_init_base_env "branch validation suite"
mview_require_tarball "$0 <tarball-url> [binary-urls]" ""
mview_populate_build_args

cases=(
  "mv-stateful none 300"
  "mv-stateful kill-db 600"
  "mv-stateful pause-db 600"
  "mv-stateful partition 600"
  "mv-lifecycle none 300"
  "mv-lifecycle kill-db 600"
  "mv-lifecycle pause-db 600"
  "mv-lifecycle partition 600"
  "mv-autosched none 900"
  "mv-autosched kill-db 900"
  "mv-autosched partition 900"
)

for case_spec in "${cases[@]}"; do
  read -r workload nemesis time_limit <<<"$case_spec"
  mview_run_test "$workload" "$nemesis" "$time_limit"
done
