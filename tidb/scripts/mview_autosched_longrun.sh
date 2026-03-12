#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/mview_common.sh
source "$SCRIPT_DIR/mview_common.sh"

mview_init_root "$SCRIPT_DIR"

TARBALL_URL="${1:-${TARBALL_URL:-}}"
BINARY_URLS="${2:-${BINARY_URLS:-}}"
mview_init_base_env "autosched longrun suite"
mview_init_time_limit 21600
mview_require_tarball "$0 <tarball-url> [binary-urls]" " TIME_LIMIT"
mview_populate_build_args

nemeses=(none kill-db pause-db partition shuffle-leader)
for nemesis in "${nemeses[@]}"; do
  mview_run_test mv-autosched "$nemesis" "$TIME_LIMIT"
done
