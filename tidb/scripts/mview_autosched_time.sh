#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/mview_common.sh
source "$SCRIPT_DIR/mview_common.sh"

mview_init_root "$SCRIPT_DIR"

TARBALL_URL="${1:-${TARBALL_URL:-}}"
BINARY_URLS="${2:-${BINARY_URLS:-}}"
NEMESIS="${NEMESIS:-clock-skew}"
mview_init_base_env "mv-autosched-time skeleton"
mview_init_time_limit 900
mview_require_tarball "$0 <tarball-url> [binary-urls]" " NEMESIS TIME_LIMIT"
mview_populate_build_args
mview_warn_if_experimental mv-autosched-time
mview_run_test mv-autosched-time "$NEMESIS" "$TIME_LIMIT"
