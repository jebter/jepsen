#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=scripts/mview_common.sh
source "$SCRIPT_DIR/mview_common.sh"

mview_init_root "$SCRIPT_DIR"

TARBALL_URL="${1:-${TARBALL_URL:-}}"
BINARY_URLS="${2:-${BINARY_URLS:-}}"
mview_init_base_env "full single fault suite"
mview_require_tarball "$0 <tarball-url> [binary-urls]" " WORKLOAD_FILTER"
mview_populate_build_args

mview_run_suite_cases full-single-fault
