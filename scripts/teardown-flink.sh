#!/bin/bash
# Flink teardown script
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_teardown-common.sh"
parse_common_args "$@"

echo "=== Flink Teardown ==="
cd "$SCRIPT_DIR/.."

confirm "Stop Flink and destroy checkpoint data?" || { echo "Aborted."; exit 0; }

run_cmd make teardown-flink
log_info "Flink teardown complete."
