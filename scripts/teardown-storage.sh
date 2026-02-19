#!/bin/bash
# Storage teardown script (MinIO + Iceberg)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_teardown-common.sh"
parse_common_args "$@"

echo "=== Storage Teardown ==="
cd "$SCRIPT_DIR/.."

confirm "Stop MinIO/Iceberg and destroy data?" || { echo "Aborted."; exit 0; }

run_cmd make teardown-storage
log_info "Storage teardown complete."
