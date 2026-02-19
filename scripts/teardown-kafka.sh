#!/bin/bash
# Kafka teardown script
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_teardown-common.sh"
parse_common_args "$@"

echo "=== Kafka Teardown ==="
cd "$SCRIPT_DIR/.."

confirm "Stop Kafka cluster and destroy data?" || { echo "Aborted."; exit 0; }

run_cmd make teardown-kafka
log_info "Kafka teardown complete."
