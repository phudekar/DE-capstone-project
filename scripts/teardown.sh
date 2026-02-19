#!/bin/bash
# Global teardown script
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_teardown-common.sh"

DESTROY_DATA=false

usage() {
  echo "Usage: $0 [OPTIONS]"
  echo ""
  echo "Options:"
  echo "  --destroy-data    Also remove Docker volumes (data loss!)"
  echo "  --dry-run         Show what would be done without executing"
  echo "  --force, -f       Skip confirmation prompts"
  echo "  --help            Show this help"
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --destroy-data) DESTROY_DATA=true; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    --force|-f) FORCE=true; shift ;;
    --help) usage; exit 0 ;;
    *) echo "Unknown option: $1"; usage; exit 1 ;;
  esac
done

echo "=== DE Project Teardown ==="
echo ""

if [ "$DESTROY_DATA" = true ]; then
  log_warn "This will stop all containers AND destroy all data volumes!"
  confirm "Are you sure?" || { echo "Aborted."; exit 0; }
fi

cd "$SCRIPT_DIR/.."

if [ "$DESTROY_DATA" = true ]; then
  log_info "Stopping all services and destroying volumes..."
  run_cmd make teardown-destroy
else
  log_info "Stopping all services (preserving data)..."
  run_cmd make down
fi

log_info "Teardown complete."
