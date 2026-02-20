#!/usr/bin/env bash
# build_plugins.sh — Build all custom Superset viz plugins.
# Usage: ./build_plugins.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLUGINS_DIR="$SCRIPT_DIR/../custom_plugins"

if ! command -v node &>/dev/null; then
  echo "ERROR: Node.js not found. Install Node 18+ first." >&2
  exit 1
fi

if ! command -v npm &>/dev/null; then
  echo "ERROR: npm not found." >&2
  exit 1
fi

echo "Building custom Superset plugins from $PLUGINS_DIR ..."

for PLUGIN_DIR in "$PLUGINS_DIR"/*/; do
  PLUGIN_NAME=$(basename "$PLUGIN_DIR")
  echo ""
  echo "=== Building $PLUGIN_NAME ==="

  if [ ! -f "$PLUGIN_DIR/package.json" ]; then
    echo "  Skipping (no package.json)"
    continue
  fi

  pushd "$PLUGIN_DIR" >/dev/null
  echo "  Installing dependencies ..."
  npm ci --silent

  echo "  Running TypeScript checks ..."
  npx tsc --noEmit 2>&1 | head -50 || true

  echo "  Building ..."
  npm run build 2>&1 | tail -20

  echo "  Done: $PLUGIN_NAME"
  popd >/dev/null
done

echo ""
echo "All plugins built successfully."
echo "Artifacts are in each plugin's dist/ directory."
