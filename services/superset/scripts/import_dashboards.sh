#!/usr/bin/env bash
# import_dashboards.sh — Import Superset dashboards from zip files.
# Usage: ./import_dashboards.sh [ZIP_FILE_OR_DIR]
set -euo pipefail

SUPERSET_URL="${SUPERSET_URL:-http://localhost:8088}"
SUPERSET_USER="${SUPERSET_USER:-admin}"
SUPERSET_PASS="${SUPERSET_PASS:-admin}"
INPUT="${1:-.}"

echo "Authenticating with Superset at $SUPERSET_URL ..."
TOKEN=$(curl -s -X POST "$SUPERSET_URL/api/v1/security/login" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"$SUPERSET_USER\",\"password\":\"$SUPERSET_PASS\",\"provider\":\"db\"}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

CSRF=$(curl -s "$SUPERSET_URL/api/v1/security/csrf_token/" \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['result'])")

import_zip() {
  local ZIP="$1"
  echo "  Importing $ZIP ..."
  curl -s -X POST "$SUPERSET_URL/api/v1/dashboard/import/" \
    -H "Authorization: Bearer $TOKEN" \
    -H "X-CSRFToken: $CSRF" \
    -F "formData=@$ZIP;type=application/zip" \
    -F 'overwrite=true' \
    | python3 -c "import sys,json; r=json.load(sys.stdin); print('  OK' if r.get('message')=='OK' else r)"
}

if [ -f "$INPUT" ]; then
  import_zip "$INPUT"
elif [ -d "$INPUT" ]; then
  for ZIP in "$INPUT"/*.zip; do
    [ -f "$ZIP" ] && import_zip "$ZIP"
  done
else
  echo "ERROR: '$INPUT' is not a file or directory." >&2
  exit 1
fi

echo "Done."
