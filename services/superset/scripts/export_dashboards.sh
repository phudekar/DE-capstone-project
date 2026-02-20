#!/usr/bin/env bash
# export_dashboards.sh — Export all Superset dashboards to zip files.
# Usage: ./export_dashboards.sh [OUTPUT_DIR]
set -euo pipefail

SUPERSET_URL="${SUPERSET_URL:-http://localhost:8088}"
SUPERSET_USER="${SUPERSET_USER:-admin}"
SUPERSET_PASS="${SUPERSET_PASS:-admin}"
OUTPUT_DIR="${1:-./exports}"

mkdir -p "$OUTPUT_DIR"

echo "Authenticating with Superset at $SUPERSET_URL ..."
TOKEN=$(curl -s -X POST "$SUPERSET_URL/api/v1/security/login" \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"$SUPERSET_USER\",\"password\":\"$SUPERSET_PASS\",\"provider\":\"db\"}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['access_token'])")

echo "Fetching dashboard list ..."
DASHBOARD_IDS=$(curl -s "$SUPERSET_URL/api/v1/dashboard/" \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); [print(r['id'],r['slug']) for r in d['result']]")

echo "$DASHBOARD_IDS" | while read -r ID SLUG; do
  OUTFILE="$OUTPUT_DIR/dashboard_${SLUG:-$ID}.zip"
  echo "  Exporting dashboard $ID ($SLUG) → $OUTFILE"
  curl -s -o "$OUTFILE" \
    "$SUPERSET_URL/api/v1/dashboard/export/?q=(ids:!($ID))" \
    -H "Authorization: Bearer $TOKEN"
done

echo "Done. Exports saved to $OUTPUT_DIR/"
