#!/usr/bin/env bash
# Register Avro schemas with Confluent Schema Registry and set BACKWARD compatibility.
set -euo pipefail

SCHEMA_REGISTRY_URL="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
SCHEMA_DIR="$(cd "$(dirname "$0")/../../libs/common/schemas/avro" && pwd)"

# subject → schema file mapping
declare -A SUBJECTS=(
  ["raw.orders-OrderPlaced"]="order_placed.avsc"
  ["raw.orders-OrderCancelled"]="order_cancelled.avsc"
  ["raw.trades-TradeExecuted"]="trade_executed.avsc"
  ["raw.quotes-QuoteUpdate"]="quote_update.avsc"
  ["raw.orderbook-snapshots-OrderBookSnapshot"]="orderbook_snapshot.avsc"
  ["raw.market-stats-MarketStats"]="market_stats.avsc"
  ["raw.trading-halts-TradingHalt"]="trading_halt.avsc"
  ["raw.trading-halts-TradingResume"]="trading_resume.avsc"
  ["raw.agent-actions-AgentAction"]="agent_action.avsc"
)

echo "Waiting for Schema Registry at ${SCHEMA_REGISTRY_URL}..."
for i in $(seq 1 30); do
  if curl -sf "${SCHEMA_REGISTRY_URL}/subjects" >/dev/null 2>&1; then
    echo "Schema Registry is ready."
    break
  fi
  if [ "$i" -eq 30 ]; then
    echo "ERROR: Schema Registry not reachable after 30s" >&2
    exit 1
  fi
  sleep 1
done

for subject in "${!SUBJECTS[@]}"; do
  schema_file="${SCHEMA_DIR}/${SUBJECTS[$subject]}"
  if [ ! -f "$schema_file" ]; then
    echo "ERROR: Schema file not found: $schema_file" >&2
    exit 1
  fi

  # Escape the schema JSON for embedding in the request payload
  schema_json=$(python3 -c "import json,sys; print(json.dumps(json.dumps(json.load(sys.stdin))))" < "$schema_file")

  echo "Registering schema: ${subject} from ${SUBJECTS[$subject]}"
  response=$(curl -sf -X POST \
    "${SCHEMA_REGISTRY_URL}/subjects/${subject}/versions" \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d "{\"schemaType\": \"AVRO\", \"schema\": ${schema_json}}")
  echo "  → id: $(echo "$response" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")"

  # Set BACKWARD compatibility
  curl -sf -X PUT \
    "${SCHEMA_REGISTRY_URL}/config/${subject}" \
    -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    -d '{"compatibility": "BACKWARD"}' >/dev/null
  echo "  → compatibility: BACKWARD"
done

echo ""
echo "All schemas registered successfully."
echo "Registered subjects:"
curl -sf "${SCHEMA_REGISTRY_URL}/subjects" | python3 -c "import json,sys; [print(f'  - {s}') for s in sorted(json.load(sys.stdin))]"
