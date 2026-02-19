#!/bin/bash
# Health check script for DE Project infrastructure
set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

pass=0
fail=0

check() {
  local name="$1"
  local cmd="$2"
  if eval "$cmd" > /dev/null 2>&1; then
    echo -e "  ${GREEN}[OK]${NC} $name"
    pass=$((pass + 1))
  else
    echo -e "  ${RED}[FAIL]${NC} $name"
    fail=$((fail + 1))
  fi
}

echo ""
echo "=== DE Project Health Check ==="
echo ""

# Kafka Brokers
echo "Kafka:"
check "Broker 1 (9092)" "docker exec kafka-broker-1 kafka-broker-api-versions --bootstrap-server localhost:29092"

# Kafka Topics
echo ""
echo "Kafka Topics:"
topics=$(docker exec kafka-broker-1 kafka-topics --bootstrap-server localhost:29092 --list 2>/dev/null || echo "")
for topic in raw.orders raw.trades raw.quotes raw.orderbook-snapshots raw.market-stats raw.trading-halts raw.agent-actions dlq.raw.failed; do
  if echo "$topics" | grep -q "^${topic}$"; then
    echo -e "  ${GREEN}[OK]${NC} Topic: $topic"
    pass=$((pass + 1))
  else
    echo -e "  ${RED}[FAIL]${NC} Topic: $topic"
    fail=$((fail + 1))
  fi
done

# Schema Registry
echo ""
echo "Schema Registry:"
check "Schema Registry (8081)" "curl -sf http://localhost:8081/subjects"

# Kafka UI
echo ""
echo "Kafka UI:"
check "Kafka UI (8080)" "curl -sf http://localhost:8080"

# MinIO
echo ""
echo "Storage:"
check "MinIO S3 API (9000)" "curl -sf http://localhost:9000/minio/health/live"
check "MinIO Console (9001)" "curl -sf http://localhost:9001"

# Iceberg REST
check "Iceberg REST (8181)" "curl -sf http://localhost:8181/v1/config"

# Flink (opt-in via `make up-flink`)
echo ""
echo "Flink (opt-in):"
if curl -sf http://localhost:8082/overview > /dev/null 2>&1; then
  echo -e "  ${GREEN}[OK]${NC} Flink JobManager (8082)"
  ((pass++))
else
  echo -e "  ${YELLOW}[SKIP]${NC} Flink JobManager (8082) — start with: make up-flink"
fi

# DE-Stock (opt-in via `make up-services`)
echo ""
echo "Services:"
if echo | nc -w1 localhost 8765 > /dev/null 2>&1; then
  echo -e "  ${GREEN}[OK]${NC} DE-Stock WebSocket (8765)"
  ((pass++))
else
  echo -e "  ${YELLOW}[SKIP]${NC} DE-Stock WebSocket (8765) — start with: make up-services"
fi

# Summary
echo ""
echo "================================"
echo -e "Results: ${GREEN}${pass} passed${NC}, ${RED}${fail} failed${NC}"
if [ "$fail" -gt 0 ]; then
  echo -e "${YELLOW}Some services are not healthy.${NC}"
  exit 1
else
  echo -e "${GREEN}All services are healthy!${NC}"
fi
