#!/usr/bin/env bash
# bootstrap.sh – Register all data services in OpenMetadata via the REST API.
# Run once after the OpenMetadata stack is healthy.
#
# Usage:
#   OM_HOST=http://localhost:8585 OM_TOKEN=<jwt> ./bootstrap.sh

set -euo pipefail

OM_HOST="${OM_HOST:-http://localhost:8585}"
OM_TOKEN="${OM_TOKEN:-CHANGE_ME}"
API="${OM_HOST}/api/v1"

log() { echo "[$(date -u +%T)] $*"; }

auth_header() { echo "Authorization: Bearer ${OM_TOKEN}"; }

# ─── Helper ──────────────────────────────────────────────────────────────────
om_post() {
  local path="$1"
  local body="$2"
  curl -sf -X POST \
    -H "Content-Type: application/json" \
    -H "$(auth_header)" \
    -d "${body}" \
    "${API}${path}" | python3 -m json.tool --no-ensure-ascii 2>/dev/null || true
}

om_put() {
  local path="$1"
  local body="$2"
  curl -sf -X PUT \
    -H "Content-Type: application/json" \
    -H "$(auth_header)" \
    -d "${body}" \
    "${API}${path}" | python3 -m json.tool --no-ensure-ascii 2>/dev/null || true
}

# ─── Wait for server ──────────────────────────────────────────────────────────
log "Waiting for OpenMetadata server at ${OM_HOST}..."
for i in $(seq 1 30); do
  if curl -sf "${OM_HOST}/api/v1/system/version" >/dev/null 2>&1; then
    log "Server is ready."
    break
  fi
  [ "$i" -eq 30 ] && { log "ERROR: Server not ready after 30 retries."; exit 1; }
  sleep 5
done

# ─── Register Kafka service ──────────────────────────────────────────────────
log "Registering Kafka messaging service..."
om_put "/services/messagingServices" '{
  "serviceType": "Kafka",
  "name": "de-capstone-kafka",
  "displayName": "DE Capstone Kafka",
  "description": "KRaft broker for real-time trade and alert events.",
  "connection": {
    "config": {
      "type": "Kafka",
      "bootstrapServers": "kafka:9092"
    }
  }
}'

# ─── Register Iceberg / DuckDB service ───────────────────────────────────────
log "Registering Iceberg database service..."
om_put "/services/databaseServices" '{
  "serviceType": "Datalake",
  "name": "de-capstone-iceberg",
  "displayName": "DE Capstone Iceberg Lakehouse",
  "description": "Medallion lakehouse: bronze / silver / gold layers via PyIceberg + REST catalog.",
  "connection": {
    "config": {
      "type": "Datalake",
      "configSource": {"securityConfig": {}},
      "databaseName": "iceberg"
    }
  }
}'

# ─── Register Dagster pipeline service ───────────────────────────────────────
log "Registering Dagster pipeline service..."
om_put "/services/pipelineServices" '{
  "serviceType": "Dagster",
  "name": "de-capstone-dagster",
  "displayName": "DE Capstone Dagster",
  "description": "Dagster orchestration for the DE capstone lakehouse pipeline.",
  "connection": {
    "config": {
      "type": "Dagster",
      "host": "http://dagster-webserver:3000"
    }
  }
}'

# ─── Create classification taxonomies ────────────────────────────────────────
log "Creating classification taxonomies..."

for classification in \
  '{"name":"Sensitivity","displayName":"Sensitivity","description":"Data sensitivity levels: PII, Confidential, Restricted, Public.","mutuallyExclusive":true}' \
  '{"name":"DataLayer","displayName":"Data Layer","description":"Lakehouse medallion layer: Bronze, Silver, Gold.","mutuallyExclusive":true}' \
  '{"name":"DataDomain","displayName":"Data Domain","description":"Business domain: Trade, Order, Instrument, Analytics.","mutuallyExclusive":false}' \
; do
  om_put "/classifications" "${classification}"
done

# ─── Create classification tags ──────────────────────────────────────────────
log "Creating classification tags..."

# Sensitivity
for tag in \
  '{"name":"PII","displayName":"PII","description":"Personally Identifiable Information — requires masking or redaction.","classification":"Sensitivity"}' \
  '{"name":"Confidential","displayName":"Confidential","description":"Internal-only; not for external sharing.","classification":"Sensitivity"}' \
  '{"name":"Restricted","displayName":"Restricted","description":"Access restricted to specific roles.","classification":"Sensitivity"}' \
  '{"name":"Public","displayName":"Public","description":"Freely shareable data.","classification":"Sensitivity"}' \
; do
  om_put "/tags" "${tag}"
done

# DataLayer
for tag in \
  '{"name":"Bronze","displayName":"Bronze","description":"Raw ingested data — exact copy of source.","classification":"DataLayer"}' \
  '{"name":"Silver","displayName":"Silver","description":"Cleansed and enriched data.","classification":"DataLayer"}' \
  '{"name":"Gold","displayName":"Gold","description":"Aggregated business-ready data.","classification":"DataLayer"}' \
; do
  om_put "/tags" "${tag}"
done

# DataDomain
for tag in \
  '{"name":"Trade","displayName":"Trade","description":"Trade execution events.","classification":"DataDomain"}' \
  '{"name":"Order","displayName":"Order","description":"Order book events.","classification":"DataDomain"}' \
  '{"name":"Instrument","displayName":"Instrument","description":"Financial instrument master data.","classification":"DataDomain"}' \
  '{"name":"Analytics","displayName":"Analytics","description":"Derived analytical aggregates.","classification":"DataDomain"}' \
; do
  om_put "/tags" "${tag}"
done

log "Bootstrap complete."
