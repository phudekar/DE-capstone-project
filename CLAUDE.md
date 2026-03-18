# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Real-time stock exchange simulation pipeline: DE-Stock (Rust simulator) → Kafka → Iceberg lakehouse with medallion architecture (Bronze/Silver/Gold), orchestrated by Dagster, served via GraphQL API and Superset dashboards, monitored by Prometheus/Grafana.

## Common Commands

### Infrastructure
```bash
make up              # Start infra + services (no Flink)
make down            # Stop all stacks (preserves data)
make up-infra        # Kafka + Storage only
make up-flink        # Flink jobmanager + taskmanager (opt-in profile)
make up-dagster      # Dagster webserver + daemon + postgres
make up-monitoring   # Prometheus + Grafana + Alertmanager
make up-superset     # Superset full stack
make init-lakehouse  # Create Iceberg tables + seed dimensions
```

### Testing
```bash
make test            # All tests (unit + e2e)
make test-unit       # Unit tests across all services
make test-e2e        # 206 e2e pipeline tests (no Docker needed)
make test-superset   # Superset analytical query tests

# Single service tests
pytest services/lakehouse/tests
pytest services/graphql-api/tests
pytest tests/e2e -m "not integration"
```

### Code Quality
```bash
make lint            # ruff check .
make format          # ruff format .
```

Ruff config: line-length 120, target py311, rules E/F/I/W. Pre-commit hooks run ruff lint+format, detect-secrets, shellcheck.

### Flink Jobs
```bash
make submit-sql-pipeline      # Job A: SQL trade aggregation
make submit-python-pipeline   # Job B: Price alerts + orderbook analytics
make submit-all-flink         # Both jobs
```

### Cleanup
```bash
make teardown-destroy   # Stop all + destroy volumes
make clean-data         # Wipe Kafka + Lakehouse + Dagster data
make clean-kafka        # Selective wipe
make clean-lakehouse    # Selective wipe (then run make up-storage && make init-lakehouse)
```

## Architecture

### Data Flow
```
DE-Stock (Rust :8765) → WebSocket → Kafka Bridge → Kafka (KRaft :9092)
  ├─→ Lakehouse Bronze writer (continuous Kafka consumer → Iceberg)
  ├─→ Flink SQL pipeline (trade aggregation)
  └─→ Flink Python pipeline (price alerts + orderbook analytics)

Lakehouse: Bronze (raw) → Silver (dedup/enrich) → Gold (OHLCV, VWAP)

Dagster orchestrates Silver/Gold batch processing
GraphQL API (:8000) serves DuckDB-on-Iceberg queries + Kafka subscriptions
Superset (:8088) provides BI dashboards
Grafana (:3001) monitors infrastructure metrics
```

### Docker Compose Structure
All compose files in `infrastructure/docker-compose/`, combined via Makefile variables:
- `docker-compose.base.yml` — Networks + volumes
- `docker-compose.kafka.yml` — Broker, Schema Registry, Kafka UI, topic init
- `docker-compose.storage.yml` — MinIO, MinIO init, Iceberg REST catalog
- `docker-compose.flink.yml` — JobManager + TaskManager (profile: "flink")
- `docker-compose.services.yml` — DE-Stock, Kafka Bridge, Lakehouse, GraphQL API, Trading Dashboard
- `docker-compose.dagster.yml` — Webserver, daemon, postgres (:5433)
- `docker-compose.monitoring.yml` — Prometheus, Grafana, Alertmanager, pushgateway, node-exporter, cadvisor

Standalone: `services/superset/docker-compose.yml` (web, worker, beat, redis, postgres)

**Important:** Uses standalone `docker-compose` command (NOT `docker compose` plugin).

### Key Ports
| Service | Port | Service | Port |
|---------|------|---------|------|
| DE-Stock | 8765 | Grafana | 3001 |
| Kafka Bridge | 9090 | Prometheus | 9099 |
| Kafka Broker | 9092 | Alertmanager | 9093 |
| Schema Registry | 8081 | Superset | 8088 |
| Kafka UI | 8089 | Dagster | 3000 |
| MinIO | 9000/9001 | Trading Dashboard | 3002 |
| Iceberg REST | 8181 | Flink UI | 8082 |
| GraphQL API | 8000 | | |

## Service Directory Layout

- `libs/common/` — Shared Pydantic models (de-common package)
- `services/kafka-bridge/` — WebSocket → Kafka router
- `services/lakehouse/` — PyIceberg Bronze/Silver/Gold writers + processors
- `services/flink-processor/` — PyFlink stream jobs (SQL + Python DataStream)
- `services/graphql-api/` — FastAPI + Strawberry GraphQL server
- `services/dagster-orchestrator/` — Dagster assets, sensors, schedules, jobs
- `services/data-quality/` — Great Expectations suites + custom expectations
- `services/superset/` — Superset config, bootstrap scripts, custom ECharts plugins
- `services/trading-dashboard/` — React + TypeScript + Vite frontend
- `services/de-stock/` — Rust exchange simulator config

Each Python service has its own `pyproject.toml` and `src/` layout (except graphql-api and dagster-orchestrator which use root layout).

## Critical Gotchas

### `from __future__ import annotations`
**Never use in files with Dagster decorators** (`@asset`, `@op`, `@sensor`, `@hook`) — breaks Dagster 1.12 at runtime. Also causes issues with Strawberry GraphQL nested type resolution in schema files.

### PyFlink output types
Python UDFs must declare `output_type=Types.STRING()` on `flat_map()`/`map()` when sinking to Kafka with `SimpleStringSchema`, otherwise PyFlink sends bytes across the JVM boundary causing `ClassCastException`.

### Flink CI tests
Install flink-processor with `pip install --no-deps` in CI — apache-flink 1.18 pins numpy==1.21.4 which conflicts with modern environments.

### Great Expectations version
Pinned to 0.18.22 (NOT 1.0). Uses `RuntimeBatchRequest`, `BaseDataContext`, `InMemoryStoreBackendDefaults`.

### Docker memory
Colima on macOS needs `colima start --memory 8 --cpu 4`. Flink requires ≥1GB process memory per container.

### Dagster schedules/sensors
Start in STOPPED state by default — enable in Dagster UI or CLI.

### Iceberg streaming reads
Use `to_arrow_batch_reader()` not `to_arrow()` for DuckDB reads to prevent OOM on large tables.

## Environment Setup

`.env` file at `infrastructure/docker-compose/.env` (copy from `.env.example`). Key vars: `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `KAFKA_CLUSTER_ID`.

All Python services require **Python 3.11** (Dagster incompatible with 3.14+, Flink incompatible with newer numpy).

## CI/CD

- **CI** (`ci.yml`): 10 parallel jobs (lint + 8 test suites + ci-gate). All must pass for merge.
- **CD** (`cd.yml`): Builds 6 Docker images → ghcr.io on push to main.
- **Container scan**: Trivy scans built + third-party images; fails on fixable CRITICAL CVEs.
