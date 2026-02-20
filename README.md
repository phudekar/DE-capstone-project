# DE Capstone Project — Real-Time Stock Market Data Pipeline

A production-grade data engineering capstone that simulates a live stock exchange,
streams trade events through a full modern data stack, and surfaces analytics in
interactive dashboards.

---

## Architecture

```
 DATA GENERATION          INGESTION             STREAM PROCESSING        STORAGE
┌────────────────┐    ┌──────────────┐    ┌──────────────────────┐  ┌───────────────┐
│  DE-Stock      │ WS │ Kafka Bridge │    │    Apache Kafka       │  │ Apache Iceberg│
│ (Rust exchange │───▶│ (WS → Kafka) │───▶│  raw.trades          │─▶│ (Parquet on   │
│  simulator)    │    │              │    │  raw.orderbook-…     │  │  MinIO / S3)  │
│                │    └──────────────┘    │  raw.quotes          │  └──────┬────────┘
│ 6 symbols      │                        │  raw.orders          │         │
│ 9 event types  │                        │  processed.*         │         ▼
└────────────────┘                        │  alerts.*            │  ┌───────────────┐
                                          └─────────┬────────────┘  │    DuckDB     │
                                                    │               │ (in-memory    │
                                          ┌─────────▼────────────┐  │  analytics)   │
                                          │    Apache Flink       │  └──────┬────────┘
                                          │ • Trade enrichment    │         │
                                          │ • VWAP windows        │         ▼
                                          │ • Price alerts        │  ┌───────────────┐
                                          └──────────────────────┘  │ Medallion     │
                                                                     │ Bronze→Silver │
                                                                     │ →Gold         │
                                                                     └───────────────┘

 ORCHESTRATION        DATA QUALITY       API & SERVING        VISUALIZATION
┌─────────────┐    ┌──────────────┐    ┌──────────────┐    ┌────────────────┐
│   Dagster   │    │    Great     │    │  GraphQL API │    │    Apache      │
│ 16 assets   │───▶│ Expectations │    │  (Strawberry │───▶│    Superset    │
│  4 sensors  │    │ 3 suites     │    │   + FastAPI) │    │ 6 dashboards   │
│  5 schedules│    │ custom OHLCV │    │  :8000       │    │ :8088          │
└─────────────┘    └──────────────┘    └──────────────┘    └────────────────┘

 GOVERNANCE              MONITORING
┌──────────────┐    ┌─────────────────────┐
│ OpenMetadata │    │ Prometheus + Grafana │
│ catalog +    │    │ pipeline SLAs        │
│ lineage      │    │ data freshness       │
│ :8585        │    │ :9090 / :3001        │
└──────────────┘    └─────────────────────┘
```

---

## Technology Stack

| Layer | Technology | Port |
|---|---|---|
| Data generation | DE-Stock (Rust exchange simulator) | WebSocket :8080 |
| Message broker | Apache Kafka (KRaft, single broker) | :9092 |
| Kafka UI | Kafka-UI | :8081 |
| Stream processing | Apache Flink | :8082 (UI) |
| Object storage | MinIO (S3-compatible) | :9000 / :9001 |
| Table format | Apache Iceberg + REST catalog | :8183 |
| In-memory analytics | DuckDB | (embedded) |
| Orchestration | Dagster | :3000 |
| Data quality | Great Expectations 0.18 | (embedded) |
| API layer | GraphQL / Strawberry + FastAPI | :8000 |
| Visualization | Apache Superset 3.1 | :8088 |
| Governance | OpenMetadata | :8585 |
| Metrics | Prometheus | :9090 |
| Dashboards | Grafana | :3001 |

---

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Docker | ≥ 24 | Colima on macOS: `colima start --memory 8 --cpu 4` |
| docker-compose | ≥ 2.24 | Standalone (not the `docker compose` plugin) |
| Python | 3.11 | For all services |
| Make | any | GNU make |

---

## Quick Start

```bash
# 1. Clone
git clone https://github.com/phudekar/DE-capstone-project
cd DE-project

# 2. Copy environment file
cp infrastructure/docker-compose/.env.example infrastructure/docker-compose/.env

# 3. Start infrastructure (Kafka + MinIO + Iceberg)
make up-infra

# 4. Register Avro schemas
make register-schemas

# 5. Initialise Iceberg tables + seed dimension data
make init-lakehouse

# 6. Start the full application stack
make up

# 7. Run the Dagster orchestrator separately
make up-dagster

# 8. (Optional) Start Flink — requires >= 4 GB Docker memory
make up-flink
make submit-all-flink
```

### Service URLs (after `make up`)

| Service | URL |
|---|---|
| DE-Stock WebSocket | ws://localhost:8080/ws |
| Kafka UI | http://localhost:8081 |
| Flink Web UI | http://localhost:8082 |
| MinIO Console | http://localhost:9001 (minioadmin / minioadmin) |
| GraphQL Playground | http://localhost:8000/graphql |
| Dagster UI | http://localhost:3000 |
| Superset | http://localhost:8088 (admin / admin) |
| OpenMetadata | http://localhost:8585 |
| Grafana | http://localhost:3001 (admin / admin) |
| Prometheus | http://localhost:9090 |

---

## Running Tests

```bash
# All unit tests (no Docker required)
make test-unit

# End-to-end pipeline tests (no Docker required)
make test-e2e

# All tests
make test

# Superset saved-query tests
make test-superset

# Lint
make lint

# Auto-format
make format
```

### Test Summary

| Suite | Tests | Requirements |
|---|---|---|
| `libs/common` | 12 | Python 3.11 |
| `services/kafka-bridge` | 25 | Python 3.11 |
| `services/lakehouse` | 28 | Python 3.11 |
| `services/flink-processor` | 30 | Python 3.11 |
| `services/graphql-api` | ~20 | Python 3.11 |
| `services/data-quality` | 40 | Python 3.11 |
| `services/dagster-orchestrator` | 70 | Python 3.11 |
| `services/superset` | 56 | Python 3.11 + duckdb |
| `tests/e2e` | **206** | Python 3.11, no Docker |

> The e2e suite drives raw WebSocket events through every stage (Kafka Bridge →
> Bronze → Silver → Gold → GraphQL → Superset) using in-memory DuckDB — no live
> services needed.

---

## Repository Structure

```
DE-project/
├── .github/workflows/          # CI (lint + test) and CD (Docker build + push)
├── infrastructure/
│   ├── docker-compose/         # All docker-compose stack files
│   └── scripts/                # Schema registration, setup helpers
├── libs/
│   └── common/                 # Shared Pydantic models + utilities (de-common)
├── services/
│   ├── de-stock/               # Rust exchange simulator (binary: de-stock)
│   ├── kafka-bridge/           # WebSocket → Kafka router
│   ├── flink-processor/        # Apache Flink stream jobs (Python)
│   ├── lakehouse/              # Iceberg medallion (Bronze / Silver / Gold)
│   ├── dagster-orchestrator/   # Dagster assets, sensors, schedules
│   ├── data-quality/           # Great Expectations suites + custom expectations
│   ├── graphql-api/            # Strawberry + FastAPI GraphQL endpoint
│   └── superset/               # Superset config, dashboards, saved queries
├── tests/
│   └── e2e/                    # End-to-end pipeline test suite (206 tests)
├── scripts/                    # Health checks, teardown helpers
├── plan/                       # 14 detailed implementation plan documents
├── Makefile                    # All common operations (make help for full list)
├── pyproject.toml              # Root Python workspace config (ruff, pytest)
└── .pre-commit-config.yaml     # Pre-commit hooks (ruff, secrets, shellcheck)
```

---

## Data Flow Detail

```
DE-Stock (Rust)
  └─[WebSocket]──▶ Kafka Bridge
                      ├─ validate (Pydantic discriminated union, 9 event types)
                      ├─ route by event_type → Kafka topic
                      └─[Kafka produce]──▶ raw.trades / raw.orderbook-snapshots / …

Kafka
  └─[Consume]──▶ Lakehouse Bronze writer
                  └──▶ Iceberg bronze_trades, bronze_orderbook

Dagster (batch, scheduled)
  ├─ bronze_ingestor asset
  ├─ silver_processor asset  ──▶ Iceberg silver_trades, silver_orderbook
  │     • dedup by trade_id
  │     • enrich from dim_symbol (company_name, sector)
  │     • spread / mid_price for orderbook
  └─ gold_aggregator asset   ──▶ Iceberg gold_daily_summary
        • OHLCV per symbol per date
        • VWAP = Σ(price×qty) / Σ(qty)

GraphQL API (FastAPI + Strawberry)
  └─[DuckDB on Iceberg]──▶ trades, dailySummary, orderBook, marketOverview queries

Apache Superset
  └─[DuckDB connector]──▶ 6 saved queries, 6 dashboards
```

---

## Dashboards

| Dashboard | Description |
|---|---|
| Executive Market Overview | KPIs, volume by symbol, sector pie, advance/decline |
| Symbol Deep Dive | OHLCV candlestick, SMA-20/50, RSI-14 |
| Trader Analytics | Top traders, buy/sell ratio, activity heatmap |
| Pipeline Health | Ingest lag, Dagster runs, Iceberg layer row counts |
| Risk & Compliance | Volume z-scores, wash-trade detection, sector concentration |
| Order Book Depth | Custom ECharts depth chart plugin |

Dashboard previews are in [services/superset/docs/snapshots/](services/superset/docs/snapshots/).

---

## CI/CD

### CI (`.github/workflows/ci.yml`)
Triggers on every push and PR to `main`.

| Job | What it checks |
|---|---|
| `lint` | `ruff format --check` + `ruff check` |
| `test-common` | libs/common unit tests |
| `test-kafka-bridge` | Kafka bridge unit tests |
| `test-lakehouse` | Lakehouse medallion tests |
| `test-flink` | Flink processor tests |
| `test-graphql-api` | GraphQL resolver tests |
| `test-data-quality` | Great Expectations suite tests |
| `test-dagster` | Dagster asset definition tests (Python 3.11) |
| `test-superset` | Superset saved-query tests |
| `test-e2e` | Full pipeline e2e (needs: common + bridge + lakehouse) |
| `ci-gate` | Requires all jobs to pass before merge |

### CD (`.github/workflows/cd.yml`)
Triggers on push to `main`. Builds and pushes Docker images for all 6 services to
GitHub Container Registry (`ghcr.io`) with `sha-*` and `latest` tags.

---

## Pre-commit Hooks

Install once:
```bash
pip install pre-commit
make pre-commit-install
```

Hooks run on `git commit`:
- **ruff** — lint + auto-fix
- **ruff-format** — code formatting
- **trailing-whitespace**, **end-of-file-fixer**, **mixed-line-ending**
- **check-yaml**, **check-toml**, **check-json**
- **detect-secrets** — prevents accidental credential commits
- **shellcheck** — shell script linting

---

## Implementation Phases

| # | Phase | Status |
|---|---|---|
| 1 | Infrastructure (Docker Compose, Kafka, MinIO, Iceberg) | ✅ Done |
| 2 | Data ingestion (DE-Stock + Kafka Bridge) | ✅ Done |
| 3 | Stream processing (Apache Flink) | ✅ Done |
| 4 | Lakehouse storage (Iceberg medallion + DuckDB) | ✅ Done |
| 5 | Orchestration (Dagster) | ✅ Done |
| 6 | Data quality (Great Expectations) | ✅ Done |
| 7 | API layer (GraphQL / Strawberry + FastAPI) | ✅ Done |
| 8 | Monitoring (Prometheus + Grafana) | ✅ Done |
| 9 | Governance (OpenMetadata) | ✅ Done |
| 10 | Visualization (Apache Superset) | ✅ Done |
| 11 | CI/CD (GitHub Actions + pre-commit) | ✅ Done |
| 12 | Performance optimization | 🔄 In progress |

---

## Teardown

```bash
make teardown           # Stop containers, preserve data volumes
make teardown-destroy   # Stop containers AND destroy all data
```

Per-service teardown scripts are in `scripts/`.

---

## Plans

Detailed implementation documents in `plan/`:

| Document | Description |
|---|---|
| [00-master-plan](plan/00-master-plan.md) | Architecture overview & phase roadmap |
| [01-infrastructure](plan/01-infrastructure-and-devops.md) | Docker, Terraform, CI/CD |
| [02-data-sources](plan/02-data-sources-and-ingestion.md) | DE-Stock, Kafka bridge, topics |
| [03-processing](plan/03-stream-processing-and-compute.md) | Flink stream jobs |
| [04-storage](plan/04-storage-and-data-modeling.md) | Iceberg, SCD, partitioning |
| [05-orchestration](plan/05-orchestration-and-workflow.md) | Dagster assets & schedules |
| [06-quality](plan/06-data-quality-and-validation.md) | Great Expectations suites |
| [07-governance](plan/07-data-governance-and-metadata.md) | OpenMetadata lineage & RBAC |
| [08-monitoring](plan/08-observability-and-monitoring.md) | Prometheus + Grafana alerting |
| [09-api](plan/09-api-and-serving-layer.md) | GraphQL schema & resolvers |
| [10-visualization](plan/10-visualization-and-reporting.md) | Superset dashboards |
| [11-scalability](plan/11-scalability-and-performance.md) | Performance tuning |
| [12-teardown](plan/12-teardown-scripts.md) | Teardown & reset scripts |
