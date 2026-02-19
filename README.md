# DE Capstone Project

Real-time stock market data pipeline demonstrating modern data engineering practices.

## Architecture

```
DE-Stock (Rust)  →  WebSocket  →  Kafka Bridge (Python)  →  Kafka (KRaft)
                                                              ↓
                                                         Flink (PyFlink)
                                                              ↓
                                                     MinIO / Iceberg (Parquet)
                                                              ↓
                                                      Dagster → Superset
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Source | [DE-Stock](https://github.com/dileepbapat/mock-stock) — Rust exchange simulator |
| Streaming | Apache Kafka (KRaft, 3 brokers) + Schema Registry |
| Processing | Apache Flink (PyFlink) |
| Storage | Apache Iceberg + Parquet on MinIO (S3-compatible) |
| Orchestration | Dagster |
| Data Quality | Great Expectations |
| Governance | OpenMetadata |
| Monitoring | Prometheus + Grafana |
| API | Strawberry GraphQL + FastAPI |
| Visualization | Apache Superset |

## Prerequisites

- Docker Desktop or Colima (8 GB RAM recommended)
- Python 3.11+
- Make

## Quick Start

```bash
# Start all infrastructure (Kafka, MinIO/Iceberg, Flink)
make up-infra

# Start the DE-Stock simulator
make up-services

# Start everything
make up

# Check health
make health

# View logs
make logs

# Stop everything (preserves data)
make down

# Destroy everything including data
make teardown-destroy
```

## Services & Ports

| Service | Port | URL |
|---------|------|-----|
| DE-Stock WebSocket | 8765 | `ws://localhost:8765` |
| Kafka Broker 1 | 9092 | — |
| Kafka Broker 2 | 9093 | — |
| Kafka Broker 3 | 9094 | — |
| Schema Registry | 8081 | `http://localhost:8081` |
| Kafka UI | 8080 | `http://localhost:8080` |
| MinIO S3 API | 9000 | `http://localhost:9000` |
| MinIO Console | 9001 | `http://localhost:9001` |
| Iceberg REST Catalog | 8181 | `http://localhost:8181` |
| Flink Web UI | 8082 | `http://localhost:8082` |

## Project Structure

```
DE-project/
├── plan/                          # Implementation plans
├── libs/common/                   # Shared Pydantic models & schemas
├── services/
│   ├── de-stock/                  # Exchange simulator (Rust, Dockerized)
│   └── kafka-bridge/              # WebSocket → Kafka bridge (Python)
├── infrastructure/
│   └── docker-compose/            # Modular compose files
├── scripts/                       # Teardown & health check scripts
├── tests/                         # Integration & load tests
├── Makefile                       # All operational targets
└── README.md
```

## Plans

Detailed implementation plans are in `plan/`:

| Plan | Description |
|------|-------------|
| [00-master-plan](plan/00-master-plan.md) | Architecture overview & phase roadmap |
| [01-infrastructure](plan/01-infrastructure-and-devops.md) | Docker, K8s, CI/CD |
| [02-data-sources](plan/02-data-sources-and-ingestion.md) | DE-Stock, Kafka bridge, topics |
| [03-processing](plan/03-stream-processing-and-compute.md) | Flink, DuckDB |
| [04-storage](plan/04-storage-and-data-modeling.md) | Iceberg, medallion architecture |
| [05-orchestration](plan/05-orchestration-and-workflow.md) | Dagster |
| [06-quality](plan/06-data-quality-and-validation.md) | Great Expectations |
| [07-governance](plan/07-data-governance-and-metadata.md) | OpenMetadata |
| [08-monitoring](plan/08-observability-and-monitoring.md) | Prometheus, Grafana |
| [09-api](plan/09-api-and-serving-layer.md) | GraphQL, FastAPI |
| [10-visualization](plan/10-visualization-and-reporting.md) | Superset |
| [11-scalability](plan/11-scalability-and-performance.md) | Performance tuning |
| [12-teardown](plan/12-teardown-scripts.md) | Teardown scripts |
