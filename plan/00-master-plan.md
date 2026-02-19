# Data Engineering Capstone Project -- Master Plan

> **Version**: 1.0
> **Last Updated**: 2026-02-18
> **Repository**: `DE-project` (monorepo)

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture Diagram](#2-architecture-diagram)
3. [Technology Stack](#3-technology-stack)
4. [Monorepo Structure](#4-monorepo-structure)
5. [Implementation Phases](#5-implementation-phases)
6. [Cross-Cutting Concerns](#6-cross-cutting-concerns)
7. [Component Dependency Map](#7-component-dependency-map)
8. [Individual Plan References](#8-individual-plan-references)

---

## 1. Project Overview

This project implements a **production-grade stock market trade data pipeline** as a
comprehensive Data Engineering capstone. The system generates synthetic trade/order data,
streams it through a real-time pipeline, processes it with both streaming and in-memory
engines, persists it in a lakehouse format, and exposes it through APIs and dashboards
for business analytics.

### Core Data Flow

A Python console application generates realistic stock market trade events and publishes
them over WebSockets. A Kafka consumer service bridges the WebSocket stream into Apache
Kafka topics. Apache Flink consumes from Kafka, applies stream processing logic (enrichment,
aggregation, anomaly detection, windowed computations), and writes results to downstream
Kafka topics. Processed data lands in Apache Iceberg tables (Parquet format) for durable,
versioned storage. Dagster orchestrates batch transformations, backfills, and scheduling.
Data quality is enforced via Great Expectations. A GraphQL API exposes curated datasets,
and Apache Superset provides interactive dashboards for business stakeholders.

### Key Objectives

- Demonstrate end-to-end data engineering across ingestion, processing, storage, serving,
  and governance.
- Apply real-time (Flink) and batch (Dagster + DuckDB) processing patterns.
- Implement data quality, lineage tracking, and observability as first-class concerns.
- Follow Infrastructure as Code -- the entire stack is reproducible from version-controlled
  scripts.
- Organize all components in a monorepo with clear ownership boundaries.

---

## 2. Architecture Diagram

```
                          DATA ENGINEERING CAPSTONE -- SYSTEM ARCHITECTURE
 ============================================================================================================

  DATA GENERATION            INGESTION              STREAM PROCESSING           STORAGE & MODELING
 +------------------+    +----------------+    +---------------------------+    +---------------------+
 |                  |    |                |    |                           |    |                     |
 | Trade Generator  | WS | Kafka Consumer | -> |      Apache Kafka        | -> |   Apache Iceberg    |
 | (Python Console) |--->| (WS -> Kafka   |    |  (raw-trades topic)      |    |   (Parquet files)   |
 |                  |    |  Bridge)       |    |                           |    |                     |
 | - Stocks/Options |    +----------------+    |          |                |    |  - Trade facts      |
 | - Orders/Trades  |                          |          v                |    |  - Instrument dims  |
 | - Market data    |                          |   +-------------+        |    |  - SCD Type 1,2,3   |
 +------------------+                          |   | Apache Flink|        |    |  - Partitioned by   |
                                               |   | (Stream     |        |    |    date/symbol      |
                                               |   |  Processing)|        |    +---------------------+
                                               |   +-------------+        |              |
                                               |          |               |              |
                                               |          v               |              v
                                               |   +-------------+       |    +---------------------+
                                               |   | Processed   |       |    |      DuckDB         |
                                               |   | Kafka Topics|       |    | (In-Memory Analytics)|
                                               |   | - enriched  |       |    +---------------------+
                                               |   | - aggregated|       |
                                               |   | - alerts    |       |
                                               |   +-------------+       |
                                               +---------------------------+

  ORCHESTRATION              DATA QUALITY          API & SERVING             VISUALIZATION
 +------------------+    +------------------+    +------------------+    +---------------------+
 |                  |    |                  |    |                  |    |                     |
 |     Dagster      |    | Great            |    |   GraphQL API    |    |  Apache Superset    |
 | - Pipelines      |--->| Expectations     |    |  (Strawberry)    |--->|  - Trade dashboards |
 | - Schedules      |    | - Completeness   |    |  - Flexible      |    |  - P&L reports      |
 | - Sensors        |    | - Accuracy       |    |    querying      |    |  - Market analysis  |
 | - Backfills      |    | - Consistency    |    |  - Subscriptions |    |  - Risk metrics     |
 +------------------+    +------------------+    +------------------+    +---------------------+

  GOVERNANCE                 MONITORING             INFRASTRUCTURE
 +------------------+    +------------------+    +---------------------------+
 |                  |    |                  |    |                           |
 |  OpenMetadata    |    |  Prometheus +    |    |  Docker Compose (local)   |
 |  - Data catalog  |    |  Grafana         |    |  Terraform (IaC)          |
 |  - Lineage       |    |  - Pipeline SLAs |    |  Colima (local K8s)       |
 |  - Access control|    |  - Data freshness|    |  GitHub Actions (CI/CD)   |
 |  - PII handling  |    |  - Alerting      |    |                           |
 +------------------+    +------------------+    +---------------------------+

 ============================================================================================================
  FLOW SUMMARY:
  Trade Generator --[WebSocket]--> Kafka Consumer --[Produce]--> Kafka --[Consume]--> Flink
  Flink --[Produce]--> Kafka (processed) --[Sink]--> Iceberg/Parquet
  Dagster orchestrates batch loads from Iceberg, triggers Great Expectations checks
  GraphQL API reads from Iceberg/DuckDB, serves Superset and external clients
 ============================================================================================================
```

---

## 3. Technology Stack

| Layer | Technology | Justification |
|---|---|---|
| **Data Generation** | Python console app + WebSocket server | Lightweight, flexible synthetic data generation. WebSockets provide a realistic real-time streaming interface similar to market data feeds. |
| **Message Broker** | Apache Kafka (Docker cluster) | Industry standard for real-time event streaming. Durable, partitioned, replayable. Decouples producers from consumers. Enables multiple downstream consumers from a single topic. |
| **Stream Processing** | Apache Flink | True stream processing engine with event-time semantics, windowing, and exactly-once guarantees. Native Kafka integration. Suitable for low-latency enrichment, aggregation, and alerting. |
| **In-Memory Analytics** | DuckDB | Embeddable OLAP engine with excellent Parquet/Iceberg support. Ideal for fast analytical queries without a separate cluster. Complements Flink for ad-hoc and batch analytics. |
| **Storage Format** | Apache Iceberg (Parquet) | Open table format with ACID transactions, schema evolution, time travel, and partition evolution. Parquet provides columnar compression. Together they form a local lakehouse. |
| **Orchestration** | Dagster | Modern orchestration framework with software-defined assets, first-class typing, built-in observability UI, and native integrations for data quality. Supports scheduling, sensors, backfills, and retry logic. |
| **Data Quality** | Great Expectations | Mature, expressive data validation library. Integrates with Dagster via `dagster-ge`. Supports expectation suites for completeness, accuracy, consistency, timeliness, and uniqueness checks. |
| **Data Governance** | OpenMetadata | Modern, open-source data catalog with REST API. Supports data lineage (column-level), access control (RBAC), data profiling, and PII detection. Active community and clean UI. |
| **Monitoring** | Prometheus + Grafana | De facto standard for metrics collection and visualization. Prometheus scrapes custom pipeline metrics; Grafana provides dashboards and alerting for freshness SLAs, volume anomalies, and schema drift. |
| **API Layer** | GraphQL (Strawberry) | Flexible query interface -- clients request exactly the fields they need. Strawberry is a modern, type-safe Python GraphQL library with async support and excellent developer experience. |
| **Visualization** | Apache Superset | Open-source BI platform comparable to Tableau. Supports SQL-based exploration, rich chart types, and shareable dashboards. Connects to DuckDB/Iceberg for direct querying. |
| **Infrastructure as Code** | Terraform + Docker Compose + Colima | Terraform manages declarative resource definitions. Docker Compose handles local multi-service orchestration. Colima provides a lightweight local Kubernetes environment on macOS. |
| **CI/CD** | GitHub Actions (local runner) | Native GitHub integration for automated testing, linting, and deployment. Local self-hosted runner enables pipeline execution against local infrastructure. |
| **Primary Language** | Python, SQL | Python is the lingua franca of data engineering with the richest ecosystem. SQL is used for transformations, analytics, and Iceberg table definitions. |

---

## 4. Monorepo Structure

```
DE-project/
├── plan/                               # Planning & design documents
│   ├── 00-master-plan.md               #   This file -- project overview & roadmap
│   ├── 01-infrastructure.md            #   Infrastructure foundation plan
│   ├── 02-data-generation.md           #   Trade generator & ingestion plan
│   ├── 03-stream-processing.md         #   Flink processing plan
│   ├── 04-storage-modeling.md          #   Iceberg storage & data modeling plan
│   ├── 05-orchestration.md             #   Dagster orchestration plan
│   ├── 06-data-quality.md              #   Great Expectations integration plan
│   ├── 07-api-serving.md               #   GraphQL API plan
│   ├── 08-monitoring.md                #   Prometheus + Grafana plan
│   ├── 09-governance.md                #   OpenMetadata governance plan
│   ├── 10-visualization.md             #   Superset dashboard plan
│   ├── 11-cicd-devops.md               #   CI/CD & DevOps plan
│   ├── 12-performance.md               #   Performance optimization plan
│   └── 12-teardown-scripts.md          #   Global & granular teardown scripts
│
├── services/                           # Application services (each independently deployable)
│   ├── trade-generator/                #   WebSocket trade data generator
│   │   ├── src/                        #     Source code
│   │   ├── tests/                      #     Unit tests
│   │   ├── Dockerfile                  #     Container definition
│   │   ├── pyproject.toml              #     Python project config
│   │   └── README.md
│   │
│   ├── kafka-consumer/                 #   WebSocket-to-Kafka bridge service
│   │   ├── src/                        #     Consumer logic, Kafka producer
│   │   ├── tests/
│   │   ├── Dockerfile
│   │   └── pyproject.toml
│   │
│   ├── flink-processor/                #   Flink stream processing jobs
│   │   ├── src/                        #     Flink job definitions
│   │   │   ├── jobs/                   #       Individual job modules
│   │   │   └── operators/              #       Custom Flink operators
│   │   ├── tests/
│   │   ├── Dockerfile
│   │   └── pyproject.toml
│   │
│   ├── dagster-orchestrator/           #   Dagster pipelines & schedules
│   │   ├── src/
│   │   │   ├── assets/                 #       Software-defined assets
│   │   │   ├── jobs/                   #       Job definitions
│   │   │   ├── schedules/              #       Schedule definitions
│   │   │   ├── sensors/                #       Event sensors
│   │   │   └── resources/              #       Resource configurations
│   │   ├── tests/
│   │   ├── Dockerfile
│   │   └── pyproject.toml
│   │
│   ├── data-quality/                   #   Great Expectations suites
│   │   ├── great_expectations/         #     GE project root
│   │   │   ├── expectations/           #       Expectation suite JSON files
│   │   │   ├── checkpoints/            #       Checkpoint definitions
│   │   │   └── plugins/               #       Custom expectations
│   │   ├── tests/
│   │   └── pyproject.toml
│   │
│   ├── graphql-api/                    #   GraphQL serving layer
│   │   ├── src/
│   │   │   ├── schema/                 #       GraphQL type definitions
│   │   │   ├── resolvers/              #       Query & mutation resolvers
│   │   │   └── dataloaders/            #       Batched data loading
│   │   ├── tests/
│   │   ├── Dockerfile
│   │   └── pyproject.toml
│   │
│   └── superset/                       #   Superset configuration & customization
│       ├── dashboards/                 #     Exported dashboard JSON
│       ├── datasets/                   #     Dataset definitions
│       ├── superset_config.py          #     Superset settings
│       └── Dockerfile
│
├── infrastructure/                     # Infrastructure as Code
│   ├── docker/                         #   Dockerfiles (shared base images)
│   ├── docker-compose/                 #   Compose files for local development
│   │   ├── docker-compose.core.yml     #     Kafka, Zookeeper, Flink
│   │   ├── docker-compose.storage.yml  #     Iceberg, MinIO (S3-compatible)
│   │   ├── docker-compose.tools.yml    #     Dagster, Superset, OpenMetadata
│   │   ├── docker-compose.monitoring.yml  # Prometheus, Grafana
│   │   └── docker-compose.override.yml #     Local developer overrides
│   ├── terraform/                      #   Terraform modules
│   │   ├── modules/                    #     Reusable modules
│   │   └── environments/              #     Environment-specific configs
│   ├── k8s/                            #   Kubernetes manifests
│   │   ├── base/                       #     Base Kustomize resources
│   │   └── overlays/                   #     Environment overlays
│   └── scripts/                        #   Setup & utility scripts
│       ├── _teardown-common.sh         #     Shared teardown helpers (colors, logging, volume ops)
│       ├── setup.sh                    #     One-command local setup
│       ├── teardown.sh                 #     Global teardown (calls all module scripts)
│       ├── teardown-kafka.sh           #     Kafka cluster teardown
│       ├── teardown-flink.sh           #     Flink teardown (with savepoint)
│       ├── teardown-storage.sh         #     MinIO + Iceberg teardown
│       ├── teardown-dagster.sh         #     Dagster teardown
│       ├── teardown-monitoring.sh      #     Prometheus + Grafana teardown
│       ├── teardown-governance.sh      #     OpenMetadata teardown
│       ├── teardown-services.sh        #     App services teardown
│       ├── teardown-superset.sh        #     Superset teardown
│       ├── teardown-k8s.sh             #     Kubernetes namespace teardown
│       ├── teardown-terraform.sh       #     Terraform destroy
│       ├── wipe-data.sh                #     Data-only wipe (per layer / Kafka / Dagster)
│       ├── health-check.sh             #     Service health verification
│       ├── seed-data.sh                #     Seed initial/reference data
│       ├── backup.sh                   #     Backup all persistent data
│       └── restore.sh                  #     Restore from backup
│
├── libs/                               # Shared libraries
│   ├── common/                         #   Shared utilities
│   │   ├── config/                     #     Configuration management
│   │   ├── logging/                    #     Structured logging
│   │   ├── schemas/                    #     Avro/JSON schemas for Kafka
│   │   └── utils/                      #     Helper functions
│   └── data-models/                    #   Data model definitions
│       ├── iceberg/                    #     Iceberg table schemas
│       ├── scd/                        #     SCD Type 1, 2, 3 models
│       └── dimensions/                 #     Dimension table definitions
│
├── tests/                              # Cross-service tests
│   ├── unit/                           #   Unit tests (per-module mirrors)
│   ├── integration/                    #   Integration tests (multi-service)
│   └── e2e/                            #   End-to-end pipeline tests
│
├── monitoring/                         # Observability configuration
│   ├── prometheus/                     #   Prometheus scrape configs & rules
│   │   └── prometheus.yml
│   ├── grafana/                        #   Grafana dashboard JSON & provisioning
│   │   ├── dashboards/
│   │   └── provisioning/
│   └── alerts/                         #   Alert rules (Prometheus alertmanager)
│       └── alert-rules.yml
│
├── governance/                         # Data governance
│   └── openmetadata/                   #   OpenMetadata configuration
│       ├── ingestion/                  #     Metadata ingestion connectors
│       └── policies/                   #     Access & classification policies
│
├── docs/                               # Project documentation
│   ├── adr/                            #   Architecture Decision Records
│   ├── runbooks/                       #   Operational runbooks
│   └── diagrams/                       #   Architecture & flow diagrams
│
├── .github/                            # GitHub configuration
│   └── workflows/                      #   CI/CD workflow definitions
│       ├── ci.yml                      #     Lint, test, build
│       ├── cd.yml                      #     Deploy pipeline
│       └── data-quality.yml            #     Scheduled quality checks
│
├── Makefile                            # Common commands (build, test, up, down)
├── pyproject.toml                      # Root Python project (workspace config)
└── README.md                           # Project overview & getting started
```

---

## 5. Implementation Phases

Each phase builds on the previous ones. Within each phase, tasks are ordered by dependency.

### Phase 1: Infrastructure Foundation
**Goal**: Establish the base environment so all subsequent services can be developed and tested locally.

| Item | Description |
|---|---|
| Docker Compose core stack | Kafka (3 brokers), Zookeeper, Flink (JobManager + TaskManager), networking |
| Storage layer | MinIO (S3-compatible) for Iceberg warehouse, Iceberg REST catalog |
| Developer tooling | Makefile targets (`make up`, `make down`, `make logs`), local Python workspace |
| Terraform scaffolding | Module structure, provider configuration, state management |
| Colima setup | Local Kubernetes cluster configuration for container orchestration |

**Deliverable**: `make up` brings up Kafka + Flink + Iceberg + MinIO. All services are accessible on localhost.

---

### Phase 2: Data Generation & Ingestion
**Goal**: Produce realistic trade data and land it in Kafka.

| Item | Description |
|---|---|
| Trade Generator | Python console app generating synthetic stock trades (symbol, price, quantity, timestamp, order type, exchange). Configurable rate and distribution. |
| WebSocket server | Embedded in the trade generator; publishes events on a WebSocket endpoint. |
| Kafka Consumer (bridge) | Connects to the WebSocket, deserializes events, and produces to `raw-trades` Kafka topic with Avro/JSON schema. |
| Schema registry | Schema definitions in `libs/common/schemas/` for trade events. |
| Shared data models | Pydantic models in `libs/common/` for trade, order, and instrument entities. |

**Deliverable**: Trade events flow continuously from the generator through WebSocket into the `raw-trades` Kafka topic.

---

### Phase 3: Stream Processing
**Goal**: Process raw trade events in real time with Apache Flink.

| Item | Description |
|---|---|
| Flink job: Trade enrichment | Enrich raw trades with instrument metadata (sector, market cap tier). |
| Flink job: Windowed aggregations | Tumbling and sliding windows for VWAP, volume by symbol, trade count per minute. |
| Flink job: Anomaly detection | Flag trades with unusual price movement or volume spikes. |
| Topic routing | Produce to `enriched-trades`, `trade-aggregations`, `trade-alerts` topics. |
| State management | Configure Flink checkpointing and savepoints for fault tolerance. |

**Deliverable**: Three downstream Kafka topics receive processed data in real time.

---

### Phase 4: Storage & Data Modeling
**Goal**: Persist processed data in Iceberg tables with proper dimensional modeling.

| Item | Description |
|---|---|
| Iceberg table definitions | `fact_trades`, `dim_instruments`, `dim_exchanges`, `dim_time`. |
| SCD implementations | SCD Type 1 (overwrite) for exchange info, Type 2 (history) for instrument attributes, Type 3 (previous value) for price targets. |
| Kafka-to-Iceberg sink | Flink sink or dedicated consumer writing to Iceberg tables. |
| Partitioning strategy | Partition by trade date and symbol hash bucket for balanced query performance. |
| DuckDB integration | Query layer over Iceberg/Parquet files for fast analytics. |

**Deliverable**: Trade data is durably stored in Iceberg with time travel, schema evolution, and efficient partitioning.

---

### Phase 5: Orchestration
**Goal**: Automate batch data processing, transformations, and scheduling with Dagster.

| Item | Description |
|---|---|
| Software-defined assets | Define assets for each Iceberg table, aggregation, and derived dataset. |
| Jobs & schedules | Hourly aggregation job, daily snapshot job, weekly compaction job. |
| Sensors | Kafka topic sensor to trigger processing on new data arrival. |
| Backfill support | Enable historical data reprocessing with Dagster partition definitions. |
| Retry & idempotency | Configure retry policies; ensure all operations are idempotent. |
| Dagster UI | Deploy Dagit for pipeline visibility and manual triggering. |

**Deliverable**: All batch processing runs on schedule with full observability in Dagster UI.

---

### Phase 6: Data Quality
**Goal**: Validate data at every stage using Great Expectations integrated with Dagster.

| Item | Description |
|---|---|
| Expectation suites | Suites for raw trades (schema, nulls, ranges), enriched trades (referential integrity), and aggregations (statistical bounds). |
| Dagster integration | Use `dagster-ge` to run expectations as part of asset materialization. |
| Completeness checks | Null checks, missing value detection, required field validation. |
| Accuracy checks | Business rule validation (price > 0, quantity > 0, valid symbols). |
| Consistency checks | Cross-topic validation (enriched count matches raw count). |
| Timeliness checks | Freshness SLAs -- alert if data is older than threshold. |
| Uniqueness checks | Deduplication validation on trade IDs. |
| Data docs | Auto-generated validation reports hosted locally. |

**Deliverable**: Every pipeline run produces a data quality report. Failures trigger alerts and block downstream processing.

---

### Phase 7: API & Serving Layer
**Goal**: Expose trade data and analytics through a GraphQL API.

| Item | Description |
|---|---|
| GraphQL schema | Types for Trade, Instrument, Aggregation, Alert. |
| Query resolvers | Queries for trades by symbol/date range, aggregations, alerts. |
| Subscriptions | Real-time trade and alert subscriptions via WebSocket. |
| DataLoaders | Batched and cached data loading to avoid N+1 queries. |
| DuckDB backend | Query Iceberg tables through DuckDB for fast response times. |
| Authentication | API key or JWT-based access control. |

**Deliverable**: A running GraphQL endpoint at `localhost:8000/graphql` with playground and documentation.

---

### Phase 8: Monitoring & Observability
**Goal**: Full-stack monitoring of pipeline health, data freshness, and system metrics.

| Item | Description |
|---|---|
| Prometheus config | Scrape targets for Kafka, Flink, Dagster, and custom app metrics. |
| Custom metrics | Pipeline latency, records processed/sec, error rates, queue depth. |
| Grafana dashboards | Pipeline health dashboard, data freshness dashboard, Kafka consumer lag dashboard. |
| Alerting rules | Alerts for SLA breach, consumer lag > threshold, Flink checkpoint failure, zero throughput. |
| Structured logging | JSON-formatted logs across all services with correlation IDs. |

**Deliverable**: Grafana dashboards showing real-time pipeline health. Alerts fire on anomalous conditions.

---

### Phase 9: Data Governance
**Goal**: Catalog all data assets, track lineage, and enforce access policies.

| Item | Description |
|---|---|
| OpenMetadata deployment | Containerized OpenMetadata instance with REST API. |
| Metadata ingestion | Connectors for Kafka topics, Iceberg tables, Dagster assets. |
| Data lineage | End-to-end lineage from trade generator to Superset dashboards. Column-level lineage for transformations. |
| Classification | Tag PII fields (if any), classify data sensitivity levels. |
| Access policies | RBAC definitions for data consumers. |

**Deliverable**: Full data catalog with lineage visualization accessible via OpenMetadata UI.

---

### Phase 10: Visualization
**Goal**: Build interactive dashboards for business stakeholders.

| Item | Description |
|---|---|
| Superset deployment | Containerized Superset connected to DuckDB/Iceberg. |
| Dashboard: Trade Overview | Real-time trade volume, top symbols, market activity heatmap. |
| Dashboard: P&L Analysis | Profit/loss by symbol, portfolio performance, position tracking. |
| Dashboard: Market Analytics | VWAP trends, volume profiles, sector rotation analysis. |
| Dashboard: Risk Metrics | Anomaly alerts, unusual activity, concentration risk. |
| Sharing | Export and embed capabilities for stakeholder distribution. |

**Deliverable**: Four interactive dashboards accessible at `localhost:8088` with drill-down capabilities.

---

### Phase 11: CI/CD & DevOps
**Goal**: Automated testing, building, and deployment for all pipeline components.

| Item | Description |
|---|---|
| GitHub Actions workflows | CI: lint (ruff), type-check (mypy), unit tests, integration tests. CD: build images, deploy to local K8s. |
| Local runner | Self-hosted GitHub Actions runner for executing against local infrastructure. |
| Test strategy | Unit tests per service, integration tests for service pairs, E2E tests for full pipeline. |
| Environment promotion | dev -> staging -> prod configuration management. |
| Blue-green deployment | Zero-downtime deployment strategy for pipeline updates. |
| Pre-commit hooks | Enforce code formatting, linting, and secret scanning before commits. |

**Deliverable**: Every push triggers CI. Merges to main trigger CD to local environment.

---

### Phase 12: Performance Optimization
**Goal**: Tune the pipeline for throughput, latency, and query performance.

| Item | Description |
|---|---|
| Kafka tuning | Partition count optimization, batch size, compression (lz4/zstd), consumer group balancing. |
| Flink tuning | Parallelism, state backend (RocksDB), checkpoint interval, watermark strategy. |
| Iceberg optimization | Partition pruning, file compaction, sort order optimization, manifest caching. |
| Query optimization | Predicate pushdown, partition elimination, DuckDB query plans, materialized views. |
| Caching | API response caching, DuckDB result caching, metadata caching. |
| Benchmarking | Load tests for throughput (trades/sec), query latency benchmarks, resource utilization profiling. |

**Deliverable**: Documented performance baselines and optimization results. Pipeline handles target throughput with acceptable latency.

---

## 6. Cross-Cutting Concerns

These concerns span all components and should be addressed consistently from Phase 1 onward.

### 6.1 Logging

- **Standard**: Structured JSON logging across all Python services.
- **Library**: `structlog` for consistent, machine-parseable log output.
- **Fields**: timestamp, service name, correlation ID, log level, message, extra context.
- **Correlation**: A unique `trace_id` propagated from trade generation through every downstream service to enable end-to-end request tracing.
- **Levels**: DEBUG (local dev), INFO (staging), WARNING+ (production).

### 6.2 Error Handling

- **Strategy**: Fail fast for configuration errors; retry with exponential backoff for transient failures.
- **Dead Letter Queues**: Kafka DLQ topics for messages that fail processing after max retries.
- **Circuit Breakers**: Protect downstream services from cascading failures (e.g., if Iceberg write fails, buffer in Kafka).
- **Alerting**: Errors above threshold trigger Prometheus alerts routed to Grafana.
- **Idempotency**: All write operations must be idempotent to support safe retries.

### 6.3 Configuration Management

- **Approach**: Environment-based configuration with sensible defaults.
- **Library**: `pydantic-settings` for typed, validated configuration in Python services.
- **Layers**: Defaults (code) -> config files (YAML) -> environment variables -> CLI arguments.
- **Environments**: `local`, `dev`, `staging`, `prod` with overrides per environment.
- **No hardcoded values**: All ports, hosts, credentials, and feature flags are externalized.

### 6.4 Secrets Management

- **Local development**: `.env` files (gitignored) loaded via Docker Compose or `pydantic-settings`.
- **CI/CD**: GitHub Actions secrets for sensitive values.
- **Kubernetes**: Kubernetes Secrets (base64 encoded; in production, use sealed-secrets or external secrets operator).
- **Policy**: No secrets in code, no secrets in Docker images, no secrets in git history.
- **Rotation**: Document secret rotation procedures in `docs/runbooks/`.

### 6.5 Schema Management

- **Trade event schemas**: Defined in `libs/common/schemas/` as Avro or JSON Schema.
- **Evolution**: Backward-compatible schema changes only. New fields are optional with defaults.
- **Validation**: Schema validation at Kafka producer and consumer boundaries.
- **Versioning**: Schema version tracked in event metadata.

### 6.6 Testing Strategy

| Level | Scope | Tools | When |
|---|---|---|---|
| Unit | Single function/class | pytest, pytest-mock | Every commit |
| Integration | Two or more services | pytest, testcontainers | Every PR |
| E2E | Full pipeline | pytest, Docker Compose | Nightly / pre-release |
| Data Quality | Data correctness | Great Expectations | Every pipeline run |
| Performance | Throughput & latency | locust, custom benchmarks | Weekly / on-demand |

---

## 7. Component Dependency Map

The following shows which components depend on others. A component can only be built and
tested once its dependencies are operational.

```
Level 0 (No dependencies):
  Docker / Docker Compose
  Terraform scaffolding

Level 1 (Depends on Level 0):
  Kafka cluster (Docker)
  MinIO / S3 storage (Docker)
  Iceberg catalog (Docker)

Level 2 (Depends on Level 1):
  Trade Generator          -- standalone, no infra dependency for dev
  Kafka Consumer           -- depends on: Kafka, Trade Generator (runtime)
  Flink cluster            -- depends on: Docker

Level 3 (Depends on Level 2):
  Flink Processor jobs     -- depends on: Flink cluster, Kafka (input topics)
  Iceberg tables           -- depends on: Iceberg catalog, MinIO

Level 4 (Depends on Level 3):
  Kafka-to-Iceberg sink    -- depends on: Kafka (processed topics), Iceberg tables
  DuckDB analytics         -- depends on: Iceberg tables (Parquet files)

Level 5 (Depends on Level 4):
  Dagster orchestrator     -- depends on: Iceberg tables, Kafka, DuckDB
  Great Expectations       -- depends on: Iceberg tables, Dagster

Level 6 (Depends on Level 5):
  GraphQL API              -- depends on: DuckDB, Iceberg tables
  Prometheus + Grafana     -- depends on: all services (scrape targets)

Level 7 (Depends on Level 6):
  Apache Superset          -- depends on: DuckDB / GraphQL API
  OpenMetadata             -- depends on: Kafka, Iceberg, Dagster (metadata sources)

Level 8 (Depends on all):
  CI/CD pipelines          -- depends on: all services (test and deploy)
  Performance optimization -- depends on: full pipeline operational
```

### Visual Dependency Graph

```
  Trade Generator
        |
        v
  Kafka Consumer ----> Kafka Cluster <---- Flink Processor
                            |                     |
                            v                     v
                      Kafka Topics ---------> Iceberg Tables
                                                  |
                                    +-------------+-------------+
                                    |             |             |
                                    v             v             v
                                 Dagster      DuckDB      Great Expectations
                                    |             |
                                    v             v
                              Scheduling     GraphQL API
                                                  |
                                                  v
                                             Superset Dashboards

  (Prometheus + Grafana observe all components)
  (OpenMetadata catalogs all data assets)
  (CI/CD wraps all components)
```

---

## 8. Individual Plan References

Each phase has a dedicated planning document with detailed requirements, design decisions,
implementation steps, and acceptance criteria.

| # | Document | Phase | Description |
|---|---|---|---|
| 01 | [01-infrastructure.md](./01-infrastructure.md) | Phase 1 | Docker Compose stack, Terraform modules, Colima K8s, network topology, Makefile targets |
| 02 | [02-data-generation.md](./02-data-generation.md) | Phase 2 | Trade generator design, WebSocket protocol, Kafka producer config, Avro schemas, data model definitions |
| 03 | [03-stream-processing.md](./03-stream-processing.md) | Phase 3 | Flink job architecture, windowing strategies, state management, topic routing, checkpointing |
| 04 | [04-storage-modeling.md](./04-storage-modeling.md) | Phase 4 | Iceberg table DDL, SCD Type 1/2/3 patterns, partitioning strategy, compaction, DuckDB integration |
| 05 | [05-orchestration.md](./05-orchestration.md) | Phase 5 | Dagster assets, jobs, schedules, sensors, backfill strategy, retry policies, resource configuration |
| 06 | [06-data-quality.md](./06-data-quality.md) | Phase 6 | Expectation suites, dagster-ge integration, quality gates, data docs, alerting on failures |
| 07 | [07-api-serving.md](./07-api-serving.md) | Phase 7 | GraphQL schema design, Strawberry resolvers, subscriptions, DataLoaders, authentication |
| 08 | [08-monitoring.md](./08-monitoring.md) | Phase 8 | Prometheus scrape configs, custom metrics, Grafana dashboard definitions, alert rules |
| 09 | [09-governance.md](./09-governance.md) | Phase 9 | OpenMetadata setup, ingestion connectors, lineage mapping, classification policies, RBAC |
| 10 | [10-visualization.md](./10-visualization.md) | Phase 10 | Superset deployment, dashboard designs, dataset connections, export/sharing setup |
| 11 | [11-cicd-devops.md](./11-cicd-devops.md) | Phase 11 | GitHub Actions workflows, test matrix, local runner setup, deployment strategy, environment promotion |
| 12 | [12-performance.md](./12-performance.md) | Phase 12 | Kafka tuning, Flink optimization, Iceberg compaction, query optimization, caching, benchmarks |
| -- | [12-teardown-scripts.md](./12-teardown-scripts.md) | All Phases | Global & granular module teardown scripts, data wipe, nuclear reset, rebuild procedures |

---

## Evaluation Rubric Alignment

This project is designed to satisfy the capstone evaluation criteria:

| Dimension | Weight | Covered In |
|---|---|---|
| Multiple source types & ingestion patterns | 10% | Phase 2 (Trade Generator, WebSocket, Kafka) |
| Processing patterns (batch + streaming) | 15% | Phase 3 (Flink streaming) + Phase 5 (Dagster batch) |
| Storage architecture & data modeling | 10% | Phase 4 (Iceberg, SCD, partitioning) |
| Orchestration & workflow design | 10% | Phase 5 (Dagster) |
| Data quality implementation | 10% | Phase 6 (Great Expectations) |
| Catalog, lineage & governance | 10% | Phase 9 (OpenMetadata) |
| Observability & monitoring | 10% | Phase 8 (Prometheus + Grafana) |
| API/serving layer | 10% | Phase 7 (GraphQL) |
| Documentation & data product thinking | 10% | All phases (plan docs, ADRs, runbooks) |
| Code quality & CI/CD practices | 5% | Phase 11 (GitHub Actions, testing, linting) |

---

*This master plan serves as the single source of truth for the project scope, architecture,
and implementation roadmap. Each phase plan document provides the detailed specification
needed for implementation.*
