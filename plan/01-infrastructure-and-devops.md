# Infrastructure & DevOps Implementation Plan

> **Component**: 11 - Infrastructure & DevOps for Data
> **Monorepo**: DE-project (Stock Market Trade Data Pipeline)
> **Principle**: Everything reproducible from code (IaC). Zero manual infrastructure steps.

---

## Table of Contents

1. [Local Development Environment Setup](#1-local-development-environment-setup)
2. [Docker Containerization](#2-docker-containerization)
3. [Docker Compose for Local Development](#3-docker-compose-for-local-development)
4. [Kubernetes Manifests](#4-kubernetes-manifests)
5. [Terraform / IaC](#5-terraform--iac)
6. [CI/CD Pipeline](#6-cicd-pipeline)
7. [Environment Promotion](#7-environment-promotion)
8. [Scripts & Automation](#8-scripts--automation)
9. [Implementation Steps (Ordered, with File Paths)](#9-implementation-steps-ordered-with-file-paths)
10. [Estimated Effort](#10-estimated-effort)

---

## 1. Local Development Environment Setup

### 1.1 Colima Installation and Configuration

Colima serves as the Docker runtime on macOS, replacing Docker Desktop. It runs a lightweight Linux VM via Lima and exposes the Docker socket.

**Installation (via Homebrew):**

```bash
brew install colima docker docker-compose docker-credential-helper
```

**Recommended VM Configuration for Data Workloads:**

| Resource | Value | Rationale |
|----------|-------|-----------|
| CPUs | 6 | Kafka (3 brokers) + Flink (JobManager + TaskManager) + auxiliary services |
| Memory | 12 GB | Kafka brokers ~1GB each, Flink ~2GB, MinIO ~1GB, Dagster ~1GB, headroom |
| Disk | 60 GB | Iceberg/Parquet storage, Kafka logs, Docker image layers |
| Runtime | docker | Standard Docker runtime (not containerd) for Compose compatibility |
| VM Type | vz | Apple Virtualization.framework -- better performance on Apple Silicon |
| Mount Type | virtiofs | Fastest host-to-VM file sharing on macOS Ventura+ |

**Colima profile for this project:**

```bash
colima start \
  --profile de-project \
  --cpu 6 \
  --memory 12 \
  --disk 60 \
  --vm-type vz \
  --mount-type virtiofs \
  --network-address
```

The `--network-address` flag assigns a routable IP to the VM, which is useful for accessing services from the host without port-forwarding conflicts.

**Colima with Kubernetes (optional):**

```bash
colima start \
  --profile de-project-k8s \
  --cpu 8 \
  --memory 16 \
  --disk 80 \
  --vm-type vz \
  --mount-type virtiofs \
  --kubernetes \
  --network-address
```

When Kubernetes is enabled, Colima provisions a single-node k3s cluster. This requires more resources because the kubelet, kube-proxy, CoreDNS, and other control-plane components run inside the VM alongside workloads.

### 1.2 Docker and Docker Compose Setup

Docker CLI and Compose plugin are installed alongside Colima. Verify after Colima starts:

```bash
docker context use colima-de-project
docker info
docker compose version   # Must be v2.20+
```

**Docker daemon configuration** (`~/.colima/de-project/colima.yaml` overrides or `daemon.json`):

```json
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "10m",
    "max-file": "3"
  },
  "default-ulimits": {
    "nofile": { "Name": "nofile", "Hard": 65536, "Soft": 65536 }
  }
}
```

The `nofile` ulimit is critical for Kafka brokers, which open many file descriptors.

### 1.3 Required Developer Tools

All tools are managed via Homebrew and a project-level `.tool-versions` (for asdf) or a Brewfile.

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.11+ | trade-generator, kafka-consumer, dagster, graphql-api, PyFlink |
| Java (Temurin) | 11 | Apache Flink runtime (Flink 1.18.x requires JDK 11) |
| Node.js | 18+ | Superset frontend build, tooling |
| uv | latest | Fast Python dependency management (replaces pip/poetry) |
| ruff | latest | Python linter and formatter |
| mypy | latest | Static type checking for Python |
| pytest | latest | Python test runner |
| terraform | 1.6+ | Infrastructure as Code |
| kubectl | 1.28+ | Kubernetes CLI |
| helm | 3.13+ | Kubernetes package manager |
| k9s | latest | Terminal-based Kubernetes UI |
| jq / yq | latest | JSON/YAML processing in scripts |
| trivy | latest | Container image vulnerability scanner |
| gh | latest | GitHub CLI for CI/CD interactions |
| pre-commit | latest | Git hook management |

**Brewfile** (placed at repo root):

```ruby
# Brewfile
brew "colima"
brew "docker"
brew "docker-compose"
brew "docker-credential-helper"
brew "python@3.11"
brew "openjdk@11"
brew "node@18"
brew "uv"
brew "ruff"
brew "terraform"
brew "kubectl"
brew "helm"
brew "k9s"
brew "jq"
brew "yq"
brew "trivy"
brew "gh"
brew "pre-commit"
```

### 1.4 Makefile Targets for Common Operations

The root `Makefile` serves as the single entry point for all developer operations. Targets are grouped by concern.

```makefile
# ── Environment ──────────────────────────────────────────
setup              # Run full local environment setup (calls setup.sh)
colima-start       # Start Colima VM with project profile
colima-stop        # Stop Colima VM

# ── Docker Compose (Start) ──────────────────────────────
up                 # Start all services (docker compose up -d)
up-kafka           # Start only Kafka cluster
up-flink           # Start only Flink cluster
up-storage         # Start only MinIO + Iceberg catalog
up-monitoring      # Start only Prometheus + Grafana
up-governance      # Start only OpenMetadata stack
up-dagster         # Start only Dagster
up-services        # Start only application services
up-superset        # Start only Superset
logs               # Tail logs for all services
logs-kafka         # Tail logs for Kafka services
logs-flink         # Tail logs for Flink services
ps                 # Show running containers and status

# ── Teardown — Global (see plan/12-teardown-scripts.md) ─
teardown           # Stop all services gracefully, preserve data
teardown-destroy   # Stop all services + delete all data volumes
teardown-nuclear   # Full reset: volumes + images + networks + caches
fresh-start        # Nuclear teardown + full setup from scratch
fresh-start-keep-images  # Destroy data + restart, keep Docker images

# ── Teardown — Granular Module (stop only, keep data) ───
down-services      # Stop app services (trade-generator, kafka-consumer, graphql-api)
down-superset      # Stop Superset stack
down-dagster       # Stop Dagster orchestrator
down-flink         # Stop Flink cluster (triggers savepoint first)
down-governance    # Stop OpenMetadata stack
down-monitoring    # Stop Prometheus + Grafana stack
down-kafka         # Stop Kafka cluster + Schema Registry
down-storage       # Stop MinIO + Iceberg catalog

# ── Teardown — Granular Module (stop + destroy data) ────
destroy-services   # Stop app services + flush Redis cache
destroy-superset   # Stop Superset + delete PostgreSQL + Redis data
destroy-dagster    # Stop Dagster + delete run history/event logs
destroy-flink      # Stop Flink + delete checkpoints/savepoints
destroy-governance # Stop OpenMetadata + delete MySQL + Elasticsearch
destroy-monitoring # Stop monitoring + delete Prometheus TSDB + Grafana
destroy-kafka      # Stop Kafka + delete all topic data + schemas
destroy-storage    # Stop MinIO/Iceberg + delete ALL warehouse data

# ── Data Wipe (services stay running) ───────────────────
wipe-bronze        # Delete only Bronze layer data
wipe-silver        # Delete only Silver layer data
wipe-gold          # Delete only Gold layer data
wipe-all-data      # Delete all Iceberg warehouse data
wipe-kafka-topics  # Delete and recreate all Kafka topics

# ── Build ────────────────────────────────────────────────
build              # Build all Docker images
build-trade-gen    # Build trade-generator image
build-kafka-con    # Build kafka-consumer image
build-flink        # Build flink-processor image
build-dagster      # Build dagster-orchestrator image
build-graphql      # Build graphql-api image

# ── Quality ──────────────────────────────────────────────
lint               # Run ruff linter on all Python code
format             # Run ruff formatter + black
typecheck          # Run mypy on all Python packages
test               # Run all unit tests (pytest)
test-integration   # Run integration tests (requires services running)
quality            # Run lint + typecheck + test in sequence
scan               # Run trivy scan on all built images

# ── Data Operations ──────────────────────────────────────
migrate            # Run database migrations (Dagster PostgreSQL, etc.)
seed               # Seed initial reference data
health             # Run health checks against all services

# ── Kubernetes ───────────────────────────────────────────
k8s-apply-dev      # Apply all K8s manifests to de-dev namespace
k8s-apply-staging  # Apply all K8s manifests to de-staging namespace
k8s-teardown-dev   # Delete all resources in de-dev namespace
k8s-teardown-staging  # Delete all resources in de-staging namespace
k8s-teardown-all   # Delete all DE namespaces
k8s-status         # Show pod/service status across namespaces

# ── Terraform ────────────────────────────────────────────
tf-init            # terraform init for local environment
tf-plan            # terraform plan
tf-apply           # terraform apply
tf-destroy         # terraform destroy (all Terraform-managed resources)
```

---

## 2. Docker Containerization

### 2.1 Base Image Strategy

Use a shared base image to reduce build times, layer duplication, and ensure consistency across Python services.

**Common Python base image** (`docker/base-python/Dockerfile`):

```dockerfile
# ── Stage 1: Base ────────────────────────────────────────
FROM python:3.11-slim-bookworm AS python-base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    tini \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency resolution
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

ENTRYPOINT ["tini", "--"]

WORKDIR /app
```

Key decisions:
- `python:3.11-slim-bookworm` -- Debian-based slim image; smaller than full but has enough C libraries for most Python packages.
- `tini` -- Proper PID 1 init process; handles signal forwarding and zombie reaping.
- `uv` -- Copied from official image; used to install dependencies at build time.

### 2.2 Dockerfiles per Service

#### 2.2.1 trade-generator

**Path**: `services/trade-generator/Dockerfile`

```dockerfile
# ── Stage 1: Dependencies ────────────────────────────────
FROM de-project/python-base:latest AS deps
COPY services/trade-generator/pyproject.toml services/trade-generator/uv.lock ./
RUN uv sync --frozen --no-dev

# ── Stage 2: Application ─────────────────────────────────
FROM de-project/python-base:latest AS runtime
COPY --from=deps /app/.venv /app/.venv
COPY services/trade-generator/src ./src
ENV PATH="/app/.venv/bin:$PATH"

HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
    CMD python -c "import socket; s=socket.socket(); s.settimeout(2); s.connect(('localhost',8765)); s.close()" || exit 1

EXPOSE 8765
CMD ["python", "-m", "trade_generator.main"]
```

**Health check**: Verifies the WebSocket server is accepting connections on port 8765.

#### 2.2.2 kafka-consumer

**Path**: `services/kafka-consumer/Dockerfile`

```dockerfile
FROM de-project/python-base:latest AS deps
COPY services/kafka-consumer/pyproject.toml services/kafka-consumer/uv.lock ./
RUN uv sync --frozen --no-dev

FROM de-project/python-base:latest AS runtime
COPY --from=deps /app/.venv /app/.venv
COPY services/kafka-consumer/src ./src
ENV PATH="/app/.venv/bin:$PATH"

HEALTHCHECK --interval=15s --timeout=10s --retries=5 \
    CMD python -c "from kafka_consumer.health import check; check()" || exit 1

CMD ["python", "-m", "kafka_consumer.main"]
```

**Health check**: Calls an internal health module that verifies Kafka consumer group connectivity and lag.

#### 2.2.3 flink-processor

**Path**: `services/flink-processor/Dockerfile`

This service can be implemented via PyFlink (Python API on top of JVM Flink). The Dockerfile extends the official Flink image and adds Python/PyFlink.

```dockerfile
# ── Stage 1: Python dependencies ─────────────────────────
FROM python:3.11-slim-bookworm AS py-deps
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
COPY services/flink-processor/pyproject.toml services/flink-processor/uv.lock ./
RUN uv sync --frozen --no-dev

# ── Stage 2: Flink runtime ───────────────────────────────
FROM flink:1.18-java11 AS runtime

# Install Python 3.11 inside the Flink image
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.11 python3.11-venv python3-pip curl \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3.11 /usr/bin/python3 \
    && ln -sf /usr/bin/python3.11 /usr/bin/python

COPY --from=py-deps /app/.venv /opt/flink/python-venv
COPY services/flink-processor/src /opt/flink/usrlib/
COPY services/flink-processor/conf/ /opt/flink/conf/

ENV PYFLINK_PYTHON=/opt/flink/python-venv/bin/python

HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD curl -f http://localhost:8081/overview || exit 1
```

**Health check**: Hits the Flink REST API `/overview` endpoint to verify the JobManager/TaskManager is responsive.

#### 2.2.4 dagster-orchestrator

**Path**: `services/dagster-orchestrator/Dockerfile`

```dockerfile
FROM de-project/python-base:latest AS deps
COPY services/dagster-orchestrator/pyproject.toml services/dagster-orchestrator/uv.lock ./
RUN uv sync --frozen --no-dev

FROM de-project/python-base:latest AS runtime
COPY --from=deps /app/.venv /app/.venv
COPY services/dagster-orchestrator/src ./src
COPY services/dagster-orchestrator/workspace.yaml ./
ENV PATH="/app/.venv/bin:$PATH"

HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:3000/server_info || exit 1

EXPOSE 3000 3030
CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]
```

**Health check**: Hits the Dagster webserver `/server_info` endpoint.

Note: The Dagster daemon runs as a separate container from the same image with a different CMD (`dagster-daemon run`).

#### 2.2.5 graphql-api

**Path**: `services/graphql-api/Dockerfile`

```dockerfile
FROM de-project/python-base:latest AS deps
COPY services/graphql-api/pyproject.toml services/graphql-api/uv.lock ./
RUN uv sync --frozen --no-dev

FROM de-project/python-base:latest AS runtime
COPY --from=deps /app/.venv /app/.venv
COPY services/graphql-api/src ./src
ENV PATH="/app/.venv/bin:$PATH"

HEALTHCHECK --interval=10s --timeout=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

EXPOSE 8000
CMD ["uvicorn", "graphql_api.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

**Health check**: Hits a dedicated `/health` endpoint that verifies DB connectivity.

### 2.3 Multi-Stage Build Summary

Every service Dockerfile uses at minimum two stages:

| Stage | Purpose | What Gets Discarded |
|-------|---------|---------------------|
| `deps` | Install Python/Java dependencies | Build tools, compilers, pip cache, intermediate wheels |
| `runtime` | Copy only venv + application source | Everything from deps stage except the final artifact |

Expected image sizes:

| Image | Approximate Size |
|-------|-----------------|
| python-base | ~150 MB |
| trade-generator | ~200 MB |
| kafka-consumer | ~220 MB |
| flink-processor | ~900 MB (JVM + Python + Flink libs) |
| dagster-orchestrator | ~350 MB |
| graphql-api | ~200 MB |

### 2.4 Image Tagging Strategy

```
de-project/<service>:<git-sha-short>       # Immutable build tag
de-project/<service>:latest                 # Mutable convenience tag
de-project/<service>:dev                    # Current dev build
de-project/<service>:v1.2.3                 # Semantic version for releases
```

---

## 3. Docker Compose for Local Development

### 3.1 Compose File Structure

The Compose configuration is split into concern-specific files to allow developers to start only what they need. All files live under `infra/docker-compose/`.

```
infra/docker-compose/
  docker-compose.base.yml        # Networks, named volumes, shared config
  docker-compose.kafka.yml       # Zookeeper, Kafka x3, Schema Registry, Kafka UI
  docker-compose.flink.yml       # Flink JobManager, TaskManager
  docker-compose.storage.yml     # MinIO (S3), Iceberg REST Catalog
  docker-compose.dagster.yml     # Dagster webserver, daemon, PostgreSQL
  docker-compose.monitoring.yml  # Prometheus, Grafana, Node Exporter
  docker-compose.governance.yml  # OpenMetadata, MySQL, Elasticsearch
  docker-compose.services.yml    # trade-generator, kafka-consumer, graphql-api
  docker-compose.superset.yml    # Superset, Redis, PostgreSQL
```

**Invocation pattern** (from Makefile):

```bash
# Start everything
docker compose \
  -f infra/docker-compose/docker-compose.base.yml \
  -f infra/docker-compose/docker-compose.kafka.yml \
  -f infra/docker-compose/docker-compose.flink.yml \
  -f infra/docker-compose/docker-compose.storage.yml \
  -f infra/docker-compose/docker-compose.dagster.yml \
  -f infra/docker-compose/docker-compose.monitoring.yml \
  -f infra/docker-compose/docker-compose.governance.yml \
  -f infra/docker-compose/docker-compose.services.yml \
  -f infra/docker-compose/docker-compose.superset.yml \
  up -d

# Start only Kafka
docker compose \
  -f infra/docker-compose/docker-compose.base.yml \
  -f infra/docker-compose/docker-compose.kafka.yml \
  up -d
```

### 3.2 docker-compose.base.yml

Defines shared networks and volumes used across all other Compose files.

```yaml
# Networks
networks:
  de-ingestion:
    driver: bridge
    name: de-ingestion
    # Kafka brokers, trade-generator, kafka-consumer
  de-processing:
    driver: bridge
    name: de-processing
    # Flink, Kafka (consumer side), storage
  de-storage:
    driver: bridge
    name: de-storage
    # MinIO, Iceberg catalog, Dagster, query engines
  de-serving:
    driver: bridge
    name: de-serving
    # GraphQL API, Superset, Dagster UI
  de-monitoring:
    driver: bridge
    name: de-monitoring
    # Prometheus, Grafana, Node Exporter (attached to all services)
  de-governance:
    driver: bridge
    name: de-governance
    # OpenMetadata, MySQL, Elasticsearch

# Volumes
volumes:
  kafka-data-1:
  kafka-data-2:
  kafka-data-3:
  zookeeper-data:
  zookeeper-log:
  minio-data:
  dagster-pg-data:
  superset-pg-data:
  superset-redis-data:
  prometheus-data:
  grafana-data:
  openmetadata-mysql-data:
  openmetadata-es-data:
  flink-checkpoints:
  flink-savepoints:
  iceberg-warehouse:
```

### 3.3 docker-compose.kafka.yml

```yaml
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    hostname: zookeeper
    container_name: de-zookeeper
    ports:
      - "2181:2181"
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    volumes:
      - zookeeper-data:/var/lib/zookeeper/data
      - zookeeper-log:/var/lib/zookeeper/log
    networks:
      - de-ingestion
    deploy:
      resources:
        limits:
          memory: 512M
          cpus: "0.5"
    healthcheck:
      test: ["CMD", "echo", "ruok", "|", "nc", "localhost", "2181"]
      interval: 10s
      timeout: 5s
      retries: 5

  kafka-broker-1:
    image: confluentinc/cp-kafka:7.6.0
    hostname: kafka-broker-1
    container_name: de-kafka-1
    depends_on:
      zookeeper:
        condition: service_healthy
    ports:
      - "9092:9092"   # External listener for host access
      - "29092:29092"  # Internal listener
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-broker-1:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
      KAFKA_DEFAULT_REPLICATION_FACTOR: 3
      KAFKA_MIN_INSYNC_REPLICAS: 2
      KAFKA_NUM_PARTITIONS: 6
      KAFKA_LOG_RETENTION_HOURS: 168
      KAFKA_LOG_SEGMENT_BYTES: 1073741824
      KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS: 300000
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 3000
    volumes:
      - kafka-data-1:/var/lib/kafka/data
    networks:
      - de-ingestion
      - de-processing
    deploy:
      resources:
        limits:
          memory: 1G
          cpus: "1.0"
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:29092"]
      interval: 15s
      timeout: 10s
      retries: 10
      start_period: 30s

  kafka-broker-2:
    # Same as kafka-broker-1 with KAFKA_BROKER_ID: 2
    # Ports: 9093:9093, 29093:29093
    # Volume: kafka-data-2

  kafka-broker-3:
    # Same as kafka-broker-1 with KAFKA_BROKER_ID: 3
    # Ports: 9094:9094, 29094:29094
    # Volume: kafka-data-3

  schema-registry:
    image: confluentinc/cp-schema-registry:7.6.0
    hostname: schema-registry
    container_name: de-schema-registry
    depends_on:
      kafka-broker-1:
        condition: service_healthy
    ports:
      - "8081:8081"
    environment:
      SCHEMA_REGISTRY_HOST_NAME: schema-registry
      SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS: kafka-broker-1:29092,kafka-broker-2:29093,kafka-broker-3:29094
      SCHEMA_REGISTRY_LISTENERS: http://0.0.0.0:8081
    networks:
      - de-ingestion
    deploy:
      resources:
        limits:
          memory: 512M
          cpus: "0.5"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/subjects"]
      interval: 10s
      timeout: 5s
      retries: 5

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    hostname: kafka-ui
    container_name: de-kafka-ui
    depends_on:
      kafka-broker-1:
        condition: service_healthy
      schema-registry:
        condition: service_healthy
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: de-local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka-broker-1:29092,kafka-broker-2:29093,kafka-broker-3:29094
      KAFKA_CLUSTERS_0_SCHEMAREGISTRY: http://schema-registry:8081
      KAFKA_CLUSTERS_0_METRICS_PORT: 9997
    networks:
      - de-ingestion
    deploy:
      resources:
        limits:
          memory: 256M
          cpus: "0.5"
```

### 3.4 docker-compose.flink.yml

```yaml
services:
  flink-jobmanager:
    image: de-project/flink-processor:dev
    hostname: flink-jobmanager
    container_name: de-flink-jobmanager
    command: jobmanager
    ports:
      - "8082:8081"   # Flink Web UI (mapped to 8082 to avoid conflict with Schema Registry)
      - "6123:6123"   # RPC
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
        state.backend: rocksdb
        state.checkpoints.dir: file:///opt/flink/checkpoints
        state.savepoints.dir: file:///opt/flink/savepoints
        execution.checkpointing.interval: 60000
        execution.checkpointing.mode: EXACTLY_ONCE
    volumes:
      - flink-checkpoints:/opt/flink/checkpoints
      - flink-savepoints:/opt/flink/savepoints
    networks:
      - de-processing
      - de-monitoring
    deploy:
      resources:
        limits:
          memory: 1G
          cpus: "1.0"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8081/overview"]
      interval: 15s
      timeout: 10s
      retries: 5

  flink-taskmanager:
    image: de-project/flink-processor:dev
    hostname: flink-taskmanager
    container_name: de-flink-taskmanager
    command: taskmanager
    depends_on:
      flink-jobmanager:
        condition: service_healthy
    environment:
      FLINK_PROPERTIES: |
        jobmanager.rpc.address: flink-jobmanager
        taskmanager.numberOfTaskSlots: 4
        taskmanager.memory.process.size: 2g
        state.backend: rocksdb
        state.checkpoints.dir: file:///opt/flink/checkpoints
        state.savepoints.dir: file:///opt/flink/savepoints
    volumes:
      - flink-checkpoints:/opt/flink/checkpoints
      - flink-savepoints:/opt/flink/savepoints
    networks:
      - de-processing
      - de-ingestion
      - de-storage
      - de-monitoring
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: "2.0"
```

### 3.5 docker-compose.storage.yml

```yaml
services:
  minio:
    image: minio/minio:RELEASE.2024-01-29T03-56-32Z
    hostname: minio
    container_name: de-minio
    ports:
      - "9000:9000"   # S3 API
      - "9001:9001"   # MinIO Console
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER:-minio}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD:-minio123}
    command: server /data --console-address ":9001"
    volumes:
      - minio-data:/data
    networks:
      - de-storage
      - de-monitoring
    deploy:
      resources:
        limits:
          memory: 1G
          cpus: "1.0"
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 10s
      timeout: 5s
      retries: 5

  # Initialize MinIO buckets on first start
  minio-init:
    image: minio/mc:latest
    container_name: de-minio-init
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
      mc alias set local http://minio:9000 $${MINIO_ROOT_USER} $${MINIO_ROOT_PASSWORD};
      mc mb --ignore-existing local/iceberg-warehouse;
      mc mb --ignore-existing local/raw-data;
      mc mb --ignore-existing local/checkpoints;
      mc mb --ignore-existing local/backups;
      echo 'Buckets created successfully';
      "
    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER:-minio}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD:-minio123}
    networks:
      - de-storage

  iceberg-rest-catalog:
    image: tabulario/iceberg-rest:1.4.0
    hostname: iceberg-rest
    container_name: de-iceberg-rest
    depends_on:
      minio-init:
        condition: service_completed_successfully
    ports:
      - "8181:8181"
    environment:
      CATALOG_WAREHOUSE: s3://iceberg-warehouse/
      CATALOG_IO__IMPL: org.apache.iceberg.aws.s3.S3FileIO
      CATALOG_S3_ENDPOINT: http://minio:9000
      CATALOG_S3_PATH__STYLE__ACCESS: "true"
      AWS_ACCESS_KEY_ID: ${MINIO_ROOT_USER:-minio}
      AWS_SECRET_ACCESS_KEY: ${MINIO_ROOT_PASSWORD:-minio123}
      AWS_REGION: us-east-1
    networks:
      - de-storage
      - de-processing
    deploy:
      resources:
        limits:
          memory: 512M
          cpus: "0.5"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8181/v1/config"]
      interval: 10s
      timeout: 5s
      retries: 5
```

### 3.6 docker-compose.dagster.yml

```yaml
services:
  dagster-postgres:
    image: postgres:16-alpine
    hostname: dagster-postgres
    container_name: de-dagster-pg
    environment:
      POSTGRES_USER: dagster
      POSTGRES_PASSWORD: ${DAGSTER_PG_PASSWORD:-dagster_pass}
      POSTGRES_DB: dagster
    volumes:
      - dagster-pg-data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    networks:
      - de-storage
    deploy:
      resources:
        limits:
          memory: 256M
          cpus: "0.5"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U dagster"]
      interval: 5s
      timeout: 3s
      retries: 5

  dagster-webserver:
    image: de-project/dagster-orchestrator:dev
    hostname: dagster-webserver
    container_name: de-dagster-webserver
    depends_on:
      dagster-postgres:
        condition: service_healthy
    command: ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000"]
    ports:
      - "3000:3000"
    environment:
      DAGSTER_POSTGRES_HOST: dagster-postgres
      DAGSTER_POSTGRES_PORT: 5432
      DAGSTER_POSTGRES_USER: dagster
      DAGSTER_POSTGRES_PASSWORD: ${DAGSTER_PG_PASSWORD:-dagster_pass}
      DAGSTER_POSTGRES_DB: dagster
    volumes:
      - ./services/dagster-orchestrator/src:/app/src:ro
    networks:
      - de-storage
      - de-serving
      - de-monitoring
    deploy:
      resources:
        limits:
          memory: 512M
          cpus: "0.5"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:3000/server_info"]
      interval: 15s
      timeout: 5s
      retries: 3

  dagster-daemon:
    image: de-project/dagster-orchestrator:dev
    hostname: dagster-daemon
    container_name: de-dagster-daemon
    depends_on:
      dagster-postgres:
        condition: service_healthy
    command: ["dagster-daemon", "run"]
    environment:
      DAGSTER_POSTGRES_HOST: dagster-postgres
      DAGSTER_POSTGRES_PORT: 5432
      DAGSTER_POSTGRES_USER: dagster
      DAGSTER_POSTGRES_PASSWORD: ${DAGSTER_PG_PASSWORD:-dagster_pass}
      DAGSTER_POSTGRES_DB: dagster
    volumes:
      - ./services/dagster-orchestrator/src:/app/src:ro
    networks:
      - de-storage
      - de-processing
      - de-ingestion
      - de-monitoring
    deploy:
      resources:
        limits:
          memory: 512M
          cpus: "0.5"
```

### 3.7 docker-compose.monitoring.yml

```yaml
services:
  prometheus:
    image: prom/prometheus:v2.48.1
    hostname: prometheus
    container_name: de-prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./infra/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - ./infra/prometheus/alerts/:/etc/prometheus/alerts/:ro
      - prometheus-data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--storage.tsdb.retention.time=30d'
      - '--web.enable-lifecycle'
    networks:
      - de-monitoring
    deploy:
      resources:
        limits:
          memory: 512M
          cpus: "0.5"
    healthcheck:
      test: ["CMD", "wget", "--spider", "-q", "http://localhost:9090/-/ready"]
      interval: 10s
      timeout: 5s
      retries: 3

  grafana:
    image: grafana/grafana:10.2.3
    hostname: grafana
    container_name: de-grafana
    depends_on:
      prometheus:
        condition: service_healthy
    ports:
      - "3001:3000"
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_PASSWORD:-admin}
      GF_INSTALL_PLUGINS: grafana-clock-panel
    volumes:
      - grafana-data:/var/lib/grafana
      - ./infra/grafana/provisioning/:/etc/grafana/provisioning/:ro
      - ./infra/grafana/dashboards/:/var/lib/grafana/dashboards/:ro
    networks:
      - de-monitoring
      - de-serving
    deploy:
      resources:
        limits:
          memory: 256M
          cpus: "0.5"
    healthcheck:
      test: ["CMD", "wget", "--spider", "-q", "http://localhost:3000/api/health"]
      interval: 10s
      timeout: 5s
      retries: 3

  node-exporter:
    image: prom/node-exporter:v1.7.0
    hostname: node-exporter
    container_name: de-node-exporter
    ports:
      - "9100:9100"
    volumes:
      - /proc:/host/proc:ro
      - /sys:/host/sys:ro
    command:
      - '--path.procfs=/host/proc'
      - '--path.sysfs=/host/sys'
    networks:
      - de-monitoring
    deploy:
      resources:
        limits:
          memory: 64M
          cpus: "0.25"
```

### 3.8 docker-compose.governance.yml

```yaml
services:
  openmetadata-mysql:
    image: mysql:8.0
    hostname: openmetadata-mysql
    container_name: de-om-mysql
    environment:
      MYSQL_ROOT_PASSWORD: ${OM_MYSQL_ROOT_PASSWORD:-openmetadata}
      MYSQL_DATABASE: openmetadata
      MYSQL_USER: openmetadata
      MYSQL_PASSWORD: ${OM_MYSQL_PASSWORD:-openmetadata}
    volumes:
      - openmetadata-mysql-data:/var/lib/mysql
    ports:
      - "3306:3306"
    networks:
      - de-governance
    deploy:
      resources:
        limits:
          memory: 512M
          cpus: "0.5"
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 10s
      timeout: 5s
      retries: 5

  openmetadata-elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.11.3
    hostname: openmetadata-elasticsearch
    container_name: de-om-es
    environment:
      discovery.type: single-node
      xpack.security.enabled: "false"
      ES_JAVA_OPTS: "-Xms512m -Xmx512m"
    volumes:
      - openmetadata-es-data:/usr/share/elasticsearch/data
    ports:
      - "9200:9200"
    networks:
      - de-governance
    deploy:
      resources:
        limits:
          memory: 1G
          cpus: "1.0"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9200/_cluster/health"]
      interval: 15s
      timeout: 10s
      retries: 5

  openmetadata:
    image: docker.getdbt.com/openmetadata/server:1.3.1
    hostname: openmetadata
    container_name: de-openmetadata
    depends_on:
      openmetadata-mysql:
        condition: service_healthy
      openmetadata-elasticsearch:
        condition: service_healthy
    ports:
      - "8585:8585"
    environment:
      DB_HOST: openmetadata-mysql
      DB_PORT: 3306
      DB_USER: openmetadata
      DB_USER_PASSWORD: ${OM_MYSQL_PASSWORD:-openmetadata}
      DB_DRIVER_CLASS: com.mysql.cj.jdbc.Driver
      ELASTICSEARCH_HOST: openmetadata-elasticsearch
      ELASTICSEARCH_PORT: 9200
    networks:
      - de-governance
      - de-monitoring
      - de-serving
    deploy:
      resources:
        limits:
          memory: 1G
          cpus: "1.0"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8585/api/v1/system/version"]
      interval: 15s
      timeout: 10s
      retries: 5
```

### 3.9 docker-compose.services.yml

```yaml
services:
  trade-generator:
    image: de-project/trade-generator:dev
    hostname: trade-generator
    container_name: de-trade-generator
    build:
      context: ../../
      dockerfile: services/trade-generator/Dockerfile
    ports:
      - "8765:8765"
    environment:
      TRADES_PER_SECOND: ${TRADES_PER_SECOND:-100}
      WS_HOST: 0.0.0.0
      WS_PORT: 8765
    networks:
      - de-ingestion
      - de-monitoring
    deploy:
      resources:
        limits:
          memory: 256M
          cpus: "0.5"

  kafka-consumer:
    image: de-project/kafka-consumer:dev
    hostname: kafka-consumer
    container_name: de-kafka-consumer
    build:
      context: ../../
      dockerfile: services/kafka-consumer/Dockerfile
    depends_on:
      kafka-broker-1:
        condition: service_healthy
      trade-generator:
        condition: service_healthy
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka-broker-1:29092,kafka-broker-2:29093,kafka-broker-3:29094
      SCHEMA_REGISTRY_URL: http://schema-registry:8081
      WS_URL: ws://trade-generator:8765
      CONSUMER_GROUP_ID: trade-consumer-group
    networks:
      - de-ingestion
      - de-monitoring
    deploy:
      resources:
        limits:
          memory: 512M
          cpus: "0.5"

  graphql-api:
    image: de-project/graphql-api:dev
    hostname: graphql-api
    container_name: de-graphql-api
    build:
      context: ../../
      dockerfile: services/graphql-api/Dockerfile
    ports:
      - "8000:8000"
    environment:
      ICEBERG_CATALOG_URI: http://iceberg-rest:8181
      S3_ENDPOINT: http://minio:9000
      S3_ACCESS_KEY: ${MINIO_ROOT_USER:-minio}
      S3_SECRET_KEY: ${MINIO_ROOT_PASSWORD:-minio123}
    networks:
      - de-serving
      - de-storage
      - de-monitoring
    deploy:
      resources:
        limits:
          memory: 512M
          cpus: "0.5"
```

### 3.10 docker-compose.superset.yml

```yaml
services:
  superset-postgres:
    image: postgres:16-alpine
    hostname: superset-postgres
    container_name: de-superset-pg
    environment:
      POSTGRES_USER: superset
      POSTGRES_PASSWORD: ${SUPERSET_PG_PASSWORD:-superset_pass}
      POSTGRES_DB: superset
    volumes:
      - superset-pg-data:/var/lib/postgresql/data
    ports:
      - "5433:5432"
    networks:
      - de-serving
    deploy:
      resources:
        limits:
          memory: 256M
          cpus: "0.5"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U superset"]
      interval: 5s
      timeout: 3s
      retries: 5

  superset-redis:
    image: redis:7-alpine
    hostname: superset-redis
    container_name: de-superset-redis
    ports:
      - "6379:6379"
    volumes:
      - superset-redis-data:/data
    networks:
      - de-serving
    deploy:
      resources:
        limits:
          memory: 128M
          cpus: "0.25"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

  superset:
    image: apache/superset:3.1.0
    hostname: superset
    container_name: de-superset
    depends_on:
      superset-postgres:
        condition: service_healthy
      superset-redis:
        condition: service_healthy
    ports:
      - "8088:8088"
    environment:
      SUPERSET_SECRET_KEY: ${SUPERSET_SECRET_KEY:-supersecretkey}
      SQLALCHEMY_DATABASE_URI: postgresql+psycopg2://superset:${SUPERSET_PG_PASSWORD:-superset_pass}@superset-postgres:5432/superset
      REDIS_URL: redis://superset-redis:6379/0
    volumes:
      - ./infra/superset/superset_config.py:/app/superset_home/superset_config.py:ro
    networks:
      - de-serving
      - de-storage
      - de-monitoring
    deploy:
      resources:
        limits:
          memory: 1G
          cpus: "1.0"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8088/health"]
      interval: 15s
      timeout: 10s
      retries: 5
```

### 3.11 Environment Variable Management

Environment variables are managed through `.env` files, one per environment:

```
infra/env/
  .env.local          # Default local development values
  .env.dev            # Dev environment overrides
  .env.staging        # Staging environment overrides
  .env.prod           # Production environment overrides (no secrets -- those go in Vault/K8s secrets)
  .env.example        # Template committed to git (no real values)
```

**`.env.example`** (committed to git):

```bash
# ── Kafka ────────────────────────────────────────────────
KAFKA_BROKER_COUNT=3
KAFKA_NUM_PARTITIONS=6
KAFKA_REPLICATION_FACTOR=3

# ── MinIO / S3 ──────────────────────────────────────────
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=CHANGE_ME

# ── Dagster ──────────────────────────────────────────────
DAGSTER_PG_PASSWORD=CHANGE_ME

# ── OpenMetadata ─────────────────────────────────────────
OM_MYSQL_ROOT_PASSWORD=CHANGE_ME
OM_MYSQL_PASSWORD=CHANGE_ME

# ── Superset ─────────────────────────────────────────────
SUPERSET_PG_PASSWORD=CHANGE_ME
SUPERSET_SECRET_KEY=CHANGE_ME

# ── Grafana ──────────────────────────────────────────────
GRAFANA_PASSWORD=CHANGE_ME

# ── Trade Generator ─────────────────────────────────────
TRADES_PER_SECOND=100
```

The actual `.env.local` file is created by `setup.sh` and is `.gitignored`.

### 3.12 Network Isolation Strategy

| Network | Purpose | Services |
|---------|---------|----------|
| `de-ingestion` | Trade data flow: generator -> Kafka | trade-generator, kafka-consumer, Kafka brokers, Zookeeper, Schema Registry, Kafka UI |
| `de-processing` | Stream processing | Flink (JobManager/TaskManager), Kafka brokers (consumer side), Iceberg REST catalog |
| `de-storage` | Persistent storage access | MinIO, Iceberg REST catalog, Dagster, dagster-postgres |
| `de-serving` | User-facing services | GraphQL API, Superset, Dagster webserver, Grafana |
| `de-monitoring` | Metrics collection | Prometheus, Grafana, Node Exporter; all services attach to this for scraping |
| `de-governance` | Data governance | OpenMetadata, MySQL, Elasticsearch |

Services that bridge concerns (e.g., Flink needs both Kafka and storage) are attached to multiple networks.

### 3.13 Resource Limits Summary

| Service | Memory Limit | CPU Limit |
|---------|-------------|-----------|
| Zookeeper | 512 MB | 0.5 |
| Kafka broker (x3) | 1 GB each | 1.0 each |
| Schema Registry | 512 MB | 0.5 |
| Kafka UI | 256 MB | 0.5 |
| Flink JobManager | 1 GB | 1.0 |
| Flink TaskManager | 2 GB | 2.0 |
| MinIO | 1 GB | 1.0 |
| Iceberg REST Catalog | 512 MB | 0.5 |
| Dagster PostgreSQL | 256 MB | 0.5 |
| Dagster webserver | 512 MB | 0.5 |
| Dagster daemon | 512 MB | 0.5 |
| Prometheus | 512 MB | 0.5 |
| Grafana | 256 MB | 0.5 |
| Node Exporter | 64 MB | 0.25 |
| OpenMetadata MySQL | 512 MB | 0.5 |
| OpenMetadata Elasticsearch | 1 GB | 1.0 |
| OpenMetadata Server | 1 GB | 1.0 |
| trade-generator | 256 MB | 0.5 |
| kafka-consumer | 512 MB | 0.5 |
| graphql-api | 512 MB | 0.5 |
| Superset PostgreSQL | 256 MB | 0.5 |
| Superset Redis | 128 MB | 0.25 |
| Superset | 1 GB | 1.0 |
| **TOTAL** | **~14.7 GB** | **~15.0 cores** |

> Note: Not all services need to run simultaneously during development. The Makefile targets allow starting subsets. Typical dev session: Kafka + Storage + 1 service = ~6 GB.

---

## 4. Kubernetes Manifests

### 4.1 Namespace Strategy

```
de-dev        # Local development (Colima k3s) and shared dev cluster
de-staging    # Pre-production environment; mirrors prod config at reduced scale
de-prod       # Production environment
```

Each namespace is isolated with:
- ResourceQuotas limiting total CPU/memory per namespace
- NetworkPolicies restricting cross-namespace traffic
- LimitRanges setting default resource requests/limits for pods

### 4.2 Directory Structure

```
infra/k8s/
  namespaces/
    de-dev.yaml
    de-staging.yaml
    de-prod.yaml
  base/                          # Kustomize base manifests
    kafka/
      statefulset.yaml
      service.yaml
      configmap.yaml
      pvc.yaml
      network-policy.yaml
    flink/
      deployment-jobmanager.yaml
      deployment-taskmanager.yaml
      service.yaml
      configmap.yaml
    storage/
      deployment-minio.yaml
      deployment-iceberg-rest.yaml
      service.yaml
      pvc.yaml
      job-init-buckets.yaml
    dagster/
      deployment-webserver.yaml
      deployment-daemon.yaml
      statefulset-postgres.yaml
      service.yaml
      configmap.yaml
      secret.yaml
    monitoring/
      deployment-prometheus.yaml
      deployment-grafana.yaml
      daemonset-node-exporter.yaml
      service.yaml
      configmap-prometheus.yaml
      configmap-grafana-dashboards.yaml
    governance/
      deployment-openmetadata.yaml
      statefulset-mysql.yaml
      statefulset-elasticsearch.yaml
      service.yaml
      configmap.yaml
    services/
      deployment-trade-generator.yaml
      deployment-kafka-consumer.yaml
      deployment-graphql-api.yaml
      service.yaml
      hpa.yaml
    superset/
      deployment.yaml
      statefulset-postgres.yaml
      deployment-redis.yaml
      service.yaml
      configmap.yaml
    ingress/
      ingress.yaml
  overlays/                      # Kustomize overlays per environment
    dev/
      kustomization.yaml         # Patches for dev (fewer replicas, lower resources)
    staging/
      kustomization.yaml
    prod/
      kustomization.yaml         # Full replicas, higher resources, production secrets
```

### 4.3 Key Manifest Patterns

#### StatefulSets (Kafka, PostgreSQL, MySQL, Elasticsearch)

Stateful services use `StatefulSet` for:
- Stable network identifiers (`kafka-0`, `kafka-1`, `kafka-2`)
- Ordered, graceful deployment and scaling
- Stable persistent storage via `volumeClaimTemplates`

**Kafka StatefulSet key fields:**

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: kafka
  namespace: de-dev
spec:
  serviceName: kafka-headless
  replicas: 3
  podManagementPolicy: Parallel
  selector:
    matchLabels:
      app: kafka
  template:
    spec:
      containers:
      - name: kafka
        image: confluentinc/cp-kafka:7.6.0
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
        livenessProbe:
          exec:
            command: ["kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
          initialDelaySeconds: 30
          periodSeconds: 15
        readinessProbe:
          exec:
            command: ["kafka-broker-api-versions", "--bootstrap-server", "localhost:9092"]
          initialDelaySeconds: 20
          periodSeconds: 10
  volumeClaimTemplates:
  - metadata:
      name: kafka-data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 10Gi
```

#### Deployments (Stateless Services)

All application services (trade-generator, kafka-consumer, graphql-api, Flink JobManager) use `Deployment`.

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: graphql-api
  namespace: de-dev
spec:
  replicas: 2
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 0
      maxSurge: 1
  selector:
    matchLabels:
      app: graphql-api
  template:
    spec:
      containers:
      - name: graphql-api
        image: de-project/graphql-api:latest
        ports:
        - containerPort: 8000
        resources:
          requests:
            memory: "256Mi"
            cpu: "100m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 5
        envFrom:
        - configMapRef:
            name: graphql-api-config
        - secretRef:
            name: graphql-api-secrets
```

#### ConfigMaps and Secrets

**Pattern**: Separate configuration from secrets. ConfigMaps hold non-sensitive config; Secrets hold credentials.

```yaml
# ConfigMap
apiVersion: v1
kind: ConfigMap
metadata:
  name: graphql-api-config
  namespace: de-dev
data:
  ICEBERG_CATALOG_URI: "http://iceberg-rest:8181"
  S3_ENDPOINT: "http://minio:9000"
  LOG_LEVEL: "INFO"

---
# Secret (values base64-encoded, managed via sealed-secrets or external-secrets in prod)
apiVersion: v1
kind: Secret
metadata:
  name: graphql-api-secrets
  namespace: de-dev
type: Opaque
stringData:
  S3_ACCESS_KEY: "minio"
  S3_SECRET_KEY: "minio123"
```

#### Services

| Service Type | Used For |
|-------------|----------|
| `ClusterIP` (default) | Internal service-to-service communication (Kafka internal, Dagster daemon, Flink internal) |
| `NodePort` | Local dev access from host to Colima VM (Kafka UI, Grafana, Dagster UI) |
| `Headless` (`clusterIP: None`) | StatefulSet DNS (kafka-0.kafka-headless, kafka-1.kafka-headless) |

```yaml
# Headless service for Kafka StatefulSet
apiVersion: v1
kind: Service
metadata:
  name: kafka-headless
  namespace: de-dev
spec:
  clusterIP: None
  selector:
    app: kafka
  ports:
  - name: internal
    port: 29092

---
# NodePort service for Kafka UI (local dev access)
apiVersion: v1
kind: Service
metadata:
  name: kafka-ui
  namespace: de-dev
spec:
  type: NodePort
  selector:
    app: kafka-ui
  ports:
  - port: 8080
    targetPort: 8080
    nodePort: 30080
```

#### Ingress

Single Ingress resource routing traffic to user-facing services:

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: de-ingress
  namespace: de-dev
  annotations:
    nginx.ingress.kubernetes.io/rewrite-target: /
spec:
  ingressClassName: nginx
  rules:
  - host: de.local
    http:
      paths:
      - path: /api
        pathType: Prefix
        backend:
          service:
            name: graphql-api
            port:
              number: 8000
      - path: /dagster
        pathType: Prefix
        backend:
          service:
            name: dagster-webserver
            port:
              number: 3000
      - path: /grafana
        pathType: Prefix
        backend:
          service:
            name: grafana
            port:
              number: 3000
      - path: /superset
        pathType: Prefix
        backend:
          service:
            name: superset
            port:
              number: 8088
      - path: /kafka-ui
        pathType: Prefix
        backend:
          service:
            name: kafka-ui
            port:
              number: 8080
      - path: /openmetadata
        pathType: Prefix
        backend:
          service:
            name: openmetadata
            port:
              number: 8585
```

#### Horizontal Pod Autoscaler

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: graphql-api-hpa
  namespace: de-dev
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: graphql-api
  minReplicas: 2
  maxReplicas: 8
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 25
        periodSeconds: 60
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60

---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: kafka-consumer-hpa
  namespace: de-dev
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: kafka-consumer
  minReplicas: 1
  maxReplicas: 6    # Should not exceed partition count
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 60
```

#### Resource Requests and Limits by Environment

| Service | Dev (requests/limits) | Staging | Prod |
|---------|----------------------|---------|------|
| Kafka broker | 256Mi-512Mi / 0.25-0.5 CPU | 512Mi-1Gi / 0.5-1 CPU | 2Gi-4Gi / 1-2 CPU |
| Flink TM | 512Mi-1Gi / 0.5-1 CPU | 1Gi-2Gi / 1-2 CPU | 4Gi-8Gi / 2-4 CPU |
| graphql-api | 128Mi-256Mi / 0.1-0.25 CPU | 256Mi-512Mi / 0.25-0.5 CPU | 512Mi-1Gi / 0.5-1 CPU |

---

## 5. Terraform / IaC

### 5.1 Purpose

Terraform manages infrastructure that lives outside of Kubernetes or Docker Compose:
- Docker networks and volumes (via the Docker provider) for reproducible local setup
- Kafka topics with specific partition counts and replication factors
- MinIO buckets and access policies
- Future: cloud provider resources when moving beyond local

### 5.2 Provider Configuration

```hcl
# infra/terraform/providers.tf
terraform {
  required_version = ">= 1.6.0"

  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 3.0"
    }
    kafka = {
      source  = "Mongey/kafka"
      version = "~> 0.7"
    }
    minio = {
      source  = "aminueza/minio"
      version = "~> 2.0"
    }
  }
}

provider "docker" {
  host = "unix:///var/run/docker.sock"
}

provider "kafka" {
  bootstrap_servers = var.kafka_bootstrap_servers
  skip_tls_verify   = true
}

provider "minio" {
  minio_server   = var.minio_endpoint
  minio_user     = var.minio_root_user
  minio_password = var.minio_root_password
  minio_ssl      = false
}
```

### 5.3 Module Structure

```
infra/terraform/
  main.tf                    # Root module composition
  providers.tf               # Provider configuration
  variables.tf               # Root variable definitions
  outputs.tf                 # Root outputs
  terraform.tfvars           # Default variable values (gitignored)
  terraform.tfvars.example   # Template (committed)
  backend.tf                 # State backend config

  modules/
    kafka/
      main.tf                # Kafka topic resources
      variables.tf           # Topic names, partition counts, retention
      outputs.tf             # Topic ARNs / IDs
    flink/
      main.tf                # Flink configuration resources (if managed outside K8s)
      variables.tf
      outputs.tf
    storage/
      main.tf                # MinIO buckets, lifecycle policies, access policies
      variables.tf
      outputs.tf
    monitoring/
      main.tf                # Prometheus scrape targets, Grafana data sources
      variables.tf
      outputs.tf
    governance/
      main.tf                # OpenMetadata configuration, ingestion pipelines
      variables.tf
      outputs.tf
```

### 5.4 Kafka Module Example

```hcl
# infra/terraform/modules/kafka/main.tf

resource "kafka_topic" "raw_trades" {
  name               = "raw-trades"
  partitions         = var.topic_partitions
  replication_factor = var.replication_factor

  config = {
    "retention.ms"          = var.retention_ms
    "cleanup.policy"        = "delete"
    "compression.type"      = "lz4"
    "segment.bytes"         = "1073741824"   # 1 GB
    "min.insync.replicas"   = "2"
  }
}

resource "kafka_topic" "processed_trades" {
  name               = "processed-trades"
  partitions         = var.topic_partitions
  replication_factor = var.replication_factor

  config = {
    "retention.ms"          = var.retention_ms
    "cleanup.policy"        = "delete"
    "compression.type"      = "lz4"
    "min.insync.replicas"   = "2"
  }
}

resource "kafka_topic" "trade_anomalies" {
  name               = "trade-anomalies"
  partitions         = var.anomaly_topic_partitions
  replication_factor = var.replication_factor

  config = {
    "retention.ms"        = var.anomaly_retention_ms
    "cleanup.policy"      = "delete"
    "compression.type"    = "lz4"
    "min.insync.replicas" = "2"
  }
}

resource "kafka_topic" "trade_aggregates" {
  name               = "trade-aggregates"
  partitions         = var.topic_partitions
  replication_factor = var.replication_factor

  config = {
    "retention.ms"          = "-1"  # Infinite retention for aggregates
    "cleanup.policy"        = "compact"
    "compression.type"      = "lz4"
    "min.insync.replicas"   = "2"
  }
}

resource "kafka_topic" "dead_letter_queue" {
  name               = "dead-letter-queue"
  partitions         = 3
  replication_factor = var.replication_factor

  config = {
    "retention.ms"        = "604800000"   # 7 days
    "cleanup.policy"      = "delete"
    "min.insync.replicas" = "1"
  }
}
```

### 5.5 Storage Module Example

```hcl
# infra/terraform/modules/storage/main.tf

resource "minio_s3_bucket" "iceberg_warehouse" {
  bucket = "iceberg-warehouse"
  acl    = "private"
}

resource "minio_s3_bucket" "raw_data" {
  bucket = "raw-data"
  acl    = "private"
}

resource "minio_s3_bucket" "checkpoints" {
  bucket = "checkpoints"
  acl    = "private"
}

resource "minio_s3_bucket" "backups" {
  bucket = "backups"
  acl    = "private"
}

resource "minio_s3_bucket_policy" "iceberg_warehouse_policy" {
  bucket = minio_s3_bucket.iceberg_warehouse.bucket
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect    = "Allow"
        Principal = { AWS = ["*"] }
        Action    = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource  = [
          "arn:aws:s3:::iceberg-warehouse",
          "arn:aws:s3:::iceberg-warehouse/*"
        ]
      }
    ]
  })
}

# Lifecycle rule: move old raw data to deep storage after 30 days
resource "minio_s3_bucket_lifecycle_rule" "raw_data_lifecycle" {
  bucket = minio_s3_bucket.raw_data.bucket

  rule {
    id      = "archive-old-raw-data"
    enabled = true
    expiration {
      days = 90
    }
  }
}
```

### 5.6 State Management

```hcl
# infra/terraform/backend.tf

# Local backend for local development
terraform {
  backend "local" {
    path = "infra/terraform/state/terraform.tfstate"
  }
}

# For team/production use, switch to S3-compatible backend (MinIO)
# terraform {
#   backend "s3" {
#     bucket         = "terraform-state"
#     key            = "de-project/terraform.tfstate"
#     region         = "us-east-1"
#     endpoint       = "http://localhost:9000"
#     force_path_style = true
#     skip_credentials_validation = true
#     skip_metadata_api_check     = true
#     skip_region_validation      = true
#   }
# }
```

### 5.7 Variable Definitions per Environment

```hcl
# infra/terraform/environments/local.tfvars
kafka_bootstrap_servers = ["localhost:9092"]
minio_endpoint          = "localhost:9000"
minio_root_user         = "minio"
minio_root_password     = "minio123"
topic_partitions        = 6
replication_factor      = 3
retention_ms            = "604800000"  # 7 days

# infra/terraform/environments/dev.tfvars
topic_partitions        = 6
replication_factor      = 3
retention_ms            = "259200000"  # 3 days

# infra/terraform/environments/prod.tfvars
topic_partitions        = 12
replication_factor      = 3
retention_ms            = "2592000000" # 30 days
```

---

## 6. CI/CD Pipeline

### 6.1 GitHub Actions Workflow Structure

```
.github/
  workflows/
    ci.yml              # Triggered on every PR
    cd-dev.yml          # Triggered on merge to develop
    cd-prod.yml         # Triggered on merge to main (with approval gate)
  actions/
    setup-python/
      action.yml        # Composite action for Python setup
    setup-docker/
      action.yml        # Composite action for Docker buildx + caching
```

### 6.2 ci.yml -- Continuous Integration

```yaml
name: CI

on:
  pull_request:
    branches: [develop, main]
  push:
    branches: [develop]

concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

env:
  PYTHON_VERSION: "3.11"
  JAVA_VERSION: "11"

jobs:
  # ── Stage 1: Code Quality ─────────────────────────────
  lint:
    name: Lint & Format
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}
      - name: Install tools
        run: pip install ruff mypy black
      - name: Ruff lint
        run: ruff check . --output-format=github
      - name: Ruff format check
        run: ruff format --check .
      - name: Black format check
        run: black --check .

  typecheck:
    name: Type Check
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}
      - name: Install dependencies
        run: |
          pip install uv
          uv sync --all-packages
      - name: mypy
        run: mypy services/ libs/ --ignore-missing-imports

  # ── Stage 2: Unit Tests ───────────────────────────────
  test:
    name: Unit Tests
    runs-on: ubuntu-latest
    needs: [lint]
    strategy:
      matrix:
        service:
          - trade-generator
          - kafka-consumer
          - dagster-orchestrator
          - graphql-api
          - flink-processor
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}
      - name: Install dependencies
        run: |
          pip install uv
          cd services/${{ matrix.service }}
          uv sync --frozen
      - name: Run tests
        run: |
          cd services/${{ matrix.service }}
          pytest tests/unit -v --junitxml=reports/junit.xml --cov=src --cov-report=xml
      - name: Upload coverage
        uses: actions/upload-artifact@v4
        with:
          name: coverage-${{ matrix.service }}
          path: services/${{ matrix.service }}/reports/

  # ── Stage 3: Integration Tests ────────────────────────
  integration-test:
    name: Integration Tests
    runs-on: ubuntu-latest
    needs: [test]
    services:
      kafka:
        image: confluentinc/cp-kafka:7.6.0
        ports:
          - 9092:9092
        env:
          KAFKA_NODE_ID: 1
          KAFKA_PROCESS_ROLES: controller,broker
          KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
          KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
          KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
          KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
          CLUSTER_ID: test-cluster-id-001
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ env.PYTHON_VERSION }}
      - name: Run integration tests
        run: |
          pip install uv
          uv sync --all-packages
          pytest tests/integration -v --timeout=120

  # ── Stage 4: Docker Build ─────────────────────────────
  docker-build:
    name: Build Docker Images
    runs-on: ubuntu-latest
    needs: [test]
    strategy:
      matrix:
        service:
          - trade-generator
          - kafka-consumer
          - dagster-orchestrator
          - graphql-api
          - flink-processor
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v5
        with:
          context: .
          file: services/${{ matrix.service }}/Dockerfile
          push: false
          load: true
          tags: de-project/${{ matrix.service }}:${{ github.sha }}
          cache-from: type=gha
          cache-to: type=gha,mode=max

  # ── Stage 5: Security Scan ────────────────────────────
  security-scan:
    name: Security Scan
    runs-on: ubuntu-latest
    needs: [docker-build]
    strategy:
      matrix:
        service:
          - trade-generator
          - kafka-consumer
          - dagster-orchestrator
          - graphql-api
          - flink-processor
    steps:
      - uses: actions/checkout@v4
      - name: Run Trivy
        uses: aquasecurity/trivy-action@master
        with:
          image-ref: de-project/${{ matrix.service }}:${{ github.sha }}
          format: sarif
          output: trivy-${{ matrix.service }}.sarif
          severity: CRITICAL,HIGH
          exit-code: 1
      - name: Upload SARIF
        uses: github/codeql-action/upload-sarif@v3
        if: always()
        with:
          sarif_file: trivy-${{ matrix.service }}.sarif
```

### 6.3 cd-dev.yml -- Deploy to Dev

```yaml
name: CD - Dev

on:
  push:
    branches: [develop]

jobs:
  deploy-dev:
    name: Deploy to Dev
    runs-on: ubuntu-latest
    environment: dev
    steps:
      - uses: actions/checkout@v4

      - name: Build and tag images
        run: |
          for service in trade-generator kafka-consumer dagster-orchestrator graphql-api flink-processor; do
            docker build -t de-project/${service}:dev -f services/${service}/Dockerfile .
          done

      - name: Deploy via Docker Compose
        run: |
          cp infra/env/.env.dev .env
          docker compose \
            -f infra/docker-compose/docker-compose.base.yml \
            -f infra/docker-compose/docker-compose.kafka.yml \
            -f infra/docker-compose/docker-compose.flink.yml \
            -f infra/docker-compose/docker-compose.storage.yml \
            -f infra/docker-compose/docker-compose.dagster.yml \
            -f infra/docker-compose/docker-compose.services.yml \
            up -d --remove-orphans

      - name: Run smoke tests
        run: |
          sleep 30
          scripts/health-check.sh --env dev

      - name: Apply Terraform
        run: |
          cd infra/terraform
          terraform init
          terraform apply -var-file=environments/dev.tfvars -auto-approve
```

### 6.4 cd-prod.yml -- Deploy to Prod

```yaml
name: CD - Prod

on:
  push:
    branches: [main]

jobs:
  deploy-prod:
    name: Deploy to Prod
    runs-on: ubuntu-latest
    environment:
      name: prod
      url: https://de.example.com
    steps:
      - uses: actions/checkout@v4

      - name: Build and tag images
        run: |
          VERSION=$(git describe --tags --always)
          for service in trade-generator kafka-consumer dagster-orchestrator graphql-api flink-processor; do
            docker build -t de-project/${service}:${VERSION} -f services/${service}/Dockerfile .
            docker tag de-project/${service}:${VERSION} de-project/${service}:latest
          done

      - name: Blue-Green Deploy
        run: |
          scripts/blue-green-deploy.sh --env prod --version ${VERSION}

      - name: Run smoke tests
        run: |
          scripts/health-check.sh --env prod

      - name: Rollback on failure
        if: failure()
        run: |
          scripts/rollback.sh --env prod
```

### 6.5 Pipeline Stages Summary

```
PR Created / Push to develop
  |
  +-- [Stage 1] Code Quality (parallel)
  |     +-- ruff lint
  |     +-- ruff format check
  |     +-- black format check
  |     +-- mypy type check
  |
  +-- [Stage 2] Unit Tests (parallel per service, depends on Stage 1)
  |     +-- trade-generator tests
  |     +-- kafka-consumer tests
  |     +-- dagster-orchestrator tests
  |     +-- graphql-api tests
  |     +-- flink-processor tests
  |
  +-- [Stage 3] Integration Tests (depends on Stage 2)
  |     +-- Kafka integration
  |     +-- End-to-end pipeline test
  |
  +-- [Stage 4] Docker Build (parallel per service, depends on Stage 2)
  |
  +-- [Stage 5] Security Scan (parallel per service, depends on Stage 4)

Merge to develop -> cd-dev.yml
Merge to main -> cd-prod.yml (requires approval)
```

### 6.6 Blue-Green Deployment Strategy

For pipeline updates (especially Flink jobs and Kafka consumers), blue-green deployment ensures zero data loss:

1. **Blue** (current active) continues processing.
2. **Green** (new version) is deployed alongside Blue.
3. Green starts consuming from the same Kafka consumer group with a new group ID (or from latest offset).
4. Health checks and smoke tests run against Green.
5. If Green is healthy, traffic switches: Blue is drained gracefully (Flink savepoint triggered) and stopped.
6. If Green fails, it is torn down and Blue continues unchanged.

**Implementation**: `scripts/blue-green-deploy.sh` manages this lifecycle via Docker Compose service scaling or Kubernetes Deployment label switching.

### 6.7 Rollback Procedures

| Scenario | Rollback Action |
|----------|----------------|
| Bad application code | Redeploy previous Docker image tag via `scripts/rollback.sh --service <name> --version <prev-tag>` |
| Bad Kafka topic config | `terraform apply` with previous tfvars (state tracked in git) |
| Bad Flink job | Restore from last Flink savepoint: `flink run -s <savepoint-path> <job.jar>` |
| Bad Dagster pipeline | Dagster supports run rollback; disable the schedule and re-run previous materialization |
| Infrastructure failure | Full `terraform destroy` + `terraform apply` (idempotent) |

---

## 7. Environment Promotion

### 7.1 dev -> staging -> prod Workflow

```
Feature Branch
  |
  | (PR + CI passes)
  v
develop branch  ──>  Dev Environment
  |                    - Full stack locally or on shared dev cluster
  | (manual promote    - Kafka: 3 brokers, RF=3
  |  or scheduled)     - Flink: 1 TM with 4 slots
  v                    - Lower resource limits
staging branch  ──>  Staging Environment
  |                    - Mirrors prod topology at 50% scale
  | (manual promote    - Kafka: 3 brokers, RF=3
  |  with approval)    - Flink: 2 TMs with 4 slots each
  v                    - Production-grade monitoring
main branch     ──>  Prod Environment
                       - Full scale
                       - Kafka: 3+ brokers, RF=3
                       - Flink: 3+ TMs
                       - Full monitoring + alerting
```

### 7.2 Configuration Differences per Environment

| Configuration | Dev | Staging | Prod |
|--------------|-----|---------|------|
| Kafka partitions per topic | 6 | 6 | 12 |
| Kafka retention | 7 days | 7 days | 30 days |
| Flink checkpointing interval | 60s | 30s | 10s |
| Flink parallelism | 4 | 8 | 16 |
| MinIO/S3 bucket lifecycle | 30 days | 60 days | 90 days |
| Trade generator rate | 100 trades/s | 1,000 trades/s | N/A (real feed) |
| Dagster schedule interval | 5 min | 1 min | 1 min |
| Log level | DEBUG | INFO | WARNING |
| Grafana alerting | Disabled | Slack (test channel) | Slack + PagerDuty |
| Replicas (graphql-api) | 1 | 2 | 3-8 (HPA) |
| Replicas (kafka-consumer) | 1 | 2 | 3-6 (HPA) |
| Data quality checks | Warn only | Warn + log | Fail + alert |

### 7.3 Data Isolation Between Environments

- **Kafka**: Each environment has its own Kafka cluster (separate broker instances). No topic sharing.
- **MinIO/S3**: Each environment has its own set of buckets with distinct prefixes (`dev/iceberg-warehouse`, `staging/iceberg-warehouse`, `prod/iceberg-warehouse`). In local dev, all environments share the same MinIO instance but use separate buckets.
- **Databases**: Each environment has its own PostgreSQL instance (Dagster, Superset). Schemas are never shared.
- **Iceberg catalog**: Separate catalog namespaces per environment (`de_dev.trades`, `de_staging.trades`, `de_prod.trades`).

### 7.4 Feature Flags for Gradual Rollouts

Feature flags are managed via environment variables and a simple JSON config file:

```json
// infra/feature-flags/flags.json
{
  "enable_anomaly_detection": {
    "dev": true,
    "staging": true,
    "prod": false
  },
  "enable_real_time_aggregations": {
    "dev": true,
    "staging": true,
    "prod": true
  },
  "enable_scd_type3": {
    "dev": true,
    "staging": false,
    "prod": false
  },
  "trade_generator_symbols": {
    "dev": ["AAPL", "GOOGL", "MSFT"],
    "staging": ["AAPL", "GOOGL", "MSFT", "AMZN", "META", "TSLA", "NVDA", "JPM", "V", "WMT"],
    "prod": "ALL"
  }
}
```

Services read flags at startup and on a configurable refresh interval. This allows enabling new Flink processing pipelines or Dagster assets incrementally.

---

## 8. Scripts & Automation

All scripts live in the `scripts/` directory at the repo root.

### 8.1 setup.sh -- One-Command Local Environment Setup

```bash
#!/usr/bin/env bash
# scripts/setup.sh
# One-command setup for the entire local development environment.
# Usage: ./scripts/setup.sh [--skip-colima] [--skip-build] [--minimal]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Steps:
# 1. Check prerequisites (brew, docker, colima, python, java, etc.)
# 2. Install missing tools via Homebrew (reads Brewfile)
# 3. Start Colima with project profile (if not already running)
# 4. Copy .env.example to .env.local if not present
# 5. Build all Docker images (make build)
# 6. Start infrastructure services (Kafka, MinIO, databases)
# 7. Wait for infrastructure health checks to pass
# 8. Run Terraform to create Kafka topics and MinIO buckets
# 9. Run database migrations
# 10. Start application services
# 11. Run final health check
# 12. Print service URLs and credentials
```

### 8.2 teardown.sh & Granular Module Teardown Scripts

> **Full teardown plan with complete script implementations**: See **[plan/12-teardown-scripts.md](./12-teardown-scripts.md)**

The teardown system provides 3 levels of control:

| Level | Command | Effect |
|-------|---------|--------|
| **Global stop** | `make teardown` | Stop all services in reverse-dependency order, preserve data |
| **Global destroy** | `make teardown-destroy` | Stop all + delete all Docker volumes (data lost) |
| **Nuclear reset** | `make teardown-nuclear` | Stop all + delete volumes + images + networks + build cache |
| **Per-module stop** | `make down-{module}` | Stop one module only, preserve data |
| **Per-module destroy** | `make destroy-{module}` | Stop one module + delete its data volumes |
| **Data-only wipe** | `make wipe-{layer}` | Delete data while services stay running |
| **Full rebuild** | `make fresh-start` | Nuclear teardown + complete setup from scratch |

**Scripts** (all in `scripts/`):
- `_teardown-common.sh` — Shared helpers (colors, logging, volume destroy, dry-run)
- `teardown.sh` — Global orchestrator, calls all module scripts in order
- `teardown-{services,superset,dagster,flink,governance,monitoring,kafka,storage}.sh` — Per-module
- `teardown-k8s.sh` — Kubernetes namespace teardown
- `teardown-terraform.sh` — Terraform destroy (all or by module)
- `wipe-data.sh` — Data-only wipe (per Iceberg layer, Kafka topics, Dagster runs)

**Key design principles**: Idempotent, reverse-dependency ordered, data-aware (stop vs destroy), confirmations for destructive ops, automatic backups before destroy, dry-run support.

### 8.3 health-check.sh -- Service Health Verification

```bash
#!/usr/bin/env bash
# scripts/health-check.sh
# Verify all services are healthy.
# Usage: ./scripts/health-check.sh [--env local|dev|staging|prod] [--service <name>]

# Checks:
# - Kafka: broker connectivity, topic list, consumer group status
# - Schema Registry: /subjects endpoint
# - Flink: /overview endpoint, running jobs count
# - MinIO: bucket list, read/write test
# - Iceberg REST: /v1/config endpoint
# - Dagster: /server_info endpoint
# - Prometheus: /-/ready endpoint, scrape targets up
# - Grafana: /api/health endpoint
# - OpenMetadata: /api/v1/system/version endpoint
# - trade-generator: WebSocket connectivity
# - kafka-consumer: consumer group lag check
# - graphql-api: /health endpoint, sample query
# - Superset: /health endpoint

# Output: Color-coded table with service name, status (OK/FAIL), response time
```

### 8.4 migrate.sh -- Database Migrations

```bash
#!/usr/bin/env bash
# scripts/migrate.sh
# Run database migrations for all services that use persistent stores.
# Usage: ./scripts/migrate.sh [--service dagster|superset|openmetadata]

# Migrations:
# - Dagster PostgreSQL: dagster instance migrate
# - Superset PostgreSQL: superset db upgrade
# - OpenMetadata MySQL: managed by OpenMetadata server on startup
# - Iceberg catalog: schema evolution handled via Iceberg API
```

### 8.5 backup.sh and restore.sh

```bash
#!/usr/bin/env bash
# scripts/backup.sh
# Back up all persistent data to MinIO backup bucket.
# Usage: ./scripts/backup.sh [--service <name>] [--output <path>]

# Backups:
# - Kafka: topic metadata (not data -- it's ephemeral by design)
# - Dagster PostgreSQL: pg_dump
# - Superset PostgreSQL: pg_dump
# - OpenMetadata MySQL: mysqldump
# - MinIO Iceberg warehouse: mc mirror to backup bucket
# - Flink savepoints: copy to backup bucket
# - Prometheus data: snapshot API
# - Grafana dashboards: export via API

# ─────────────────────────────────────────────────────────

#!/usr/bin/env bash
# scripts/restore.sh
# Restore data from backup.
# Usage: ./scripts/restore.sh --from <backup-path|backup-timestamp> [--service <name>]

# Restore order (dependencies matter):
# 1. Databases first (PostgreSQL, MySQL)
# 2. MinIO data (Iceberg warehouse)
# 3. Flink savepoints
# 4. Grafana dashboards
# 5. Restart services
```

### 8.6 Additional Utility Scripts

```
scripts/
  setup.sh                    # Full local environment setup
  teardown.sh                 # Clean shutdown
  health-check.sh             # Health verification for all services
  migrate.sh                  # Database migrations
  backup.sh                   # Backup all persistent data
  restore.sh                  # Restore from backup
  blue-green-deploy.sh        # Blue-green deployment orchestration
  rollback.sh                 # Rollback to previous version
  create-kafka-topics.sh      # Create Kafka topics (alternative to Terraform)
  reset-consumer-offsets.sh   # Reset Kafka consumer group offsets
  flink-submit-job.sh         # Submit a Flink job with proper classpath
  flink-savepoint.sh          # Trigger Flink savepoint
  generate-certs.sh           # Generate self-signed TLS certs for local dev
  seed-data.sh                # Seed reference data (stock symbols, exchanges)
  port-forward.sh             # K8s port-forwarding for all services
  log-collector.sh            # Collect logs from all services into a tarball
```

---

## 9. Implementation Steps (Ordered, with File Paths)

Below is the ordered list of every file that needs to be created. The paths are relative to the monorepo root (`DE-project/`). Files are grouped by implementation phase.

### Phase 1: Repository Foundation

| # | File Path | Description |
|---|-----------|-------------|
| 1 | `.gitignore` | Git ignore rules (venvs, .env files, terraform state, __pycache__, etc.) |
| 2 | `.pre-commit-config.yaml` | Pre-commit hooks (ruff, black, trailing whitespace, YAML lint) |
| 3 | `Brewfile` | Homebrew dependencies for macOS developer setup |
| 4 | `.tool-versions` | asdf version pinning (Python 3.11, Java 11, Node 18) |
| 5 | `Makefile` | Root Makefile with all targets listed in section 1.4 |
| 6 | `.editorconfig` | Editor configuration (indent style, line endings) |
| 7 | `pyproject.toml` | Root Python project config (workspace-level ruff/mypy/black settings) |

### Phase 2: Environment Configuration

| # | File Path | Description |
|---|-----------|-------------|
| 8 | `infra/env/.env.example` | Template environment variables (committed) |
| 9 | `infra/env/.env.local` | Local development values (gitignored, created by setup.sh) |
| 10 | `infra/env/.env.dev` | Dev environment overrides |
| 11 | `infra/env/.env.staging` | Staging environment overrides |
| 12 | `infra/env/.env.prod` | Production environment overrides (no secrets) |
| 13 | `infra/feature-flags/flags.json` | Feature flag definitions per environment |

### Phase 3: Docker Base Images

| # | File Path | Description |
|---|-----------|-------------|
| 14 | `docker/base-python/Dockerfile` | Common Python 3.11 base image with tini and uv |
| 15 | `docker/base-python/.dockerignore` | Docker build context ignore for base image |

### Phase 4: Service Dockerfiles

| # | File Path | Description |
|---|-----------|-------------|
| 16 | `services/trade-generator/Dockerfile` | Multi-stage Dockerfile for trade-generator |
| 17 | `services/trade-generator/.dockerignore` | Build context ignore |
| 18 | `services/kafka-consumer/Dockerfile` | Multi-stage Dockerfile for kafka-consumer |
| 19 | `services/kafka-consumer/.dockerignore` | Build context ignore |
| 20 | `services/flink-processor/Dockerfile` | Multi-stage Dockerfile (Flink + PyFlink) |
| 21 | `services/flink-processor/.dockerignore` | Build context ignore |
| 22 | `services/dagster-orchestrator/Dockerfile` | Multi-stage Dockerfile for Dagster |
| 23 | `services/dagster-orchestrator/.dockerignore` | Build context ignore |
| 24 | `services/graphql-api/Dockerfile` | Multi-stage Dockerfile for GraphQL API |
| 25 | `services/graphql-api/.dockerignore` | Build context ignore |

### Phase 5: Docker Compose Files

| # | File Path | Description |
|---|-----------|-------------|
| 26 | `infra/docker-compose/docker-compose.base.yml` | Shared networks and volumes |
| 27 | `infra/docker-compose/docker-compose.kafka.yml` | Zookeeper, 3 Kafka brokers, Schema Registry, Kafka UI |
| 28 | `infra/docker-compose/docker-compose.flink.yml` | Flink JobManager and TaskManager |
| 29 | `infra/docker-compose/docker-compose.storage.yml` | MinIO, minio-init, Iceberg REST catalog |
| 30 | `infra/docker-compose/docker-compose.dagster.yml` | Dagster PostgreSQL, webserver, daemon |
| 31 | `infra/docker-compose/docker-compose.monitoring.yml` | Prometheus, Grafana, Node Exporter |
| 32 | `infra/docker-compose/docker-compose.governance.yml` | OpenMetadata, MySQL, Elasticsearch |
| 33 | `infra/docker-compose/docker-compose.services.yml` | trade-generator, kafka-consumer, graphql-api |
| 34 | `infra/docker-compose/docker-compose.superset.yml` | Superset, Redis, PostgreSQL |

### Phase 6: Monitoring Configuration

| # | File Path | Description |
|---|-----------|-------------|
| 35 | `infra/prometheus/prometheus.yml` | Prometheus scrape configuration |
| 36 | `infra/prometheus/alerts/kafka.yml` | Kafka alerting rules |
| 37 | `infra/prometheus/alerts/flink.yml` | Flink alerting rules |
| 38 | `infra/prometheus/alerts/pipeline.yml` | Pipeline freshness/volume alerting rules |
| 39 | `infra/grafana/provisioning/datasources/prometheus.yml` | Auto-provision Prometheus data source |
| 40 | `infra/grafana/provisioning/dashboards/dashboard.yml` | Dashboard provisioning config |
| 41 | `infra/grafana/dashboards/kafka-overview.json` | Kafka cluster dashboard |
| 42 | `infra/grafana/dashboards/flink-overview.json` | Flink cluster dashboard |
| 43 | `infra/grafana/dashboards/pipeline-health.json` | Pipeline health dashboard |
| 44 | `infra/grafana/dashboards/data-quality.json` | Data quality metrics dashboard |

### Phase 7: Superset Configuration

| # | File Path | Description |
|---|-----------|-------------|
| 45 | `infra/superset/superset_config.py` | Superset configuration (DB URI, Redis, secret key) |
| 46 | `infra/superset/bootstrap.sh` | Superset init script (admin user, DB init) |

### Phase 8: Terraform

| # | File Path | Description |
|---|-----------|-------------|
| 47 | `infra/terraform/main.tf` | Root module composing all child modules |
| 48 | `infra/terraform/providers.tf` | Provider configuration (Docker, Kafka, MinIO) |
| 49 | `infra/terraform/variables.tf` | Root variable definitions |
| 50 | `infra/terraform/outputs.tf` | Root output definitions |
| 51 | `infra/terraform/backend.tf` | State backend configuration |
| 52 | `infra/terraform/terraform.tfvars.example` | Variable template (committed) |
| 53 | `infra/terraform/environments/local.tfvars` | Local dev variable values |
| 54 | `infra/terraform/environments/dev.tfvars` | Dev variable values |
| 55 | `infra/terraform/environments/staging.tfvars` | Staging variable values |
| 56 | `infra/terraform/environments/prod.tfvars` | Prod variable values |
| 57 | `infra/terraform/modules/kafka/main.tf` | Kafka topics resource definitions |
| 58 | `infra/terraform/modules/kafka/variables.tf` | Kafka module variables |
| 59 | `infra/terraform/modules/kafka/outputs.tf` | Kafka module outputs |
| 60 | `infra/terraform/modules/storage/main.tf` | MinIO buckets, policies, lifecycle rules |
| 61 | `infra/terraform/modules/storage/variables.tf` | Storage module variables |
| 62 | `infra/terraform/modules/storage/outputs.tf` | Storage module outputs |
| 63 | `infra/terraform/modules/flink/main.tf` | Flink configuration resources |
| 64 | `infra/terraform/modules/flink/variables.tf` | Flink module variables |
| 65 | `infra/terraform/modules/flink/outputs.tf` | Flink module outputs |
| 66 | `infra/terraform/modules/monitoring/main.tf` | Monitoring configuration resources |
| 67 | `infra/terraform/modules/monitoring/variables.tf` | Monitoring module variables |
| 68 | `infra/terraform/modules/monitoring/outputs.tf` | Monitoring module outputs |
| 69 | `infra/terraform/modules/governance/main.tf` | Governance configuration resources |
| 70 | `infra/terraform/modules/governance/variables.tf` | Governance module variables |
| 71 | `infra/terraform/modules/governance/outputs.tf` | Governance module outputs |

### Phase 9: Kubernetes Manifests

| # | File Path | Description |
|---|-----------|-------------|
| 72 | `infra/k8s/namespaces/de-dev.yaml` | Dev namespace + ResourceQuota + LimitRange |
| 73 | `infra/k8s/namespaces/de-staging.yaml` | Staging namespace + ResourceQuota + LimitRange |
| 74 | `infra/k8s/namespaces/de-prod.yaml` | Prod namespace + ResourceQuota + LimitRange |
| 75 | `infra/k8s/base/kafka/statefulset.yaml` | Kafka StatefulSet (3 replicas) |
| 76 | `infra/k8s/base/kafka/service.yaml` | Kafka headless + ClusterIP services |
| 77 | `infra/k8s/base/kafka/configmap.yaml` | Kafka broker configuration |
| 78 | `infra/k8s/base/kafka/pvc.yaml` | Kafka PersistentVolumeClaim templates |
| 79 | `infra/k8s/base/kafka/network-policy.yaml` | Kafka network policy |
| 80 | `infra/k8s/base/flink/deployment-jobmanager.yaml` | Flink JobManager Deployment |
| 81 | `infra/k8s/base/flink/deployment-taskmanager.yaml` | Flink TaskManager Deployment |
| 82 | `infra/k8s/base/flink/service.yaml` | Flink services (RPC, REST UI) |
| 83 | `infra/k8s/base/flink/configmap.yaml` | Flink configuration |
| 84 | `infra/k8s/base/storage/deployment-minio.yaml` | MinIO Deployment |
| 85 | `infra/k8s/base/storage/deployment-iceberg-rest.yaml` | Iceberg REST catalog Deployment |
| 86 | `infra/k8s/base/storage/service.yaml` | Storage services |
| 87 | `infra/k8s/base/storage/pvc.yaml` | MinIO PersistentVolumeClaim |
| 88 | `infra/k8s/base/storage/job-init-buckets.yaml` | Job to initialize MinIO buckets |
| 89 | `infra/k8s/base/dagster/deployment-webserver.yaml` | Dagster webserver Deployment |
| 90 | `infra/k8s/base/dagster/deployment-daemon.yaml` | Dagster daemon Deployment |
| 91 | `infra/k8s/base/dagster/statefulset-postgres.yaml` | Dagster PostgreSQL StatefulSet |
| 92 | `infra/k8s/base/dagster/service.yaml` | Dagster services |
| 93 | `infra/k8s/base/dagster/configmap.yaml` | Dagster configuration |
| 94 | `infra/k8s/base/dagster/secret.yaml` | Dagster secrets (DB passwords) |
| 95 | `infra/k8s/base/monitoring/deployment-prometheus.yaml` | Prometheus Deployment |
| 96 | `infra/k8s/base/monitoring/deployment-grafana.yaml` | Grafana Deployment |
| 97 | `infra/k8s/base/monitoring/daemonset-node-exporter.yaml` | Node Exporter DaemonSet |
| 98 | `infra/k8s/base/monitoring/service.yaml` | Monitoring services |
| 99 | `infra/k8s/base/monitoring/configmap-prometheus.yaml` | Prometheus scrape config |
| 100 | `infra/k8s/base/monitoring/configmap-grafana-dashboards.yaml` | Grafana dashboard provisioning |
| 101 | `infra/k8s/base/governance/deployment-openmetadata.yaml` | OpenMetadata Deployment |
| 102 | `infra/k8s/base/governance/statefulset-mysql.yaml` | OpenMetadata MySQL StatefulSet |
| 103 | `infra/k8s/base/governance/statefulset-elasticsearch.yaml` | Elasticsearch StatefulSet |
| 104 | `infra/k8s/base/governance/service.yaml` | Governance services |
| 105 | `infra/k8s/base/governance/configmap.yaml` | Governance configuration |
| 106 | `infra/k8s/base/services/deployment-trade-generator.yaml` | trade-generator Deployment |
| 107 | `infra/k8s/base/services/deployment-kafka-consumer.yaml` | kafka-consumer Deployment |
| 108 | `infra/k8s/base/services/deployment-graphql-api.yaml` | graphql-api Deployment |
| 109 | `infra/k8s/base/services/service.yaml` | Application services |
| 110 | `infra/k8s/base/services/hpa.yaml` | HPA for graphql-api and kafka-consumer |
| 111 | `infra/k8s/base/superset/deployment.yaml` | Superset Deployment |
| 112 | `infra/k8s/base/superset/statefulset-postgres.yaml` | Superset PostgreSQL StatefulSet |
| 113 | `infra/k8s/base/superset/deployment-redis.yaml` | Superset Redis Deployment |
| 114 | `infra/k8s/base/superset/service.yaml` | Superset services |
| 115 | `infra/k8s/base/superset/configmap.yaml` | Superset configuration |
| 116 | `infra/k8s/base/ingress/ingress.yaml` | Ingress resource for all user-facing UIs |
| 117 | `infra/k8s/base/kustomization.yaml` | Base Kustomize file referencing all resources |
| 118 | `infra/k8s/overlays/dev/kustomization.yaml` | Dev overlay (reduced replicas, lower resources) |
| 119 | `infra/k8s/overlays/staging/kustomization.yaml` | Staging overlay |
| 120 | `infra/k8s/overlays/prod/kustomization.yaml` | Prod overlay (full scale) |

### Phase 10: CI/CD

| # | File Path | Description |
|---|-----------|-------------|
| 121 | `.github/workflows/ci.yml` | CI pipeline (lint, test, build, scan) |
| 122 | `.github/workflows/cd-dev.yml` | Deploy to dev on merge to develop |
| 123 | `.github/workflows/cd-prod.yml` | Deploy to prod on merge to main |
| 124 | `.github/actions/setup-python/action.yml` | Composite action for Python setup |
| 125 | `.github/actions/setup-docker/action.yml` | Composite action for Docker buildx |

### Phase 11: Scripts

| # | File Path | Description |
|---|-----------|-------------|
| 126 | `scripts/setup.sh` | One-command local environment setup |
| 127 | `scripts/teardown.sh` | Clean shutdown with optional data removal |
| 128 | `scripts/health-check.sh` | Service health verification |
| 129 | `scripts/migrate.sh` | Database migrations |
| 130 | `scripts/backup.sh` | Backup persistent data |
| 131 | `scripts/restore.sh` | Restore from backup |
| 132 | `scripts/blue-green-deploy.sh` | Blue-green deployment orchestration |
| 133 | `scripts/rollback.sh` | Rollback to previous version |
| 134 | `scripts/create-kafka-topics.sh` | Create Kafka topics (shell alternative to TF) |
| 135 | `scripts/reset-consumer-offsets.sh` | Reset Kafka consumer group offsets |
| 136 | `scripts/flink-submit-job.sh` | Submit Flink job |
| 137 | `scripts/flink-savepoint.sh` | Trigger Flink savepoint |
| 138 | `scripts/generate-certs.sh` | Generate self-signed TLS certificates |
| 139 | `scripts/seed-data.sh` | Seed reference data |
| 140 | `scripts/port-forward.sh` | K8s port-forwarding for all services |
| 141 | `scripts/log-collector.sh` | Collect logs into tarball for debugging |

**Total files: 141**

---

## 10. Estimated Effort

### Task Breakdown with Relative Sizing

Sizes use T-shirt sizing: **XS** (< 1 hour), **S** (1-3 hours), **M** (3-8 hours), **L** (1-2 days), **XL** (2-4 days).

| Phase | Task | Size | Estimated Hours |
|-------|------|------|----------------|
| **1. Repository Foundation** | | | |
| | .gitignore, .editorconfig, pyproject.toml | XS | 0.5 |
| | Brewfile and .tool-versions | XS | 0.5 |
| | Makefile (all targets) | M | 6 |
| | Pre-commit configuration | S | 1 |
| **2. Environment Config** | | | |
| | .env files (all environments) | S | 2 |
| | Feature flags JSON | XS | 0.5 |
| **3. Docker Base Image** | | | |
| | Python base Dockerfile | S | 1 |
| **4. Service Dockerfiles** | | | |
| | trade-generator Dockerfile | S | 1 |
| | kafka-consumer Dockerfile | S | 1 |
| | flink-processor Dockerfile | M | 4 |
| | dagster-orchestrator Dockerfile | S | 2 |
| | graphql-api Dockerfile | S | 1 |
| **5. Docker Compose** | | | |
| | docker-compose.base.yml | S | 1 |
| | docker-compose.kafka.yml (3 brokers + ecosystem) | L | 8 |
| | docker-compose.flink.yml | M | 4 |
| | docker-compose.storage.yml (MinIO + Iceberg) | M | 5 |
| | docker-compose.dagster.yml | M | 4 |
| | docker-compose.monitoring.yml | M | 4 |
| | docker-compose.governance.yml | M | 5 |
| | docker-compose.services.yml | S | 2 |
| | docker-compose.superset.yml | M | 4 |
| **6. Monitoring Config** | | | |
| | Prometheus config + alert rules | M | 6 |
| | Grafana provisioning + dashboards | L | 12 |
| **7. Terraform** | | | |
| | Provider + backend setup | S | 2 |
| | Kafka module | M | 4 |
| | Storage module | M | 4 |
| | Flink module | S | 2 |
| | Monitoring module | S | 2 |
| | Governance module | S | 2 |
| | Environment tfvars | S | 1 |
| **8. Kubernetes** | | | |
| | Namespace definitions + RBAC | S | 2 |
| | Kafka StatefulSet + services | L | 10 |
| | Flink Deployments + services | M | 6 |
| | Storage (MinIO, Iceberg) manifests | M | 5 |
| | Dagster manifests | M | 5 |
| | Monitoring manifests | M | 6 |
| | Governance manifests | M | 5 |
| | Application service manifests + HPA | M | 5 |
| | Superset manifests | M | 4 |
| | Ingress | S | 2 |
| | Kustomize overlays (dev, staging, prod) | M | 6 |
| **9. CI/CD** | | | |
| | ci.yml workflow | L | 10 |
| | cd-dev.yml workflow | M | 5 |
| | cd-prod.yml workflow | M | 5 |
| | Composite actions | S | 2 |
| **10. Scripts** | | | |
| | setup.sh | L | 10 |
| | teardown.sh | M | 4 |
| | health-check.sh | M | 6 |
| | migrate.sh | S | 2 |
| | backup.sh + restore.sh | L | 10 |
| | blue-green-deploy.sh | L | 12 |
| | rollback.sh | M | 5 |
| | Utility scripts (7 scripts) | M | 8 |

### Summary

| Phase | Total Hours |
|-------|------------|
| 1. Repository Foundation | 8 |
| 2. Environment Config | 2.5 |
| 3. Docker Base Image | 1 |
| 4. Service Dockerfiles | 9 |
| 5. Docker Compose | 37 |
| 6. Monitoring Config | 18 |
| 7. Terraform | 17 |
| 8. Kubernetes | 56 |
| 9. CI/CD | 22 |
| 10. Scripts | 57 |
| **TOTAL** | **~227.5 hours** |

### Recommended Implementation Order

The phases should be implemented in this order due to dependencies:

```
Phase 1 (Foundation)
  |
  v
Phase 2 (Environment Config)
  |
  v
Phase 3 (Base Image) --> Phase 4 (Service Dockerfiles)
                           |
                           v
                         Phase 5 (Docker Compose) --> Phase 6 (Monitoring Config)
                           |
                           v
                         Phase 7 (Terraform)
                           |
                           v
                         Phase 10 (Scripts -- setup.sh, teardown.sh, health-check.sh first)
                           |
                           v
                         Phase 8 (Kubernetes -- optional, can be deferred)
                           |
                           v
                         Phase 9 (CI/CD -- after local dev works end-to-end)
```

> **Critical path**: Phases 1-5 must be completed first to have a working local development environment. Everything else builds on top.

### Port Reference Table

For quick reference, here are all service ports as assigned in this plan:

| Port | Service | Protocol |
|------|---------|----------|
| 2181 | Zookeeper | TCP |
| 8000 | GraphQL API | HTTP |
| 8080 | Kafka UI | HTTP |
| 8081 | Schema Registry | HTTP |
| 8082 | Flink Web UI | HTTP |
| 8088 | Superset | HTTP |
| 8181 | Iceberg REST Catalog | HTTP |
| 8585 | OpenMetadata | HTTP |
| 8765 | trade-generator (WebSocket) | WS |
| 9000 | MinIO S3 API | HTTP |
| 9001 | MinIO Console | HTTP |
| 9090 | Prometheus | HTTP |
| 9092 | Kafka broker 1 (external) | TCP |
| 9093 | Kafka broker 2 (external) | TCP |
| 9094 | Kafka broker 3 (external) | TCP |
| 9100 | Node Exporter | HTTP |
| 9200 | Elasticsearch | HTTP |
| 3000 | Dagster Webserver | HTTP |
| 3001 | Grafana | HTTP |
| 3306 | OpenMetadata MySQL | TCP |
| 5432 | Dagster PostgreSQL | TCP |
| 5433 | Superset PostgreSQL | TCP |
| 6123 | Flink RPC | TCP |
| 6379 | Superset Redis | TCP |
