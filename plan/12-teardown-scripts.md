# Teardown Scripts — Global & Granular Module Teardown

> **Goal**: Be able to destroy everything (or any individual module) and rebuild from scratch without manual intervention. Every teardown is idempotent — safe to run even if the target is already down.

---

## Table of Contents

1. [Design Principles](#1-design-principles)
2. [Module Dependency Order](#2-module-dependency-order)
3. [Makefile Targets](#3-makefile-targets)
4. [Global Teardown Script](#4-global-teardown-script)
5. [Granular Module Teardown Scripts](#5-granular-module-teardown-scripts)
   - [5.1 Kafka Teardown](#51-kafka-teardown)
   - [5.2 Flink Teardown](#52-flink-teardown)
   - [5.3 Storage Teardown (MinIO + Iceberg)](#53-storage-teardown-minio--iceberg)
   - [5.4 Dagster Teardown](#54-dagster-teardown)
   - [5.5 Monitoring Teardown (Prometheus + Grafana)](#55-monitoring-teardown-prometheus--grafana)
   - [5.6 Governance Teardown (OpenMetadata)](#56-governance-teardown-openmetadata)
   - [5.7 Application Services Teardown](#57-application-services-teardown)
   - [5.8 Superset Teardown](#58-superset-teardown)
   - [5.9 Kubernetes Teardown](#59-kubernetes-teardown)
   - [5.10 Terraform Teardown](#510-terraform-teardown)
6. [Data-Only Wipe Scripts](#6-data-only-wipe-scripts)
7. [Nuclear Option — Full Reset](#7-nuclear-option--full-reset)
8. [Rebuild After Teardown](#8-rebuild-after-teardown)
9. [Implementation File List](#9-implementation-file-list)

---

## 1. Design Principles

| Principle | Detail |
|-----------|--------|
| **Idempotent** | Every script is safe to run repeatedly. If a container/volume doesn't exist, skip it without error. |
| **Ordered** | Teardown follows reverse-dependency order (consumers before producers, app layer before infra). |
| **Data-aware** | Separate flags for stopping services vs. destroying data volumes. Default = **stop only, preserve data**. |
| **Verbose** | Every step prints what it's doing with color-coded status (green=done, yellow=skipped, red=error). |
| **Composable** | Global teardown calls the granular scripts in order. Each granular script works standalone. |
| **Pre-checks** | Each script verifies prerequisites (Docker running, compose files exist) before proceeding. |

---

## 2. Module Dependency Order

Teardown must happen in **reverse dependency order** — stop consumers before producers, app layer before infrastructure.

```
Teardown order (top to bottom):
═══════════════════════════════════════════════

 Step 1:  Application Services      (trade-generator, kafka-consumer, graphql-api)
 Step 2:  Superset                  (visualization layer)
 Step 3:  Dagster                   (orchestration — daemon first, then webserver, then DB)
 Step 4:  Flink                     (savepoint first, then stop jobs, then cluster)
 Step 5:  Governance / OpenMetadata (server, elasticsearch, mysql)
 Step 6:  Monitoring                (grafana, alertmanager, prometheus, pushgateway, exporters)
 Step 7:  Kafka                     (consumers stop first → brokers → schema registry)
 Step 8:  Storage                   (iceberg REST catalog → MinIO)

═══════════════════════════════════════════════

Startup order is the exact reverse (bottom to top).
```

---

## 3. Makefile Targets

Add these targets to the root `Makefile` alongside the existing `up-*` targets:

```makefile
# ══════════════════════════════════════════════════════════════
# Teardown Targets
# ══════════════════════════════════════════════════════════════

# ── Global ──────────────────────────────────────────────────
teardown:                  ## Stop all services gracefully, preserve data
	@bash scripts/teardown.sh

teardown-destroy:          ## Stop all services AND delete all data volumes
	@bash scripts/teardown.sh --destroy-data

teardown-nuclear:          ## Full reset: stop all, remove volumes, images, networks, caches
	@bash scripts/teardown.sh --nuclear

# ── Granular Module Teardown (stop only, preserve data) ─────
down-services:             ## Stop application services (trade-generator, kafka-consumer, graphql-api)
	@bash scripts/teardown-services.sh

down-superset:             ## Stop Superset stack
	@bash scripts/teardown-superset.sh

down-dagster:              ## Stop Dagster orchestrator
	@bash scripts/teardown-dagster.sh

down-flink:                ## Stop Flink cluster (triggers savepoint first)
	@bash scripts/teardown-flink.sh

down-governance:           ## Stop OpenMetadata stack
	@bash scripts/teardown-governance.sh

down-monitoring:           ## Stop Prometheus + Grafana monitoring stack
	@bash scripts/teardown-monitoring.sh

down-kafka:                ## Stop Kafka cluster + Schema Registry
	@bash scripts/teardown-kafka.sh

down-storage:              ## Stop MinIO + Iceberg catalog
	@bash scripts/teardown-storage.sh

# ── Granular Module Teardown + Data Destroy ─────────────────
destroy-services:          ## Stop app services + remove any state
	@bash scripts/teardown-services.sh --destroy-data

destroy-superset:          ## Stop Superset + delete its PostgreSQL + Redis data
	@bash scripts/teardown-superset.sh --destroy-data

destroy-dagster:           ## Stop Dagster + delete PostgreSQL run/event/schedule data
	@bash scripts/teardown-dagster.sh --destroy-data

destroy-flink:             ## Stop Flink + delete checkpoints and savepoints
	@bash scripts/teardown-flink.sh --destroy-data

destroy-governance:        ## Stop OpenMetadata + delete MySQL + Elasticsearch data
	@bash scripts/teardown-governance.sh --destroy-data

destroy-monitoring:        ## Stop monitoring + delete Prometheus TSDB + Grafana state
	@bash scripts/teardown-monitoring.sh --destroy-data

destroy-kafka:             ## Stop Kafka + delete all topic data + Schema Registry
	@bash scripts/teardown-kafka.sh --destroy-data

destroy-storage:           ## Stop MinIO/Iceberg + delete ALL warehouse data (Bronze/Silver/Gold)
	@bash scripts/teardown-storage.sh --destroy-data

# ── Data-Only Wipe (services stay running) ──────────────────
wipe-bronze:               ## Delete only Bronze layer data, keep Silver/Gold
	@bash scripts/wipe-data.sh --layer bronze

wipe-silver:               ## Delete only Silver layer data, keep Bronze/Gold
	@bash scripts/wipe-data.sh --layer silver

wipe-gold:                 ## Delete only Gold layer data, keep Bronze/Silver
	@bash scripts/wipe-data.sh --layer gold

wipe-all-data:             ## Delete all Iceberg warehouse data (Bronze + Silver + Gold)
	@bash scripts/wipe-data.sh --layer all

wipe-kafka-topics:         ## Delete and recreate all Kafka topics (reset offsets)
	@bash scripts/wipe-data.sh --kafka

# ── Kubernetes Teardown ─────────────────────────────────────
k8s-teardown-dev:          ## Delete all resources in de-dev namespace
	@bash scripts/teardown-k8s.sh --env dev

k8s-teardown-staging:      ## Delete all resources in de-staging namespace
	@bash scripts/teardown-k8s.sh --env staging

k8s-teardown-all:          ## Delete all DE namespaces
	@bash scripts/teardown-k8s.sh --env all

# ── Terraform Teardown ──────────────────────────────────────
tf-destroy:                ## Destroy all Terraform-managed resources
	cd infrastructure/terraform && terraform destroy -auto-approve

# ── Rebuild shortcuts ──────────────────────────────────────
fresh-start:               ## Nuclear teardown + full setup from scratch
	@bash scripts/teardown.sh --nuclear && bash scripts/setup.sh

fresh-start-keep-images:   ## Destroy data + restart, but keep Docker images
	@bash scripts/teardown.sh --destroy-data && bash scripts/setup.sh
```

---

## 4. Global Teardown Script

**File**: `scripts/teardown.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────
# teardown.sh — Global teardown for the entire DE platform
# ──────────────────────────────────────────────────────────────
#
# Usage:
#   ./scripts/teardown.sh                   # Stop all services, keep data
#   ./scripts/teardown.sh --destroy-data    # Stop all + remove data volumes
#   ./scripts/teardown.sh --nuclear         # Full reset: volumes + images + networks + caches
#   ./scripts/teardown.sh --dry-run         # Show what would be done without executing
#
# This script calls each module teardown script in reverse-dependency order.
# ──────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Defaults ──────────────────────────────────────────────────
DESTROY_DATA=false
NUCLEAR=false
DRY_RUN=false

# ── Colors ────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# ── Logging helpers ───────────────────────────────────────────
info()    { echo -e "${BLUE}[INFO]${NC}  $1"; }
success() { echo -e "${GREEN}[DONE]${NC}  $1"; }
warn()    { echo -e "${YELLOW}[SKIP]${NC}  $1"; }
error()   { echo -e "${RED}[FAIL]${NC}  $1"; }
header()  { echo -e "\n${BOLD}${CYAN}═══ $1 ═══${NC}\n"; }

# ── Argument parsing ──────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --destroy-data) DESTROY_DATA=true; shift ;;
        --nuclear)      NUCLEAR=true; DESTROY_DATA=true; shift ;;
        --dry-run)      DRY_RUN=true; shift ;;
        -h|--help)
            echo "Usage: $0 [--destroy-data] [--nuclear] [--dry-run]"
            echo ""
            echo "Options:"
            echo "  --destroy-data   Remove all Docker volumes (data is lost)"
            echo "  --nuclear        Remove volumes + images + networks + build cache"
            echo "  --dry-run        Print actions without executing"
            exit 0
            ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ── Pre-checks ────────────────────────────────────────────────
if ! command -v docker &> /dev/null; then
    error "Docker is not installed or not in PATH"
    exit 1
fi

if ! docker info &> /dev/null 2>&1; then
    warn "Docker daemon is not running. Nothing to tear down."
    exit 0
fi

# ── Build flags to pass to module scripts ─────────────────────
MODULE_FLAGS=""
if $DESTROY_DATA; then MODULE_FLAGS="--destroy-data"; fi
if $DRY_RUN; then MODULE_FLAGS="$MODULE_FLAGS --dry-run"; fi

# ── Summary ───────────────────────────────────────────────────
header "DE Platform Teardown"
info "Mode: $(if $NUCLEAR; then echo 'NUCLEAR (full reset)'; elif $DESTROY_DATA; then echo 'DESTROY (stop + delete data)'; else echo 'GRACEFUL (stop only, preserve data)'; fi)"
if $DRY_RUN; then warn "DRY RUN — no changes will be made"; fi
echo ""

# ── Confirmation for destructive operations ───────────────────
if $DESTROY_DATA && ! $DRY_RUN; then
    echo -e "${RED}${BOLD}WARNING: This will permanently delete all data volumes!${NC}"
    if $NUCLEAR; then
        echo -e "${RED}${BOLD}NUCLEAR mode will also remove all Docker images and build cache.${NC}"
    fi
    read -p "Type 'yes' to confirm: " confirm
    if [[ "$confirm" != "yes" ]]; then
        info "Aborted by user."
        exit 0
    fi
fi

# ── Step 1: Application Services ─────────────────────────────
header "Step 1/8: Stopping Application Services"
if [[ -f "$SCRIPT_DIR/teardown-services.sh" ]]; then
    bash "$SCRIPT_DIR/teardown-services.sh" $MODULE_FLAGS
else
    warn "teardown-services.sh not found, attempting direct compose down"
    docker compose -f "$PROJECT_ROOT/infrastructure/docker-compose/docker-compose.services.yml" down --remove-orphans 2>/dev/null || warn "Services already stopped"
fi
success "Application services stopped"

# ── Step 2: Superset ─────────────────────────────────────────
header "Step 2/8: Stopping Superset"
if [[ -f "$SCRIPT_DIR/teardown-superset.sh" ]]; then
    bash "$SCRIPT_DIR/teardown-superset.sh" $MODULE_FLAGS
else
    docker compose -f "$PROJECT_ROOT/infrastructure/docker-compose/docker-compose.superset.yml" down --remove-orphans 2>/dev/null || warn "Superset already stopped"
fi
success "Superset stopped"

# ── Step 3: Dagster ──────────────────────────────────────────
header "Step 3/8: Stopping Dagster"
if [[ -f "$SCRIPT_DIR/teardown-dagster.sh" ]]; then
    bash "$SCRIPT_DIR/teardown-dagster.sh" $MODULE_FLAGS
else
    docker compose -f "$PROJECT_ROOT/infrastructure/docker-compose/docker-compose.dagster.yml" down --remove-orphans 2>/dev/null || warn "Dagster already stopped"
fi
success "Dagster stopped"

# ── Step 4: Flink ────────────────────────────────────────────
header "Step 4/8: Stopping Flink"
if [[ -f "$SCRIPT_DIR/teardown-flink.sh" ]]; then
    bash "$SCRIPT_DIR/teardown-flink.sh" $MODULE_FLAGS
else
    docker compose -f "$PROJECT_ROOT/infrastructure/docker-compose/docker-compose.flink.yml" down --remove-orphans 2>/dev/null || warn "Flink already stopped"
fi
success "Flink stopped"

# ── Step 5: Governance / OpenMetadata ────────────────────────
header "Step 5/8: Stopping Governance (OpenMetadata)"
if [[ -f "$SCRIPT_DIR/teardown-governance.sh" ]]; then
    bash "$SCRIPT_DIR/teardown-governance.sh" $MODULE_FLAGS
else
    docker compose -f "$PROJECT_ROOT/infrastructure/docker-compose/docker-compose.governance.yml" down --remove-orphans 2>/dev/null || warn "Governance already stopped"
fi
success "Governance stopped"

# ── Step 6: Monitoring ───────────────────────────────────────
header "Step 6/8: Stopping Monitoring"
if [[ -f "$SCRIPT_DIR/teardown-monitoring.sh" ]]; then
    bash "$SCRIPT_DIR/teardown-monitoring.sh" $MODULE_FLAGS
else
    docker compose -f "$PROJECT_ROOT/infrastructure/docker-compose/docker-compose.monitoring.yml" down --remove-orphans 2>/dev/null || warn "Monitoring already stopped"
fi
success "Monitoring stopped"

# ── Step 7: Kafka ────────────────────────────────────────────
header "Step 7/8: Stopping Kafka"
if [[ -f "$SCRIPT_DIR/teardown-kafka.sh" ]]; then
    bash "$SCRIPT_DIR/teardown-kafka.sh" $MODULE_FLAGS
else
    docker compose -f "$PROJECT_ROOT/infrastructure/docker-compose/docker-compose.kafka.yml" down --remove-orphans 2>/dev/null || warn "Kafka already stopped"
fi
success "Kafka stopped"

# ── Step 8: Storage ──────────────────────────────────────────
header "Step 8/8: Stopping Storage (MinIO + Iceberg)"
if [[ -f "$SCRIPT_DIR/teardown-storage.sh" ]]; then
    bash "$SCRIPT_DIR/teardown-storage.sh" $MODULE_FLAGS
else
    docker compose -f "$PROJECT_ROOT/infrastructure/docker-compose/docker-compose.storage.yml" down --remove-orphans 2>/dev/null || warn "Storage already stopped"
fi
success "Storage stopped"

# ── Nuclear: Remove images, networks, build cache ────────────
if $NUCLEAR && ! $DRY_RUN; then
    header "Nuclear Cleanup"

    info "Removing project Docker images..."
    docker images --format '{{.Repository}}:{{.Tag}}' | grep "^de-project" | while read -r img; do
        docker rmi "$img" 2>/dev/null && success "Removed image: $img" || warn "Could not remove: $img"
    done

    info "Removing dangling images..."
    docker image prune -f 2>/dev/null || true

    info "Removing orphaned project networks..."
    docker network ls --format '{{.Name}}' | grep "^de-" | while read -r net; do
        docker network rm "$net" 2>/dev/null && success "Removed network: $net" || warn "Could not remove: $net"
    done

    info "Pruning Docker build cache..."
    docker builder prune -f 2>/dev/null || true

    success "Nuclear cleanup complete"
fi

# ── Summary ──────────────────────────────────────────────────
header "Teardown Complete"
echo -e "  Services stopped:  ${GREEN}✓${NC}"
if $DESTROY_DATA; then
    echo -e "  Data volumes:      ${RED}DELETED${NC}"
else
    echo -e "  Data volumes:      ${GREEN}PRESERVED${NC}"
fi
if $NUCLEAR; then
    echo -e "  Docker images:     ${RED}DELETED${NC}"
    echo -e "  Build cache:       ${RED}DELETED${NC}"
    echo -e "  Networks:          ${RED}DELETED${NC}"
fi
echo ""
info "To rebuild: make setup  (or: make fresh-start)"
```

---

## 5. Granular Module Teardown Scripts

Each script follows the same pattern:
- Idempotent (safe to re-run)
- `--destroy-data` flag to remove volumes
- `--dry-run` flag to preview
- Color-coded output
- Pre-checks for Docker

### 5.1 Kafka Teardown

**File**: `scripts/teardown-kafka.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────
# teardown-kafka.sh — Stop Kafka cluster + Schema Registry
# ──────────────────────────────────────────────────────────────
# Usage:
#   ./scripts/teardown-kafka.sh                  # Stop only
#   ./scripts/teardown-kafka.sh --destroy-data   # Stop + delete topic data + SR schemas
# ──────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_teardown-common.sh"

COMPOSE_FILE="$PROJECT_ROOT/infrastructure/docker-compose/docker-compose.kafka.yml"

parse_args "$@"
require_docker

header "Kafka Teardown"

# Step 1: Export Schema Registry schemas (safety backup before destroy)
if $DESTROY_DATA && ! $DRY_RUN; then
    info "Backing up Schema Registry schemas..."
    BACKUP_DIR="$PROJECT_ROOT/.teardown-backups/schema-registry/$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$BACKUP_DIR"
    if curl -s http://localhost:8081/subjects 2>/dev/null | jq -r '.[]' 2>/dev/null | while read -r subject; do
        curl -s "http://localhost:8081/subjects/$subject/versions/latest" > "$BACKUP_DIR/$subject.json" 2>/dev/null
    done; then
        success "Schema Registry schemas backed up to $BACKUP_DIR"
    else
        warn "Schema Registry not reachable, skipping backup"
    fi
fi

# Step 2: Stop Kafka UI
run_step "Stopping Kafka UI..." \
    "docker compose -f $COMPOSE_FILE stop kafka-ui"

# Step 3: Stop Schema Registry
run_step "Stopping Schema Registry..." \
    "docker compose -f $COMPOSE_FILE stop schema-registry"

# Step 4: Stop Kafka brokers (in reverse order for clean shutdown)
for broker in kafka-broker-3 kafka-broker-2 kafka-broker-1; do
    run_step "Stopping $broker..." \
        "docker compose -f $COMPOSE_FILE stop $broker"
done

# Step 5: Remove containers
run_step "Removing Kafka containers..." \
    "docker compose -f $COMPOSE_FILE down --remove-orphans"

# Step 6: Destroy data if requested
if $DESTROY_DATA; then
    destroy_volumes "kafka-broker-1-data" "kafka-broker-2-data" "kafka-broker-3-data" \
                    "schema-registry-data"
fi

success "Kafka teardown complete"
summary "Kafka" "kafka-broker-1, kafka-broker-2, kafka-broker-3, schema-registry, kafka-ui"
```

**Volumes destroyed with `--destroy-data`**:
| Volume | Contents |
|--------|----------|
| `kafka-broker-1-data` | Topic partitions, consumer offsets, logs |
| `kafka-broker-2-data` | Topic partitions, consumer offsets, logs |
| `kafka-broker-3-data` | Topic partitions, consumer offsets, logs |
| `schema-registry-data` | Avro schemas and compatibility settings |

---

### 5.2 Flink Teardown

**File**: `scripts/teardown-flink.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────
# teardown-flink.sh — Stop Flink cluster with savepoint
# ──────────────────────────────────────────────────────────────
# Usage:
#   ./scripts/teardown-flink.sh                  # Savepoint + stop
#   ./scripts/teardown-flink.sh --destroy-data   # Stop + delete checkpoints/savepoints
#   ./scripts/teardown-flink.sh --skip-savepoint # Stop without triggering savepoint
# ──────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_teardown-common.sh"

COMPOSE_FILE="$PROJECT_ROOT/infrastructure/docker-compose/docker-compose.flink.yml"
SKIP_SAVEPOINT=false

parse_args "$@"
# Additional Flink-specific args
for arg in "$@"; do
    [[ "$arg" == "--skip-savepoint" ]] && SKIP_SAVEPOINT=true
done
require_docker

header "Flink Teardown"

FLINK_REST="http://localhost:8081"

# Step 1: Trigger savepoints for all running jobs
if ! $SKIP_SAVEPOINT && ! $DRY_RUN; then
    info "Checking for running Flink jobs..."
    JOBS=$(curl -s "$FLINK_REST/jobs" 2>/dev/null | jq -r '.jobs[] | select(.status == "RUNNING") | .id' 2>/dev/null || echo "")

    if [[ -n "$JOBS" ]]; then
        SAVEPOINT_DIR="$PROJECT_ROOT/.flink-savepoints/$(date +%Y%m%d_%H%M%S)"
        mkdir -p "$SAVEPOINT_DIR"
        for job_id in $JOBS; do
            info "Triggering savepoint for job $job_id..."
            RESULT=$(curl -s -X POST "$FLINK_REST/jobs/$job_id/savepoints" \
                -H "Content-Type: application/json" \
                -d "{\"cancel-job\": true, \"target-directory\": \"file://$SAVEPOINT_DIR\"}" 2>/dev/null || echo "")
            if [[ -n "$RESULT" ]]; then
                TRIGGER_ID=$(echo "$RESULT" | jq -r '.request-id' 2>/dev/null || echo "unknown")
                success "Savepoint triggered for job $job_id (trigger: $TRIGGER_ID)"
                # Wait for savepoint to complete (max 60s)
                for i in $(seq 1 12); do
                    STATUS=$(curl -s "$FLINK_REST/jobs/$job_id/savepoints/$TRIGGER_ID" 2>/dev/null | jq -r '.status.id' 2>/dev/null || echo "")
                    if [[ "$STATUS" == "COMPLETED" ]]; then
                        success "Savepoint completed for job $job_id"
                        break
                    fi
                    sleep 5
                done
            else
                warn "Could not trigger savepoint for job $job_id (Flink REST not reachable)"
            fi
        done
        info "Savepoints stored in: $SAVEPOINT_DIR"
    else
        warn "No running Flink jobs found"
    fi
else
    if $SKIP_SAVEPOINT; then warn "Skipping savepoints as requested"; fi
fi

# Step 2: Cancel any remaining running jobs
if ! $DRY_RUN; then
    REMAINING=$(curl -s "$FLINK_REST/jobs" 2>/dev/null | jq -r '.jobs[] | select(.status == "RUNNING") | .id' 2>/dev/null || echo "")
    for job_id in $REMAINING; do
        info "Cancelling job $job_id..."
        curl -s -X PATCH "$FLINK_REST/jobs/$job_id" 2>/dev/null || true
    done
fi

# Step 3: Stop TaskManagers first, then JobManager
run_step "Stopping Flink TaskManagers..." \
    "docker compose -f $COMPOSE_FILE stop flink-taskmanager"

run_step "Stopping Flink JobManager..." \
    "docker compose -f $COMPOSE_FILE stop flink-jobmanager"

# Step 4: Remove containers
run_step "Removing Flink containers..." \
    "docker compose -f $COMPOSE_FILE down --remove-orphans"

# Step 5: Destroy data if requested
if $DESTROY_DATA; then
    destroy_volumes "flink-checkpoints" "flink-savepoints" "flink-ha"
fi

success "Flink teardown complete"
summary "Flink" "flink-jobmanager, flink-taskmanager"
```

**Volumes destroyed with `--destroy-data`**:
| Volume | Contents |
|--------|----------|
| `flink-checkpoints` | Incremental RocksDB checkpoints |
| `flink-savepoints` | Manual savepoint snapshots |
| `flink-ha` | High-availability metadata (ZooKeeper) |

---

### 5.3 Storage Teardown (MinIO + Iceberg)

**File**: `scripts/teardown-storage.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────
# teardown-storage.sh — Stop MinIO + Iceberg REST catalog
# ──────────────────────────────────────────────────────────────
# Usage:
#   ./scripts/teardown-storage.sh                  # Stop only (data preserved in volumes)
#   ./scripts/teardown-storage.sh --destroy-data   # Stop + delete ALL warehouse data
#
# ⚠️  WARNING: --destroy-data removes the ENTIRE Iceberg warehouse
#     including Bronze, Silver, Gold layers and all Parquet files.
# ──────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_teardown-common.sh"

COMPOSE_FILE="$PROJECT_ROOT/infrastructure/docker-compose/docker-compose.storage.yml"

parse_args "$@"
require_docker

header "Storage Teardown (MinIO + Iceberg)"

# Step 1: Extra confirmation for storage destruction
if $DESTROY_DATA && ! $DRY_RUN; then
    echo -e "${RED}${BOLD}⚠️  THIS WILL DELETE ALL ICEBERG WAREHOUSE DATA ⚠️${NC}"
    echo -e "${RED}    Bronze, Silver, Gold layers — ALL Parquet files — ALL table metadata${NC}"
    read -p "Type 'destroy-warehouse' to confirm: " confirm
    if [[ "$confirm" != "destroy-warehouse" ]]; then
        info "Aborted. Data preserved."
        exit 0
    fi
fi

# Step 2: Stop Iceberg REST catalog first (depends on MinIO)
run_step "Stopping Iceberg REST catalog..." \
    "docker compose -f $COMPOSE_FILE stop iceberg-rest"

# Step 3: Stop MinIO
run_step "Stopping MinIO..." \
    "docker compose -f $COMPOSE_FILE stop minio"

# Step 4: Remove containers
run_step "Removing storage containers..." \
    "docker compose -f $COMPOSE_FILE down --remove-orphans"

# Step 5: Destroy data if requested
if $DESTROY_DATA; then
    destroy_volumes "minio-data" "iceberg-catalog-data"
fi

success "Storage teardown complete"
summary "Storage" "minio, iceberg-rest"
```

**Volumes destroyed with `--destroy-data`**:
| Volume | Contents |
|--------|----------|
| `minio-data` | All Parquet files, Iceberg metadata, manifests — the entire warehouse |
| `iceberg-catalog-data` | REST catalog state (table registry, namespace definitions) |

---

### 5.4 Dagster Teardown

**File**: `scripts/teardown-dagster.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────
# teardown-dagster.sh — Stop Dagster orchestrator
# ──────────────────────────────────────────────────────────────
# Usage:
#   ./scripts/teardown-dagster.sh                  # Stop only
#   ./scripts/teardown-dagster.sh --destroy-data   # Stop + delete run history, event logs, schedules
# ──────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_teardown-common.sh"

COMPOSE_FILE="$PROJECT_ROOT/infrastructure/docker-compose/docker-compose.dagster.yml"

parse_args "$@"
require_docker

header "Dagster Teardown"

# Step 1: Stop Dagster daemon (handles sensors + schedules)
run_step "Stopping Dagster daemon..." \
    "docker compose -f $COMPOSE_FILE stop dagster-daemon"

# Step 2: Stop Dagster webserver (Dagit UI)
run_step "Stopping Dagster webserver..." \
    "docker compose -f $COMPOSE_FILE stop dagster-webserver"

# Step 3: Stop Dagster code server
run_step "Stopping Dagster code server..." \
    "docker compose -f $COMPOSE_FILE stop dagster-code"

# Step 4: Backup PostgreSQL before destroying (if applicable)
if $DESTROY_DATA && ! $DRY_RUN; then
    info "Backing up Dagster PostgreSQL..."
    BACKUP_DIR="$PROJECT_ROOT/.teardown-backups/dagster-pg/$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$BACKUP_DIR"
    docker compose -f "$COMPOSE_FILE" exec -T dagster-postgres \
        pg_dump -U dagster dagster > "$BACKUP_DIR/dagster_backup.sql" 2>/dev/null \
        && success "Dagster DB backed up to $BACKUP_DIR" \
        || warn "Could not backup Dagster DB (may already be stopped)"
fi

# Step 5: Stop Dagster PostgreSQL
run_step "Stopping Dagster PostgreSQL..." \
    "docker compose -f $COMPOSE_FILE stop dagster-postgres"

# Step 6: Remove containers
run_step "Removing Dagster containers..." \
    "docker compose -f $COMPOSE_FILE down --remove-orphans"

# Step 7: Destroy data if requested
if $DESTROY_DATA; then
    destroy_volumes "dagster-postgres-data"
fi

success "Dagster teardown complete"
summary "Dagster" "dagster-daemon, dagster-webserver, dagster-code, dagster-postgres"
```

**Volumes destroyed with `--destroy-data`**:
| Volume | Contents |
|--------|----------|
| `dagster-postgres-data` | Run history, event logs, schedule state, sensor ticks |

---

### 5.5 Monitoring Teardown (Prometheus + Grafana)

**File**: `scripts/teardown-monitoring.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────
# teardown-monitoring.sh — Stop Prometheus + Grafana + Alertmanager
# ──────────────────────────────────────────────────────────────
# Usage:
#   ./scripts/teardown-monitoring.sh                  # Stop only
#   ./scripts/teardown-monitoring.sh --destroy-data   # Stop + delete metrics TSDB + dashboards
# ──────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_teardown-common.sh"

COMPOSE_FILE="$PROJECT_ROOT/infrastructure/docker-compose/docker-compose.monitoring.yml"

parse_args "$@"
require_docker

header "Monitoring Teardown"

# Step 1: Export Grafana dashboards before destroying
if $DESTROY_DATA && ! $DRY_RUN; then
    info "Exporting Grafana dashboards..."
    BACKUP_DIR="$PROJECT_ROOT/.teardown-backups/grafana/$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$BACKUP_DIR"
    GRAFANA_URL="http://localhost:3000"
    GRAFANA_CREDS="admin:admin"
    DASHBOARDS=$(curl -s -u "$GRAFANA_CREDS" "$GRAFANA_URL/api/search?type=dash-db" 2>/dev/null | jq -r '.[].uid' 2>/dev/null || echo "")
    for uid in $DASHBOARDS; do
        curl -s -u "$GRAFANA_CREDS" "$GRAFANA_URL/api/dashboards/uid/$uid" > "$BACKUP_DIR/$uid.json" 2>/dev/null
    done
    if [[ -n "$DASHBOARDS" ]]; then
        success "Grafana dashboards backed up to $BACKUP_DIR"
    else
        warn "No Grafana dashboards to export (Grafana may not be running)"
    fi
fi

# Step 2: Stop exporters first (leaf nodes)
run_step "Stopping Node Exporter and cAdvisor..." \
    "docker compose -f $COMPOSE_FILE stop node-exporter cadvisor"

# Step 3: Stop Pushgateway
run_step "Stopping Prometheus Pushgateway..." \
    "docker compose -f $COMPOSE_FILE stop pushgateway"

# Step 4: Stop Grafana
run_step "Stopping Grafana..." \
    "docker compose -f $COMPOSE_FILE stop grafana"

# Step 5: Stop Alertmanager
run_step "Stopping Alertmanager..." \
    "docker compose -f $COMPOSE_FILE stop alertmanager"

# Step 6: Stop Prometheus
run_step "Stopping Prometheus..." \
    "docker compose -f $COMPOSE_FILE stop prometheus"

# Step 7: Remove containers
run_step "Removing monitoring containers..." \
    "docker compose -f $COMPOSE_FILE down --remove-orphans"

# Step 8: Destroy data if requested
if $DESTROY_DATA; then
    destroy_volumes "prometheus-data" "grafana-data" "alertmanager-data"
fi

success "Monitoring teardown complete"
summary "Monitoring" "prometheus, alertmanager, grafana, pushgateway, node-exporter, cadvisor"
```

**Volumes destroyed with `--destroy-data`**:
| Volume | Contents |
|--------|----------|
| `prometheus-data` | Time-series database (TSDB), metric history |
| `grafana-data` | Dashboard definitions, user preferences, alert states |
| `alertmanager-data` | Silence rules, notification log |

---

### 5.6 Governance Teardown (OpenMetadata)

**File**: `scripts/teardown-governance.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────
# teardown-governance.sh — Stop OpenMetadata stack
# ──────────────────────────────────────────────────────────────
# Usage:
#   ./scripts/teardown-governance.sh                  # Stop only
#   ./scripts/teardown-governance.sh --destroy-data   # Stop + delete catalog, search index, DB
# ──────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_teardown-common.sh"

COMPOSE_FILE="$PROJECT_ROOT/infrastructure/docker-compose/docker-compose.governance.yml"

parse_args "$@"
require_docker

header "Governance Teardown (OpenMetadata)"

# Step 1: Stop OpenMetadata ingestion workers
run_step "Stopping OpenMetadata ingestion..." \
    "docker compose -f $COMPOSE_FILE stop openmetadata-ingestion"

# Step 2: Stop OpenMetadata server
run_step "Stopping OpenMetadata server..." \
    "docker compose -f $COMPOSE_FILE stop openmetadata-server"

# Step 3: Stop Elasticsearch
run_step "Stopping Elasticsearch..." \
    "docker compose -f $COMPOSE_FILE stop openmetadata-elasticsearch"

# Step 4: Stop MySQL
run_step "Stopping OpenMetadata MySQL..." \
    "docker compose -f $COMPOSE_FILE stop openmetadata-mysql"

# Step 5: Remove containers
run_step "Removing governance containers..." \
    "docker compose -f $COMPOSE_FILE down --remove-orphans"

# Step 6: Destroy data if requested
if $DESTROY_DATA; then
    destroy_volumes "openmetadata-mysql-data" "openmetadata-es-data"
fi

success "Governance teardown complete"
summary "Governance" "openmetadata-server, openmetadata-ingestion, openmetadata-elasticsearch, openmetadata-mysql"
```

**Volumes destroyed with `--destroy-data`**:
| Volume | Contents |
|--------|----------|
| `openmetadata-mysql-data` | Data catalog, lineage, policies, glossary, tags |
| `openmetadata-es-data` | Search index for data discovery |

---

### 5.7 Application Services Teardown

**File**: `scripts/teardown-services.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────
# teardown-services.sh — Stop application services
# ──────────────────────────────────────────────────────────────
# Usage:
#   ./scripts/teardown-services.sh                  # Stop all app services
#   ./scripts/teardown-services.sh --destroy-data   # Stop + remove any local state
#   ./scripts/teardown-services.sh --only <name>    # Stop only a specific service
#     Supported names: trade-generator, kafka-consumer, graphql-api
# ──────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_teardown-common.sh"

COMPOSE_FILE="$PROJECT_ROOT/infrastructure/docker-compose/docker-compose.services.yml"
ONLY_SERVICE=""

parse_args "$@"
# Parse --only flag
for i in $(seq 1 $#); do
    if [[ "${!i}" == "--only" ]]; then
        next=$((i + 1))
        ONLY_SERVICE="${!next:-}"
    fi
done
require_docker

header "Application Services Teardown"

if [[ -n "$ONLY_SERVICE" ]]; then
    run_step "Stopping $ONLY_SERVICE..." \
        "docker compose -f $COMPOSE_FILE stop $ONLY_SERVICE"
    run_step "Removing $ONLY_SERVICE container..." \
        "docker compose -f $COMPOSE_FILE rm -f $ONLY_SERVICE"
else
    # Stop in order: consumers of data first, then producers
    run_step "Stopping GraphQL API..." \
        "docker compose -f $COMPOSE_FILE stop graphql-api"

    run_step "Stopping Kafka Consumer (WebSocket bridge)..." \
        "docker compose -f $COMPOSE_FILE stop kafka-consumer"

    run_step "Stopping Trade Generator..." \
        "docker compose -f $COMPOSE_FILE stop trade-generator"

    # Remove all containers
    run_step "Removing service containers..." \
        "docker compose -f $COMPOSE_FILE down --remove-orphans"
fi

# App services are stateless — no volumes to destroy
# But clean up any Redis cache used by GraphQL
if $DESTROY_DATA; then
    info "Flushing Redis cache (if running)..."
    docker exec de-redis redis-cli FLUSHALL 2>/dev/null \
        && success "Redis cache flushed" \
        || warn "Redis not running or not reachable"
fi

success "Application services teardown complete"
summary "App Services" "trade-generator, kafka-consumer, graphql-api"
```

---

### 5.8 Superset Teardown

**File**: `scripts/teardown-superset.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────
# teardown-superset.sh — Stop Apache Superset stack
# ──────────────────────────────────────────────────────────────
# Usage:
#   ./scripts/teardown-superset.sh                  # Stop only
#   ./scripts/teardown-superset.sh --destroy-data   # Stop + delete dashboards, saved queries, DB
# ──────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_teardown-common.sh"

COMPOSE_FILE="$PROJECT_ROOT/infrastructure/docker-compose/docker-compose.superset.yml"

parse_args "$@"
require_docker

header "Superset Teardown"

# Step 1: Stop Celery workers (background tasks)
run_step "Stopping Superset Celery workers..." \
    "docker compose -f $COMPOSE_FILE stop superset-worker superset-beat"

# Step 2: Stop Superset web server
run_step "Stopping Superset web server..." \
    "docker compose -f $COMPOSE_FILE stop superset"

# Step 3: Stop Redis (Celery broker + cache)
run_step "Stopping Superset Redis..." \
    "docker compose -f $COMPOSE_FILE stop superset-redis"

# Step 4: Stop PostgreSQL (metadata store)
run_step "Stopping Superset PostgreSQL..." \
    "docker compose -f $COMPOSE_FILE stop superset-postgres"

# Step 5: Remove containers
run_step "Removing Superset containers..." \
    "docker compose -f $COMPOSE_FILE down --remove-orphans"

# Step 6: Destroy data if requested
if $DESTROY_DATA; then
    destroy_volumes "superset-postgres-data" "superset-redis-data"
fi

success "Superset teardown complete"
summary "Superset" "superset, superset-worker, superset-beat, superset-redis, superset-postgres"
```

**Volumes destroyed with `--destroy-data`**:
| Volume | Contents |
|--------|----------|
| `superset-postgres-data` | Dashboards, charts, saved queries, user accounts, permissions |
| `superset-redis-data` | Celery task queue, query cache |

---

### 5.9 Kubernetes Teardown

**File**: `scripts/teardown-k8s.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────
# teardown-k8s.sh — Delete Kubernetes resources
# ──────────────────────────────────────────────────────────────
# Usage:
#   ./scripts/teardown-k8s.sh --env dev          # Delete de-dev namespace resources
#   ./scripts/teardown-k8s.sh --env staging      # Delete de-staging namespace resources
#   ./scripts/teardown-k8s.sh --env all          # Delete all DE namespaces
#   ./scripts/teardown-k8s.sh --env dev --delete-pvc  # Also delete PersistentVolumeClaims
#   ./scripts/teardown-k8s.sh --env dev --delete-ns   # Delete the entire namespace
# ──────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_teardown-common.sh"

TARGET_ENV=""
DELETE_PVC=false
DELETE_NS=false
K8S_DIR="$PROJECT_ROOT/infrastructure/k8s"

# Parse arguments
for arg in "$@"; do
    case "$arg" in
        --env) ;; # handled below
        dev|staging|prod|all) TARGET_ENV="$arg" ;;
        --delete-pvc) DELETE_PVC=true ;;
        --delete-ns) DELETE_NS=true; DELETE_PVC=true ;;
    esac
done
# Also handle --env <val> pair
while [[ $# -gt 0 ]]; do
    case "$1" in
        --env) TARGET_ENV="$2"; shift 2 ;;
        *) shift ;;
    esac
done

if [[ -z "$TARGET_ENV" ]]; then
    error "Must specify --env (dev|staging|prod|all)"
    exit 1
fi

if ! command -v kubectl &> /dev/null; then
    error "kubectl is not installed"
    exit 1
fi

header "Kubernetes Teardown (env: $TARGET_ENV)"

teardown_namespace() {
    local ns="de-$1"

    if ! kubectl get namespace "$ns" &>/dev/null; then
        warn "Namespace $ns does not exist, skipping"
        return
    fi

    info "Tearing down namespace: $ns"

    # Scale down deployments first for graceful shutdown
    info "Scaling down all deployments in $ns..."
    kubectl get deployments -n "$ns" -o name 2>/dev/null | while read -r dep; do
        kubectl scale "$dep" -n "$ns" --replicas=0 2>/dev/null || true
    done

    # Delete all workloads
    run_step "Deleting Deployments..." \
        "kubectl delete deployments --all -n $ns --timeout=60s"

    run_step "Deleting StatefulSets..." \
        "kubectl delete statefulsets --all -n $ns --timeout=60s"

    run_step "Deleting Jobs and CronJobs..." \
        "kubectl delete jobs,cronjobs --all -n $ns --timeout=30s"

    run_step "Deleting Services and Ingresses..." \
        "kubectl delete services,ingresses --all -n $ns --timeout=30s"

    run_step "Deleting ConfigMaps and Secrets..." \
        "kubectl delete configmaps,secrets --all -n $ns --timeout=30s"

    run_step "Deleting HPAs..." \
        "kubectl delete hpa --all -n $ns --timeout=30s"

    if $DELETE_PVC; then
        run_step "Deleting PersistentVolumeClaims (DATA LOSS)..." \
            "kubectl delete pvc --all -n $ns --timeout=60s"
    fi

    if $DELETE_NS; then
        run_step "Deleting namespace $ns..." \
            "kubectl delete namespace $ns --timeout=120s"
    fi

    success "Namespace $ns torn down"
}

if [[ "$TARGET_ENV" == "all" ]]; then
    for env in dev staging prod; do
        teardown_namespace "$env"
    done
else
    teardown_namespace "$TARGET_ENV"
fi

success "Kubernetes teardown complete"
```

---

### 5.10 Terraform Teardown

**File**: `scripts/teardown-terraform.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────
# teardown-terraform.sh — Destroy Terraform-managed resources
# ──────────────────────────────────────────────────────────────
# Usage:
#   ./scripts/teardown-terraform.sh                     # Destroy all TF resources
#   ./scripts/teardown-terraform.sh --module kafka      # Destroy only Kafka module
#   ./scripts/teardown-terraform.sh --module storage    # Destroy only storage module
#   ./scripts/teardown-terraform.sh --plan-only         # Show destroy plan without executing
# ──────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_teardown-common.sh"

TF_DIR="$PROJECT_ROOT/infrastructure/terraform"
TARGET_MODULE=""
PLAN_ONLY=false

parse_args "$@"
for arg in "$@"; do
    case "$arg" in
        --module) ;; # next arg is the module name
        kafka|storage|flink|monitoring|governance) TARGET_MODULE="$arg" ;;
        --plan-only) PLAN_ONLY=true ;;
    esac
done
# Handle --module <val>
while [[ $# -gt 0 ]]; do
    case "$1" in
        --module) TARGET_MODULE="$2"; shift 2 ;;
        *) shift ;;
    esac
done

if ! command -v terraform &> /dev/null; then
    error "Terraform is not installed"
    exit 1
fi

header "Terraform Teardown"

cd "$TF_DIR"

# Ensure state is initialized
terraform init -input=false 2>/dev/null || true

if [[ -n "$TARGET_MODULE" ]]; then
    info "Targeting module: $TARGET_MODULE"
    TF_TARGET="-target=module.$TARGET_MODULE"
else
    TF_TARGET=""
    info "Destroying ALL Terraform-managed resources"
fi

if $PLAN_ONLY; then
    info "Plan-only mode (no changes):"
    terraform plan -destroy $TF_TARGET
else
    terraform destroy -auto-approve $TF_TARGET
fi

success "Terraform teardown complete"
```

---

## 6. Data-Only Wipe Scripts

Wipe data while services keep running — useful for resetting to clean state without full teardown.

**File**: `scripts/wipe-data.sh`

```bash
#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────
# wipe-data.sh — Delete data without stopping services
# ──────────────────────────────────────────────────────────────
# Usage:
#   ./scripts/wipe-data.sh --layer bronze          # Wipe Bronze layer only
#   ./scripts/wipe-data.sh --layer silver          # Wipe Silver layer only
#   ./scripts/wipe-data.sh --layer gold            # Wipe Gold layer only
#   ./scripts/wipe-data.sh --layer all             # Wipe all Iceberg data
#   ./scripts/wipe-data.sh --kafka                 # Delete + recreate Kafka topics
#   ./scripts/wipe-data.sh --dagster-runs          # Purge Dagster run history
#   ./scripts/wipe-data.sh --quality-results       # Purge quality validation results
#   ./scripts/wipe-data.sh --everything            # All of the above
# ──────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/_teardown-common.sh"

LAYER=""
WIPE_KAFKA=false
WIPE_DAGSTER=false
WIPE_QUALITY=false
WIPE_EVERYTHING=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --layer) LAYER="$2"; shift 2 ;;
        --kafka) WIPE_KAFKA=true; shift ;;
        --dagster-runs) WIPE_DAGSTER=true; shift ;;
        --quality-results) WIPE_QUALITY=true; shift ;;
        --everything) WIPE_EVERYTHING=true; shift ;;
        *) shift ;;
    esac
done

if $WIPE_EVERYTHING; then
    LAYER="all"
    WIPE_KAFKA=true
    WIPE_DAGSTER=true
    WIPE_QUALITY=true
fi

header "Data Wipe"

MINIO_ALIAS="local"
MINIO_URL="http://localhost:9000"
WAREHOUSE_BUCKET="warehouse"

# ── Wipe Iceberg layers ──────────────────────────────────────
if [[ -n "$LAYER" ]]; then
    # Ensure mc (MinIO client) is configured
    mc alias set "$MINIO_ALIAS" "$MINIO_URL" minioadmin minioadmin 2>/dev/null || true

    case "$LAYER" in
        bronze)
            echo -e "${RED}Wiping Bronze layer...${NC}"
            read -p "Confirm (yes/no): " confirm
            [[ "$confirm" == "yes" ]] || { info "Aborted"; exit 0; }
            mc rm --recursive --force "$MINIO_ALIAS/$WAREHOUSE_BUCKET/bronze/" 2>/dev/null \
                && success "Bronze layer wiped" \
                || warn "Bronze layer already empty or MinIO not reachable"
            ;;
        silver)
            echo -e "${RED}Wiping Silver layer...${NC}"
            read -p "Confirm (yes/no): " confirm
            [[ "$confirm" == "yes" ]] || { info "Aborted"; exit 0; }
            mc rm --recursive --force "$MINIO_ALIAS/$WAREHOUSE_BUCKET/silver/" 2>/dev/null \
                && success "Silver layer wiped" \
                || warn "Silver layer already empty or MinIO not reachable"
            ;;
        gold)
            echo -e "${RED}Wiping Gold layer...${NC}"
            read -p "Confirm (yes/no): " confirm
            [[ "$confirm" == "yes" ]] || { info "Aborted"; exit 0; }
            mc rm --recursive --force "$MINIO_ALIAS/$WAREHOUSE_BUCKET/gold/" 2>/dev/null \
                && success "Gold layer wiped" \
                || warn "Gold layer already empty or MinIO not reachable"
            ;;
        all)
            echo -e "${RED}${BOLD}Wiping ALL Iceberg data (Bronze + Silver + Gold + dimensions)...${NC}"
            read -p "Type 'wipe-all' to confirm: " confirm
            [[ "$confirm" == "wipe-all" ]] || { info "Aborted"; exit 0; }
            mc rm --recursive --force "$MINIO_ALIAS/$WAREHOUSE_BUCKET/" 2>/dev/null \
                && success "All warehouse data wiped" \
                || warn "Warehouse already empty or MinIO not reachable"
            # Recreate empty bucket structure
            mc mb "$MINIO_ALIAS/$WAREHOUSE_BUCKET" 2>/dev/null || true
            mc mb "$MINIO_ALIAS/$WAREHOUSE_BUCKET/bronze" 2>/dev/null || true
            mc mb "$MINIO_ALIAS/$WAREHOUSE_BUCKET/silver" 2>/dev/null || true
            mc mb "$MINIO_ALIAS/$WAREHOUSE_BUCKET/gold" 2>/dev/null || true
            mc mb "$MINIO_ALIAS/$WAREHOUSE_BUCKET/quality" 2>/dev/null || true
            success "Empty bucket structure recreated"
            ;;
    esac
fi

# ── Wipe Kafka topics ────────────────────────────────────────
if $WIPE_KAFKA; then
    info "Deleting and recreating Kafka topics..."

    KAFKA_CONTAINER="kafka-broker-1"
    KAFKA_CMD="docker exec $KAFKA_CONTAINER kafka-topics"
    BOOTSTRAP="localhost:9092"

    # List of all project topics
    TOPICS=(
        "raw.trades"
        "raw.orderbook"
        "raw.marketdata"
        "processed.trades.new"
        "processed.trades.filled"
        "processed.trades.cancelled"
        "processed.trades.rejected"
        "analytics.trade-aggregates"
        "analytics.orderbook"
        "enriched.trades"
        "alerts.price-movement"
        "dlq.raw.failed"
    )

    for topic in "${TOPICS[@]}"; do
        $KAFKA_CMD --bootstrap-server "$BOOTSTRAP" --delete --topic "$topic" 2>/dev/null \
            && success "Deleted topic: $topic" \
            || warn "Topic $topic doesn't exist or already deleted"
    done

    sleep 3  # Wait for deletions to propagate

    # Recreate topics with original settings
    info "Recreating topics..."
    $KAFKA_CMD --bootstrap-server "$BOOTSTRAP" --create --topic raw.trades --partitions 12 --replication-factor 3 --config retention.ms=604800000 --config min.insync.replicas=2 2>/dev/null
    $KAFKA_CMD --bootstrap-server "$BOOTSTRAP" --create --topic raw.orderbook --partitions 12 --replication-factor 3 --config retention.ms=604800000 --config min.insync.replicas=2 2>/dev/null
    $KAFKA_CMD --bootstrap-server "$BOOTSTRAP" --create --topic raw.marketdata --partitions 12 --replication-factor 3 --config retention.ms=604800000 --config min.insync.replicas=2 2>/dev/null
    $KAFKA_CMD --bootstrap-server "$BOOTSTRAP" --create --topic processed.trades.new --partitions 12 --replication-factor 3 --config retention.ms=2592000000 2>/dev/null
    $KAFKA_CMD --bootstrap-server "$BOOTSTRAP" --create --topic processed.trades.filled --partitions 12 --replication-factor 3 --config retention.ms=2592000000 2>/dev/null
    $KAFKA_CMD --bootstrap-server "$BOOTSTRAP" --create --topic processed.trades.cancelled --partitions 6 --replication-factor 3 --config retention.ms=2592000000 2>/dev/null
    $KAFKA_CMD --bootstrap-server "$BOOTSTRAP" --create --topic processed.trades.rejected --partitions 6 --replication-factor 3 --config retention.ms=2592000000 2>/dev/null
    $KAFKA_CMD --bootstrap-server "$BOOTSTRAP" --create --topic analytics.trade-aggregates --partitions 12 --replication-factor 3 --config retention.ms=2592000000 2>/dev/null
    $KAFKA_CMD --bootstrap-server "$BOOTSTRAP" --create --topic analytics.orderbook --partitions 12 --replication-factor 3 --config retention.ms=2592000000 2>/dev/null
    $KAFKA_CMD --bootstrap-server "$BOOTSTRAP" --create --topic enriched.trades --partitions 12 --replication-factor 3 --config retention.ms=2592000000 2>/dev/null
    $KAFKA_CMD --bootstrap-server "$BOOTSTRAP" --create --topic alerts.price-movement --partitions 3 --replication-factor 3 --config retention.ms=2592000000 2>/dev/null
    $KAFKA_CMD --bootstrap-server "$BOOTSTRAP" --create --topic dlq.raw.failed --partitions 3 --replication-factor 3 --config retention.ms=7776000000 2>/dev/null

    success "All Kafka topics recreated"
fi

# ── Wipe Dagster run history ─────────────────────────────────
if $WIPE_DAGSTER; then
    info "Purging Dagster run history..."
    docker exec dagster-webserver dagster run wipe 2>/dev/null \
        && success "Dagster run history purged" \
        || warn "Could not purge Dagster runs (service may not be running)"
fi

# ── Wipe quality validation results ─────────────────────────
if $WIPE_QUALITY; then
    info "Wiping quality validation results..."
    mc rm --recursive --force "$MINIO_ALIAS/$WAREHOUSE_BUCKET/quality/" 2>/dev/null \
        && success "Quality results wiped" \
        || warn "Quality results already empty"
    mc mb "$MINIO_ALIAS/$WAREHOUSE_BUCKET/quality" 2>/dev/null || true
fi

success "Data wipe complete"
```

---

## 7. Nuclear Option — Full Reset

The `--nuclear` flag on the global teardown performs the most aggressive cleanup:

```
What --nuclear does:
═══════════════════════════════════════════════
1. Stops ALL containers (graceful, reverse-dependency order)
2. Removes ALL Docker volumes (all data lost)
3. Removes ALL project Docker images (de-project/*)
4. Removes ALL project Docker networks (de-*)
5. Prunes Docker build cache
6. Removes .teardown-backups/ directory
7. Removes .flink-savepoints/ directory

What --nuclear does NOT do:
═══════════════════════════════════════════════
- Does NOT uninstall system tools (Python, Java, Terraform, etc.)
- Does NOT remove source code or configuration files
- Does NOT remove Colima VM (use --stop-colima separately)
- Does NOT remove Terraform state files (run tf-destroy first)
- Does NOT remove git history

After --nuclear, run `make setup` to rebuild everything from scratch.
```

---

## 8. Rebuild After Teardown

| Teardown Level | Rebuild Command | What Gets Rebuilt |
|---|---|---|
| `make down-kafka` | `make up-kafka` | Kafka brokers + Schema Registry (data preserved) |
| `make destroy-kafka` | `make up-kafka && make seed-kafka-topics` | Kafka from scratch, topics recreated |
| `make teardown` | `make up` | All services restart (data preserved) |
| `make teardown-destroy` | `make setup` | Full setup: compose up + migrations + seeding |
| `make teardown-nuclear` | `make fresh-start` | Complete rebuild: images + compose + migrations + seed |
| `make wipe-bronze` | Dagster re-ingests from Kafka | Bronze data re-ingested |
| `make wipe-all-data` | Dagster full backfill | All layers recomputed |

---

## 9. Implementation File List

### Shared Helper (used by all scripts)

**File**: `scripts/_teardown-common.sh`

```bash
#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
# _teardown-common.sh — Shared functions for all teardown scripts
# ──────────────────────────────────────────────────────────────
# Source this at the top of every teardown script:
#   source "$SCRIPT_DIR/_teardown-common.sh"
# ──────────────────────────────────────────────────────────────

SCRIPT_DIR="${SCRIPT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)}"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Defaults ──────────────────────────────────────────────────
DESTROY_DATA=false
DRY_RUN=false

# ── Colors ────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'
BOLD='\033[1m'

# ── Logging ───────────────────────────────────────────────────
info()    { echo -e "${BLUE}[INFO]${NC}  $1"; }
success() { echo -e "${GREEN}[DONE]${NC}  $1"; }
warn()    { echo -e "${YELLOW}[SKIP]${NC}  $1"; }
error()   { echo -e "${RED}[FAIL]${NC}  $1"; }
header()  { echo -e "\n${BOLD}${CYAN}═══ $1 ═══${NC}\n"; }

# ── Argument parser ───────────────────────────────────────────
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --destroy-data) DESTROY_DATA=true ;;
            --dry-run)      DRY_RUN=true ;;
        esac
        shift
    done
}

# ── Pre-checks ────────────────────────────────────────────────
require_docker() {
    if ! command -v docker &> /dev/null; then
        error "Docker is not installed or not in PATH"
        exit 1
    fi
    if ! docker info &> /dev/null 2>&1; then
        warn "Docker daemon is not running. Nothing to tear down."
        exit 0
    fi
}

# ── Execute a step (with dry-run support) ─────────────────────
run_step() {
    local description="$1"
    local command="$2"

    info "$description"
    if $DRY_RUN; then
        echo -e "  ${YELLOW}[DRY RUN]${NC} Would execute: $command"
    else
        eval "$command" 2>/dev/null && success "$description" || warn "Already stopped or not found"
    fi
}

# ── Destroy Docker volumes by name ────────────────────────────
destroy_volumes() {
    if ! $DESTROY_DATA; then return; fi

    for vol in "$@"; do
        if $DRY_RUN; then
            echo -e "  ${YELLOW}[DRY RUN]${NC} Would remove volume: $vol"
        else
            # Try both plain name and project-prefixed name
            docker volume rm "$vol" 2>/dev/null \
                && success "Removed volume: $vol" \
                || docker volume rm "de-project_${vol}" 2>/dev/null \
                    && success "Removed volume: de-project_${vol}" \
                    || warn "Volume $vol not found (already removed)"
        fi
    done
}

# ── Print teardown summary ────────────────────────────────────
summary() {
    local module="$1"
    local services="$2"

    echo ""
    echo -e "  ${BOLD}Module:${NC}   $module"
    echo -e "  ${BOLD}Services:${NC} $services"
    echo -e "  ${BOLD}Data:${NC}     $(if $DESTROY_DATA; then echo -e "${RED}DELETED${NC}"; else echo -e "${GREEN}PRESERVED${NC}"; fi)"
    echo ""
}
```

### Complete File List

```
scripts/
├── _teardown-common.sh          # Shared helper functions (colors, logging, volume destroy)
├── teardown.sh                  # Global teardown (calls all module scripts in order)
├── teardown-services.sh         # App services: trade-generator, kafka-consumer, graphql-api
├── teardown-superset.sh         # Superset: web, celery, redis, postgres
├── teardown-dagster.sh          # Dagster: daemon, webserver, code-server, postgres
├── teardown-flink.sh            # Flink: savepoint → cancel jobs → taskmanager → jobmanager
├── teardown-governance.sh       # OpenMetadata: server, ingestion, elasticsearch, mysql
├── teardown-monitoring.sh       # Prometheus, Grafana, Alertmanager, exporters
├── teardown-kafka.sh            # Kafka: UI → Schema Registry → brokers (reverse order)
├── teardown-storage.sh          # MinIO + Iceberg REST catalog
├── teardown-k8s.sh              # Kubernetes namespace resources
├── teardown-terraform.sh        # Terraform destroy (all or by module)
└── wipe-data.sh                 # Data-only wipe (layer-specific, Kafka topics, Dagster runs)
```

### Quick Reference Card

```
┌──────────────────────────────────────────────────────────────┐
│                    TEARDOWN QUICK REFERENCE                  │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  STOP everything, keep data:     make teardown               │
│  STOP everything, delete data:   make teardown-destroy       │
│  FULL RESET from scratch:        make teardown-nuclear       │
│                                                              │
│  Stop one module:       make down-{module}                   │
│  Destroy one module:    make destroy-{module}                │
│                                                              │
│  Modules: services, superset, dagster, flink,                │
│           governance, monitoring, kafka, storage              │
│                                                              │
│  Wipe data only:        make wipe-{bronze|silver|gold}       │
│  Wipe all data:         make wipe-all-data                   │
│  Reset Kafka topics:    make wipe-kafka-topics               │
│                                                              │
│  K8s teardown:          make k8s-teardown-{dev|staging|all}  │
│  Terraform destroy:     make tf-destroy                      │
│                                                              │
│  Rebuild after destroy: make setup                           │
│  Rebuild after nuclear: make fresh-start                     │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```
