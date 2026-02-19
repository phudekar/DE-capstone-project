# 07 - Data Governance & Metadata

## Overview

This document defines the implementation plan for the Data Governance and Metadata layer of the stock market trade data pipeline. The governance layer provides a unified data catalog, end-to-end lineage tracking, role-based access control, PII handling, and data masking. OpenMetadata is the chosen governance platform.

---

## Table of Contents

1. [Framework Selection: OpenMetadata](#1-framework-selection-openmetadata)
2. [OpenMetadata Setup](#2-openmetadata-setup)
3. [Data Catalog](#3-data-catalog)
4. [Data Lineage](#4-data-lineage)
5. [Impact Analysis](#5-impact-analysis)
6. [Access Control (RBAC)](#6-access-control-rbac)
7. [Row-Level and Column-Level Security](#7-row-level-and-column-level-security)
8. [Data Masking and Anonymization](#8-data-masking-and-anonymization)
9. [PII Handling](#9-pii-handling)
10. [Data Glossary](#10-data-glossary)
11. [Implementation Steps](#11-implementation-steps)
12. [Testing Strategy](#12-testing-strategy)

---

## 1. Framework Selection: OpenMetadata

### Why OpenMetadata Over Apache Atlas

| Criterion | OpenMetadata | Apache Atlas |
|---|---|---|
| **UI / UX** | Modern React-based UI with intuitive navigation, search, and collaboration features | Dated UI, limited interactivity |
| **API Design** | REST API-first architecture with full OpenAPI spec; every feature is API-accessible | REST + Kafka-based, but API coverage is incomplete for many operations |
| **Data Lineage** | Built-in lineage visualization at table and column level, with manual and automated lineage | Lineage support exists but visualization is basic and column-level tracking is limited |
| **Native Connectors** | 70+ connectors including Kafka, Iceberg, Flink, Dagster, DuckDB, Superset | Primarily Hadoop ecosystem (Hive, HBase, Sqoop); limited modern stack support |
| **OpenLineage Support** | Native consumer for OpenLineage events; first-class support for the standard | No native OpenLineage integration; requires custom development |
| **Deployment** | Docker Compose or Kubernetes; single binary server with minimal dependencies | Requires HBase, Solr, Kafka; heavy operational footprint |
| **Configuration** | YAML-based configuration; environment variable overrides; simple initial setup | XML-heavy configuration; complex dependency management |
| **Data Quality** | Built-in data quality test framework with profiling, test suites, and alerting | No built-in data quality; requires external tools |
| **Community** | Active GitHub (4k+ stars), frequent releases, responsive maintainers | Apache project but slower release cadence; declining contribution activity |
| **Collaboration** | Built-in conversations, tasks, announcements on any data asset | Limited collaboration features |

### Decision

**OpenMetadata** is selected because it aligns with the modern data stack used in this project (Kafka, Iceberg, Flink, Dagster, Superset) and provides native connectors for each component. Its REST API-first design enables programmatic governance workflows, and its built-in OpenLineage consumer simplifies end-to-end lineage tracking.

### OpenMetadata Architecture

```
                    +---------------------+
                    |   OpenMetadata UI   |
                    |   (React SPA)       |
                    +----------+----------+
                               |
                               | HTTP/REST
                               v
                    +----------+----------+
                    | OpenMetadata Server  |
                    | (Java/Dropwizard)    |
                    |                     |
                    | - Catalog API       |
                    | - Lineage API       |
                    | - Search API        |
                    | - Auth/RBAC         |
                    | - Ingestion API     |
                    | - Data Quality API  |
                    +--+------+-------+---+
                       |      |       |
              +--------+   +--+--+  +-+----------+
              |            |     |  |             |
              v            v     |  v             v
        +-----+----+ +----+--+  | +------+----+ +--------+
        |  MySQL    | |Elastic|  | |Ingestion  | |OpenLine|
        | (metadata | |Search |  | |Bot        | |age     |
        |  store)   | |(index)|  | |(Airflow/  | |Consumer|
        +----------+ +-------+  | | scheduled)| +--------+
                                 | +-----------+
                                 |
                                 v
                          +------+------+
                          |   JWT Auth  |
                          | (or SSO/    |
                          |  OIDC)      |
                          +-------------+
```

**Components:**

- **OpenMetadata Server**: Core Java application that exposes REST APIs for all governance operations. Handles catalog CRUD, lineage management, search delegation, authentication, and authorization.
- **OpenMetadata UI**: React single-page application providing data discovery, lineage visualization, glossary management, and collaboration features.
- **MySQL**: Persistent metadata store holding all catalog entities, relationships, lineage edges, policies, and user data.
- **Elasticsearch**: Powers full-text search and faceted discovery across all data assets. Indexes are kept in sync by the server.
- **Ingestion Bot**: Scheduled workflows that connect to source systems (Kafka, Iceberg, Dagster, etc.) and pull metadata into the catalog. Can run via Airflow or the built-in scheduler.
- **OpenLineage Consumer**: HTTP endpoint that receives OpenLineage RunEvents and translates them into lineage edges within the catalog.

---

## 2. OpenMetadata Setup

### 2.1 Docker Deployment Configuration

**File**: `governance/openmetadata/docker-compose.yml`

```yaml
version: "3.9"

services:
  # -------------------------------------------------------
  # MySQL - Metadata Store
  # -------------------------------------------------------
  openmetadata-mysql:
    image: mysql:8.0
    container_name: openmetadata-mysql
    restart: always
    environment:
      MYSQL_ROOT_PASSWORD: "${OM_MYSQL_ROOT_PASSWORD:-openmetadata_root}"
      MYSQL_DATABASE: openmetadata_db
      MYSQL_USER: openmetadata_user
      MYSQL_PASSWORD: "${OM_MYSQL_PASSWORD:-openmetadata_password}"
    ports:
      - "3306:3306"
    volumes:
      - openmetadata-mysql-data:/var/lib/mysql
      - ./config/mysql/init.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost", "-u", "root", "-p${OM_MYSQL_ROOT_PASSWORD:-openmetadata_root}"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - governance-network

  # -------------------------------------------------------
  # Elasticsearch - Search Index
  # -------------------------------------------------------
  openmetadata-elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:7.17.15
    container_name: openmetadata-elasticsearch
    restart: always
    environment:
      discovery.type: single-node
      ES_JAVA_OPTS: "-Xms512m -Xmx512m"
      xpack.security.enabled: "false"
    ports:
      - "9200:9200"
      - "9300:9300"
    volumes:
      - openmetadata-es-data:/usr/share/elasticsearch/data
    healthcheck:
      test: ["CMD-SHELL", "curl -f http://localhost:9200/_cluster/health || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - governance-network

  # -------------------------------------------------------
  # OpenMetadata Server
  # -------------------------------------------------------
  openmetadata-server:
    image: openmetadata/server:1.3.1
    container_name: openmetadata-server
    restart: always
    depends_on:
      openmetadata-mysql:
        condition: service_healthy
      openmetadata-elasticsearch:
        condition: service_healthy
    environment:
      # Database
      DB_DRIVER_CLASS: com.mysql.cj.jdbc.Driver
      DB_SCHEME: mysql
      DB_HOST: openmetadata-mysql
      DB_PORT: "3306"
      DB_USER: openmetadata_user
      DB_USER_PASSWORD: "${OM_MYSQL_PASSWORD:-openmetadata_password}"
      OM_DATABASE: openmetadata_db

      # Elasticsearch
      ELASTICSEARCH_HOST: openmetadata-elasticsearch
      ELASTICSEARCH_PORT: "9200"
      ELASTICSEARCH_SCHEME: http

      # Server
      SERVER_HOST: "0.0.0.0"
      SERVER_PORT: "8585"
      SERVER_ADMIN_PORT: "8586"

      # Authentication
      AUTHENTICATION_PROVIDER: basic
      AUTHENTICATION_PUBLIC_KEYS: "[http://localhost:8585/api/v1/system/config/jwks]"
      AUTHENTICATION_AUTHORITY: "https://accounts.google.com"
      AUTHENTICATION_CALLBACK_URL: "http://localhost:8585/callback"

      # Authorizer
      AUTHORIZER_CLASS_NAME: org.openmetadata.service.security.DefaultAuthorizer
      AUTHORIZER_ADMIN_PRINCIPALS: "[admin]"
      AUTHORIZER_PRINCIPAL_DOMAIN: "open-metadata.org"

      # Airflow / Ingestion
      PIPELINE_SERVICE_CLIENT_ENABLED: "true"
      PIPELINE_SERVICE_CLIENT_HOST: "http://openmetadata-ingestion:8080"
      PIPELINE_SERVICE_CLIENT_CLASS_NAME: org.openmetadata.service.clients.pipeline.airflow.AirflowRESTClient

      # OpenLineage Consumer
      OPENLINEAGE_ENABLED: "true"
    ports:
      - "8585:8585"
      - "8586:8586"
    volumes:
      - ./config/openmetadata/openmetadata.yaml:/opt/openmetadata/conf/openmetadata.yaml
    networks:
      - governance-network

  # -------------------------------------------------------
  # OpenMetadata Ingestion (Airflow-based)
  # -------------------------------------------------------
  openmetadata-ingestion:
    image: openmetadata/ingestion:1.3.1
    container_name: openmetadata-ingestion
    restart: always
    depends_on:
      openmetadata-server:
        condition: service_started
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: "mysql+pymysql://openmetadata_user:${OM_MYSQL_PASSWORD:-openmetadata_password}@openmetadata-mysql:3306/airflow_db"
      AIRFLOW__OPENMETADATA__SERVER_HOST: "http://openmetadata-server:8585/api"
    ports:
      - "8080:8080"
    volumes:
      - openmetadata-ingestion-data:/opt/airflow
      - ./config/ingestion/dags:/opt/airflow/dags
    networks:
      - governance-network

volumes:
  openmetadata-mysql-data:
  openmetadata-es-data:
  openmetadata-ingestion-data:

networks:
  governance-network:
    name: governance-network
    driver: bridge
```

### 2.2 Database Setup (MySQL)

**File**: `governance/openmetadata/config/mysql/init.sql`

```sql
-- Create databases for OpenMetadata and Airflow
CREATE DATABASE IF NOT EXISTS openmetadata_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE IF NOT EXISTS airflow_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Grant privileges
GRANT ALL PRIVILEGES ON openmetadata_db.* TO 'openmetadata_user'@'%';
GRANT ALL PRIVILEGES ON airflow_db.* TO 'openmetadata_user'@'%';
FLUSH PRIVILEGES;
```

### 2.3 Elasticsearch Configuration

Elasticsearch is used for full-text search across the catalog. Key index settings:

- **Entity indices**: `table_search_index`, `topic_search_index`, `pipeline_search_index`, `dashboard_search_index`
- **Replication**: Single node for local development; production would use 3-node cluster
- **Refresh interval**: 1 second (near real-time search)
- **Analyzers**: Standard analyzer with custom stop words for data domain terms

No additional configuration files are required for local development. The OpenMetadata server creates and manages indices automatically on startup.

### 2.4 Ingestion Bot Configuration

**File**: `governance/openmetadata/config/ingestion/bot-config.yaml`

```yaml
# Ingestion bot configuration
# The bot authenticates to the OpenMetadata server to run metadata ingestion workflows

ingestionBot:
  name: "ingestion-bot"
  description: "Automated metadata ingestion bot for trade data pipeline"
  botType: "ingestion"
  authenticationMechanism:
    authType: "JWT"
    config:
      # Generated via OpenMetadata UI: Settings > Bots > ingestion-bot
      token: "${OM_INGESTION_BOT_JWT}"

  # Default schedule for all ingestion workflows
  defaultSchedule:
    repeatFrequency: "PT1H"  # Every 1 hour
    startDate: "2024-01-01T00:00:00Z"
```

### 2.5 Authentication Setup

For local development, basic authentication is used. For production, OIDC/SSO integration is recommended.

**Basic Auth Configuration** (embedded in `openmetadata.yaml`):

```yaml
authenticationConfiguration:
  provider: basic
  publicKeyUrls:
    - "http://localhost:8585/api/v1/system/config/jwks"
  authority: "https://accounts.google.com"
  callbackUrl: "http://localhost:8585/callback"
  enableSelfSignup: true

authorizerConfiguration:
  className: org.openmetadata.service.security.DefaultAuthorizer
  containerRequestFilter: org.openmetadata.service.security.JwtFilter
  adminPrincipals:
    - admin
  principalDomain: "open-metadata.org"
```

**Production OIDC Configuration** (for future reference):

```yaml
authenticationConfiguration:
  provider: google  # or okta, azure, custom-oidc
  publicKeyUrls:
    - "https://www.googleapis.com/oauth2/v3/certs"
  authority: "https://accounts.google.com"
  clientId: "${GOOGLE_CLIENT_ID}"
  callbackUrl: "https://governance.yourcompany.com/callback"
  enableSelfSignup: false
```

### 2.6 Initial Configuration

After deployment, the following bootstrap steps are performed:

```bash
#!/usr/bin/env bash
# File: governance/openmetadata/scripts/bootstrap.sh

set -euo pipefail

OM_SERVER="http://localhost:8585/api/v1"
OM_TOKEN="${OM_ADMIN_TOKEN}"

echo "=== Waiting for OpenMetadata server ==="
until curl -sf "${OM_SERVER}/system/version" > /dev/null 2>&1; do
  echo "Waiting for server..."
  sleep 5
done
echo "Server is ready."

echo "=== Creating service connections ==="

# 1. Register Kafka messaging service
curl -X PUT "${OM_SERVER}/services/messagingServices" \
  -H "Authorization: Bearer ${OM_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "trade-kafka-cluster",
    "serviceType": "Kafka",
    "description": "Stock trade data Kafka cluster",
    "connection": {
      "config": {
        "type": "Kafka",
        "bootstrapServers": "kafka-broker-1:9092,kafka-broker-2:9092",
        "schemaRegistryURL": "http://schema-registry:8081"
      }
    }
  }'

# 2. Register Iceberg database service
curl -X PUT "${OM_SERVER}/services/databaseServices" \
  -H "Authorization: Bearer ${OM_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "trade-iceberg-lakehouse",
    "serviceType": "Iceberg",
    "description": "Iceberg lakehouse for trade data (Bronze/Silver/Gold)",
    "connection": {
      "config": {
        "type": "Iceberg",
        "catalog": {
          "name": "trade_catalog",
          "connection": {
            "type": "Rest",
            "uri": "http://iceberg-rest-catalog:8181"
          }
        }
      }
    }
  }'

# 3. Register Dagster pipeline service
curl -X PUT "${OM_SERVER}/services/pipelineServices" \
  -H "Authorization: Bearer ${OM_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "trade-dagster-orchestrator",
    "serviceType": "Dagster",
    "description": "Dagster orchestrator for trade data pipeline",
    "connection": {
      "config": {
        "type": "Dagster",
        "host": "http://dagster-webserver:3000"
      }
    }
  }'

# 4. Register Superset dashboard service
curl -X PUT "${OM_SERVER}/services/dashboardServices" \
  -H "Authorization: Bearer ${OM_TOKEN}" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "trade-superset-dashboards",
    "serviceType": "Superset",
    "description": "Superset dashboards for trade analytics",
    "connection": {
      "config": {
        "type": "Superset",
        "hostPort": "http://superset:8088",
        "connection": {
          "provider": "db",
          "username": "admin",
          "password": "${SUPERSET_PASSWORD}"
        }
      }
    }
  }'

echo "=== Bootstrap complete ==="
```

---

## 3. Data Catalog

### 3.1 Auto-Discovery of Data Assets

#### 3.1.1 Kafka Topics (via Kafka Connector)

**Ingestion workflow**: `governance/openmetadata/config/ingestion/dags/kafka_metadata_ingestion.yaml`

```yaml
source:
  type: kafka
  serviceName: trade-kafka-cluster
  sourceConfig:
    config:
      type: MessagingMetadata
      topicFilterPattern:
        includes:
          - "raw\\..*"           # raw.trades, raw.orders
          - "processed\\..*"     # processed.trades, processed.aggregated
          - "enriched\\..*"      # enriched.trades
          - "alerts\\..*"        # alerts.anomalies
        excludes:
          - "__.*"               # Exclude internal Kafka topics
          - ".*\\.dlq"           # Exclude dead letter queues from primary catalog
      generateSampleData: true
      # Parse Avro schemas from Schema Registry
      schemaRegistryConfig:
        url: "http://schema-registry:8081"

sink:
  type: metadata-rest
  config: {}

workflowConfig:
  openMetadataServerConfig:
    hostPort: "http://openmetadata-server:8585/api"
    authProvider: openmetadata
    securityConfig:
      jwtToken: "${OM_INGESTION_BOT_JWT}"
  loggerLevel: INFO
```

**Discovered assets:**

| Topic | Description | Schema Source |
|---|---|---|
| `raw.trades` | Raw trade events from WebSocket generator | Avro (Schema Registry) |
| `raw.orders` | Raw order book events | Avro (Schema Registry) |
| `processed.trades` | Cleaned and validated trades from Flink | Avro (Schema Registry) |
| `processed.aggregated` | Windowed aggregations (1m, 5m, 15m) | Avro (Schema Registry) |
| `enriched.trades` | Trades enriched with reference data | Avro (Schema Registry) |
| `alerts.anomalies` | Anomaly detection alerts from Flink | Avro (Schema Registry) |

#### 3.1.2 Iceberg Tables (via Iceberg/Hive Connector)

**Ingestion workflow**: `governance/openmetadata/config/ingestion/dags/iceberg_metadata_ingestion.yaml`

```yaml
source:
  type: iceberg
  serviceName: trade-iceberg-lakehouse
  sourceConfig:
    config:
      type: DatabaseMetadata
      schemaFilterPattern:
        includes:
          - "bronze"
          - "silver"
          - "gold"
      tableFilterPattern:
        includes:
          - ".*"
      includeViews: true
      includeTags: true
      markDeletedTables: true
      # Profile tables for statistics
      includeProfile: true

sink:
  type: metadata-rest
  config: {}

workflowConfig:
  openMetadataServerConfig:
    hostPort: "http://openmetadata-server:8585/api"
    authProvider: openmetadata
    securityConfig:
      jwtToken: "${OM_INGESTION_BOT_JWT}"
```

**Discovered assets:**

| Layer | Table | Description |
|---|---|---|
| Bronze | `bronze.raw_trades` | Append-only raw trade events from Kafka |
| Bronze | `bronze.raw_orders` | Append-only raw order events from Kafka |
| Silver | `silver.trades` | Cleaned, deduplicated trades with SCD Type 2 |
| Silver | `silver.orders` | Cleaned order data with SCD Type 2 |
| Silver | `silver.instruments` | Instrument reference data with SCD Type 1 |
| Gold | `gold.daily_trade_summary` | Daily aggregated trade statistics per symbol |
| Gold | `gold.vwap_analysis` | VWAP calculations per symbol per interval |
| Gold | `gold.market_depth` | Bid-ask spread and market depth snapshots |
| Gold | `gold.trader_performance` | Trader-level performance metrics |

#### 3.1.3 Dagster Pipelines (via Dagster Connector)

**Ingestion workflow**: `governance/openmetadata/config/ingestion/dags/dagster_metadata_ingestion.yaml`

```yaml
source:
  type: dagster
  serviceName: trade-dagster-orchestrator
  sourceConfig:
    config:
      type: PipelineMetadata

sink:
  type: metadata-rest
  config: {}

workflowConfig:
  openMetadataServerConfig:
    hostPort: "http://openmetadata-server:8585/api"
    authProvider: openmetadata
    securityConfig:
      jwtToken: "${OM_INGESTION_BOT_JWT}"
```

**Discovered pipelines:**

| Pipeline | Type | Description |
|---|---|---|
| `kafka_to_bronze_ingestion` | Streaming | Consumes Kafka topics, writes to Bronze Iceberg |
| `bronze_to_silver_transform` | Batch | Cleans, deduplicates, applies SCD logic |
| `silver_to_gold_aggregation` | Batch | Computes aggregations, VWAP, market depth |
| `data_quality_checks` | Batch | Runs quality validation suites |
| `iceberg_maintenance` | Batch | Compaction, snapshot expiry, orphan cleanup |

### 3.2 Manual Enrichment

After auto-discovery populates the catalog with technical metadata, manual enrichment adds business context. This is performed via the OpenMetadata UI or API.

#### 3.2.1 Business Descriptions

Example API call to add a description to a table:

```python
# File: governance/openmetadata/scripts/enrich_descriptions.py

import requests

OM_SERVER = "http://localhost:8585/api/v1"
HEADERS = {
    "Authorization": f"Bearer {OM_INGESTION_BOT_JWT}",
    "Content-Type": "application/json",
}

TABLE_DESCRIPTIONS = {
    "trade-iceberg-lakehouse.bronze.raw_trades": {
        "description": (
            "Append-only landing table for raw trade events ingested from Kafka. "
            "Each row represents a single trade execution as reported by the exchange. "
            "No deduplication or cleaning is applied at this layer. "
            "Partitioned by ingestion date (ds) for efficient backfill and replay."
        ),
        "columns": {
            "trade_id": "Unique identifier for the trade assigned by the exchange.",
            "symbol": "Ticker symbol of the traded instrument (e.g., AAPL, MSFT).",
            "price": "Execution price of the trade in USD.",
            "quantity": "Number of shares traded.",
            "timestamp": "Exchange timestamp of the trade execution (UTC epoch ms).",
            "trader_id": "Anonymized identifier of the trader. Classified as indirect PII.",
            "account_id": "Trading account identifier. Classified as indirect PII.",
            "exchange": "Exchange code where the trade was executed (e.g., NYSE, NASDAQ).",
            "side": "Trade direction: BUY or SELL.",
            "ingestion_ts": "Timestamp when the record was ingested into Bronze layer.",
        },
    },
    "trade-iceberg-lakehouse.gold.daily_trade_summary": {
        "description": (
            "Daily aggregated trade statistics per symbol. Computed from Silver layer trades. "
            "Used by business dashboards and API serving layer. "
            "Tier 1 critical asset - SLA: refreshed by 06:00 UTC daily."
        ),
        "columns": {
            "symbol": "Ticker symbol.",
            "trade_date": "Trading date (YYYY-MM-DD).",
            "open_price": "First trade price of the day.",
            "close_price": "Last trade price of the day.",
            "high_price": "Maximum trade price of the day.",
            "low_price": "Minimum trade price of the day.",
            "total_volume": "Total number of shares traded.",
            "total_value": "Sum of (price * quantity) for all trades.",
            "trade_count": "Number of individual trades.",
            "vwap": "Volume-weighted average price for the day.",
        },
    },
}


def enrich_table(fqn: str, config: dict):
    """Add business descriptions to a table and its columns."""
    # Get table by fully qualified name
    resp = requests.get(
        f"{OM_SERVER}/tables/name/{fqn}",
        headers=HEADERS,
    )
    resp.raise_for_status()
    table = resp.json()

    # Patch table description
    patch_ops = [
        {"op": "add", "path": "/description", "value": config["description"]},
    ]

    # Patch column descriptions
    for i, col in enumerate(table.get("columns", [])):
        col_name = col["name"]
        if col_name in config.get("columns", {}):
            patch_ops.append(
                {
                    "op": "add",
                    "path": f"/columns/{i}/description",
                    "value": config["columns"][col_name],
                }
            )

    requests.patch(
        f"{OM_SERVER}/tables/{table['id']}",
        headers={**HEADERS, "Content-Type": "application/json-patch+json"},
        json=patch_ops,
    )
    print(f"Enriched: {fqn}")


if __name__ == "__main__":
    for fqn, config in TABLE_DESCRIPTIONS.items():
        enrich_table(fqn, config)
```

#### 3.2.2 Tags and Classifications

**Classification taxonomy for the trade data pipeline:**

```
Classifications
+-- Sensitivity
|   +-- PII              (Personally Identifiable Information)
|   +-- Confidential      (Internal-only business data)
|   +-- Restricted        (Regulatory-restricted data)
|   +-- Public            (Publicly available market data)
|
+-- DataLayer
|   +-- Bronze            (Raw landing zone)
|   +-- Silver            (Cleaned and conformed)
|   +-- Gold              (Business-ready aggregations)
|
+-- DataDomain
    +-- Trade             (Trade execution data)
    +-- Order             (Order book data)
    +-- Instrument        (Reference data)
    +-- Analytics         (Computed metrics)
```

**Tag assignment script**: `governance/openmetadata/scripts/apply_tags.py`

```python
TAG_ASSIGNMENTS = {
    "trade-iceberg-lakehouse.bronze.raw_trades": {
        "table_tags": ["DataLayer.Bronze", "DataDomain.Trade"],
        "column_tags": {
            "trader_id": ["Sensitivity.PII"],
            "account_id": ["Sensitivity.PII"],
            "symbol": ["Sensitivity.Public"],
            "price": ["Sensitivity.Public"],
            "quantity": ["Sensitivity.Public"],
        },
    },
    "trade-iceberg-lakehouse.silver.trades": {
        "table_tags": ["DataLayer.Silver", "DataDomain.Trade"],
        "column_tags": {
            "trader_id": ["Sensitivity.PII"],
            "account_id": ["Sensitivity.PII"],
        },
    },
    "trade-iceberg-lakehouse.gold.daily_trade_summary": {
        "table_tags": ["DataLayer.Gold", "DataDomain.Analytics", "Sensitivity.Confidential"],
        "column_tags": {
            "vwap": ["Sensitivity.Confidential"],
        },
    },
}
```

#### 3.2.3 Glossary Terms

See [Section 10: Data Glossary](#10-data-glossary) for the full glossary. Terms are linked to columns and tables in the catalog so that business users can discover data by searching for domain concepts.

#### 3.2.4 Data Owners Assignment

| Asset | Owner (Team) | Owner (Individual) |
|---|---|---|
| Bronze tables | Data Engineering | DE Lead |
| Silver tables | Data Engineering | DE Lead |
| Gold tables | Analytics Engineering | Analytics Lead |
| Kafka topics | Data Engineering | Streaming Lead |
| Dagster pipelines | Data Engineering | Orchestration Lead |
| Superset dashboards | Business Intelligence | BI Lead |
| GraphQL API | Platform Engineering | API Lead |

#### 3.2.5 Tier Classification

| Tier | SLA | Assets |
|---|---|---|
| **Tier 1: Critical** | < 1 hour freshness, 99.9% uptime | `gold.daily_trade_summary`, `gold.vwap_analysis`, `silver.trades`, GraphQL API |
| **Tier 2: Important** | < 4 hours freshness, 99.5% uptime | `gold.market_depth`, `gold.trader_performance`, `silver.orders`, Superset dashboards |
| **Tier 3: Informational** | Best effort, 99% uptime | `bronze.*`, reference data, development/sandbox tables |

---

## 4. Data Lineage

### 4.1 Table-Level Lineage

The end-to-end data flow through the pipeline:

```
Trade Generator (WebSocket)
        |
        v
  +-----+------+
  | Kafka       |
  | raw.trades  |
  | raw.orders  |
  +-----+------+
        |
        v
  +-----+--------+          +------------------+
  | Apache Flink  |--------->| Kafka            |
  | (stream       |          | processed.trades |
  |  processing)  |          | processed.agg    |
  +-----+---------+          | enriched.trades  |
        |                    +--------+---------+
        |                             |
        v                             v
  +-----+----------+          +-------+--------+
  | Dagster         |          | Dagster         |
  | kafka_to_bronze |          | kafka_to_bronze |
  +-----+----------+          +-------+--------+
        |                             |
        v                             v
  +-----+---------+           +-------+--------+
  | Iceberg Bronze |           | Iceberg Bronze  |
  | raw_trades     |           | processed_trades|
  +-----+---------+           +-------+---------+
        |                             |
        +-------------+---------------+
                      |
                      v
              +-------+--------+
              | Dagster         |
              | bronze_to_silver|
              +-------+--------+
                      |
                      v
              +-------+---------+
              | Iceberg Silver   |
              | trades (SCD2)    |
              | orders (SCD2)    |
              | instruments      |
              +-------+---------+
                      |
                      v
              +-------+--------+
              | Dagster         |
              | silver_to_gold  |
              +-------+--------+
                      |
                      v
              +-------+-----------+
              | Iceberg Gold       |
              | daily_trade_summary|
              | vwap_analysis      |
              | market_depth       |
              | trader_performance |
              +-------+-----------+
                      |
          +-----------+-----------+
          |                       |
          v                       v
  +-------+--------+     +-------+--------+
  | GraphQL API     |     | Superset       |
  | (serving layer) |     | (dashboards)   |
  +----------------+     +----------------+
```

### 4.2 Column-Level Lineage

Column-level lineage maps specific transformations between source and target columns.

#### Bronze to Silver Transformations

| Source (Bronze) | Target (Silver) | Transformation |
|---|---|---|
| `raw_trades.trade_id` | `trades.trade_id` | Deduplication (keep first occurrence) |
| `raw_trades.symbol` | `trades.symbol` | UPPER() trim, validate against instrument reference |
| `raw_trades.price` | `trades.price` | Cast to DECIMAL(18,6), null check, range validation |
| `raw_trades.quantity` | `trades.quantity` | Cast to BIGINT, positive value check |
| `raw_trades.timestamp` | `trades.trade_timestamp` | Rename, convert epoch ms to TIMESTAMP_TZ |
| `raw_trades.trader_id` | `trades.trader_id` | Pass-through (PII flag preserved) |
| `raw_trades.account_id` | `trades.account_id` | Pass-through (PII flag preserved) |
| `raw_trades.side` | `trades.side` | Validate: must be 'BUY' or 'SELL' |
| (computed) | `trades.surrogate_key` | SHA256(trade_id + symbol + timestamp) |
| (computed) | `trades.valid_from` | Ingestion timestamp for SCD2 |
| (computed) | `trades.valid_to` | NULL (current record) or next version timestamp |
| (computed) | `trades.is_current` | BOOLEAN flag for SCD2 |

#### Silver to Gold Transformations

| Source (Silver) | Target (Gold) | Transformation |
|---|---|---|
| `trades.symbol` | `daily_trade_summary.symbol` | GROUP BY key |
| `trades.trade_timestamp` | `daily_trade_summary.trade_date` | DATE(trade_timestamp) |
| `trades.price` | `daily_trade_summary.open_price` | FIRST_VALUE(price) OVER (PARTITION BY symbol, date ORDER BY timestamp) |
| `trades.price` | `daily_trade_summary.close_price` | LAST_VALUE(price) OVER (PARTITION BY symbol, date ORDER BY timestamp) |
| `trades.price` | `daily_trade_summary.high_price` | MAX(price) |
| `trades.price` | `daily_trade_summary.low_price` | MIN(price) |
| `trades.quantity` | `daily_trade_summary.total_volume` | SUM(quantity) |
| `trades.price`, `trades.quantity` | `daily_trade_summary.total_value` | SUM(price * quantity) |
| (computed) | `daily_trade_summary.trade_count` | COUNT(*) |
| `trades.price`, `trades.quantity` | `daily_trade_summary.vwap` | SUM(price * quantity) / SUM(quantity) |

### 4.3 OpenLineage Standard

#### 4.3.1 OpenLineage Event Structure

Every data transformation emits an OpenLineage `RunEvent` containing:

```json
{
  "eventType": "COMPLETE",
  "eventTime": "2024-01-15T10:30:00.000Z",
  "run": {
    "runId": "d46e465b-d358-4d32-83d4-df660ff614dd",
    "facets": {
      "nominalTime": {
        "nominalStartTime": "2024-01-15T10:00:00.000Z",
        "nominalEndTime": "2024-01-15T10:30:00.000Z"
      },
      "processing_engine": {
        "name": "dagster",
        "version": "1.6.0"
      }
    }
  },
  "job": {
    "namespace": "trade-pipeline",
    "name": "bronze_to_silver.trades",
    "facets": {
      "jobType": {
        "processingType": "BATCH",
        "integration": "DAGSTER",
        "jobType": "ASSET_MATERIALIZATION"
      },
      "sql": {
        "query": "INSERT INTO silver.trades SELECT ... FROM bronze.raw_trades WHERE ..."
      },
      "sourceCode": {
        "type": "python",
        "url": "https://github.com/org/de-project/blob/main/pipelines/silver/trades.py"
      }
    }
  },
  "inputs": [
    {
      "namespace": "iceberg://trade-catalog",
      "name": "bronze.raw_trades",
      "facets": {
        "schema": {
          "fields": [
            {"name": "trade_id", "type": "STRING"},
            {"name": "symbol", "type": "STRING"},
            {"name": "price", "type": "DOUBLE"},
            {"name": "quantity", "type": "LONG"},
            {"name": "timestamp", "type": "LONG"},
            {"name": "trader_id", "type": "STRING"},
            {"name": "account_id", "type": "STRING"}
          ]
        },
        "dataSource": {
          "name": "trade-iceberg-lakehouse",
          "uri": "iceberg://trade-catalog/bronze/raw_trades"
        },
        "columnLineage": {
          "fields": {
            "trade_id": {"inputFields": [{"namespace": "iceberg://trade-catalog", "name": "bronze.raw_trades", "field": "trade_id"}], "transformationType": "IDENTITY", "transformationDescription": "Deduplicated pass-through"},
            "price": {"inputFields": [{"namespace": "iceberg://trade-catalog", "name": "bronze.raw_trades", "field": "price"}], "transformationType": "TRANSFORMATION", "transformationDescription": "Cast to DECIMAL(18,6) with range validation"}
          }
        }
      }
    }
  ],
  "outputs": [
    {
      "namespace": "iceberg://trade-catalog",
      "name": "silver.trades",
      "facets": {
        "schema": {
          "fields": [
            {"name": "surrogate_key", "type": "STRING"},
            {"name": "trade_id", "type": "STRING"},
            {"name": "symbol", "type": "STRING"},
            {"name": "price", "type": "DECIMAL(18,6)"},
            {"name": "quantity", "type": "BIGINT"},
            {"name": "trade_timestamp", "type": "TIMESTAMP_TZ"}
          ]
        },
        "outputStatistics": {
          "rowCount": 145230,
          "size": 12458900
        }
      }
    }
  ]
}
```

#### 4.3.2 Emitting OpenLineage Events from Dagster

Dagster has built-in OpenLineage support via the `dagster-openlineage` package.

**File**: `orchestration/dagster/openlineage_config.py`

```python
from dagster import Definitions
from dagster_openlineage import OpenLineageResource

openlineage_resource = OpenLineageResource(
    # OpenMetadata's OpenLineage consumer endpoint
    transport={
        "type": "http",
        "url": "http://openmetadata-server:8585/api/v1/lineage/openlineage",
        "auth": {
            "type": "api_key",
            "api_key": "${OM_INGESTION_BOT_JWT}",
        },
    },
    namespace="trade-pipeline",
)

# Include in Definitions
defs = Definitions(
    resources={
        "openlineage": openlineage_resource,
    },
    # ... assets, jobs, schedules
)
```

#### 4.3.3 Emitting OpenLineage Events from Flink

Flink does not have built-in OpenLineage support, so a custom listener is implemented.

**File**: `processing/flink/src/main/java/com/trades/lineage/FlinkOpenLineageListener.java`

```java
/**
 * Custom OpenLineage event emitter for Flink jobs.
 *
 * Hooks into Flink's JobListener interface to emit START, COMPLETE,
 * and FAIL events for each Flink job execution.
 *
 * Configuration:
 *   openlineage.transport.type: http
 *   openlineage.transport.url: http://openmetadata-server:8585/api/v1/lineage/openlineage
 *   openlineage.namespace: trade-pipeline-flink
 */
public class FlinkOpenLineageListener implements JobListener {

    private final OpenLineageClient client;
    private final String namespace;

    public FlinkOpenLineageListener(Configuration config) {
        String url = config.getString("openlineage.transport.url", "");
        this.namespace = config.getString("openlineage.namespace", "trade-pipeline-flink");
        this.client = OpenLineageClient.builder()
            .transport(HttpTransport.builder().uri(url).build())
            .build();
    }

    @Override
    public void onJobSubmitted(JobClient jobClient, Throwable throwable) {
        RunEvent startEvent = RunEvent.builder()
            .eventType(EventType.START)
            .eventTime(ZonedDateTime.now())
            .run(new Run(jobClient.getJobID().toString()))
            .job(Job.builder()
                .namespace(namespace)
                .name(extractJobName(jobClient))
                .build())
            .inputs(extractInputDatasets(jobClient))
            .outputs(extractOutputDatasets(jobClient))
            .build();
        client.emit(startEvent);
    }

    @Override
    public void onJobExecuted(JobExecutionResult result, Throwable throwable) {
        EventType type = (throwable == null) ? EventType.COMPLETE : EventType.FAIL;
        RunEvent event = RunEvent.builder()
            .eventType(type)
            .eventTime(ZonedDateTime.now())
            // ... build complete event with facets
            .build();
        client.emit(event);
    }
}
```

#### 4.3.4 OpenMetadata as OpenLineage Consumer

OpenMetadata exposes an HTTP endpoint that receives OpenLineage events and automatically creates or updates lineage edges in the catalog.

**Endpoint**: `POST /api/v1/lineage/openlineage`

**Configuration** (in `openmetadata.yaml`):

```yaml
pipelineServiceClientConfiguration:
  # ... existing config ...

# OpenLineage consumer is enabled by default when the server starts.
# Events received at /api/v1/lineage/openlineage are parsed and:
#   1. Matched to existing catalog entities by namespace + name
#   2. Lineage edges are created/updated between matched entities
#   3. Column-level lineage is extracted from ColumnLineageDatasetFacet
#   4. Run metadata is stored for audit purposes
```

---

## 5. Impact Analysis

### 5.1 Upstream/Downstream Dependency Visualization

OpenMetadata provides built-in lineage graph visualization. For each data asset, users can:

- **View upstream dependencies**: Trace back to the original data sources to understand where data comes from.
- **View downstream dependencies**: See all consumers and derived datasets to understand who is affected by changes.
- **Filter by depth**: Limit the lineage graph to N hops upstream or downstream.
- **Filter by entity type**: Show only tables, topics, pipelines, or dashboards.

**API for programmatic access:**

```bash
# Get lineage for a table (3 hops upstream, 3 hops downstream)
GET /api/v1/lineage/table/name/trade-iceberg-lakehouse.silver.trades?upstreamDepth=3&downstreamDepth=3
```

### 5.2 "What Happens If This Table Changes?" Analysis

**Scenario**: The schema of `bronze.raw_trades` changes (a column is renamed).

**Impact analysis query:**

```python
# File: governance/openmetadata/scripts/impact_analysis.py

import requests

OM_SERVER = "http://localhost:8585/api/v1"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}


def get_downstream_impact(table_fqn: str, max_depth: int = 5) -> dict:
    """
    Given a table FQN, returns all downstream assets that would be
    impacted by a schema or data change.
    """
    resp = requests.get(
        f"{OM_SERVER}/lineage/table/name/{table_fqn}",
        headers=HEADERS,
        params={"upstreamDepth": 0, "downstreamDepth": max_depth},
    )
    resp.raise_for_status()
    lineage = resp.json()

    impacted = []
    for edge in lineage.get("downstreamEdges", []):
        node = edge.get("toEntity", {})
        impacted.append({
            "name": node.get("fullyQualifiedName"),
            "type": node.get("type"),
            "owner": node.get("owner", {}).get("name", "unassigned"),
            "tier": node.get("tags", [{}])[0].get("tagFQN", "untiered"),
        })

    return {
        "source": table_fqn,
        "impacted_count": len(impacted),
        "impacted_assets": impacted,
    }


# Example usage
if __name__ == "__main__":
    import json
    result = get_downstream_impact(
        "trade-iceberg-lakehouse.bronze.raw_trades"
    )
    print(json.dumps(result, indent=2))
    # Output:
    # {
    #   "source": "trade-iceberg-lakehouse.bronze.raw_trades",
    #   "impacted_count": 7,
    #   "impacted_assets": [
    #     {"name": "trade-iceberg-lakehouse.silver.trades", "type": "table", ...},
    #     {"name": "trade-iceberg-lakehouse.gold.daily_trade_summary", "type": "table", ...},
    #     {"name": "trade-iceberg-lakehouse.gold.vwap_analysis", "type": "table", ...},
    #     {"name": "trade-dagster-orchestrator.bronze_to_silver", "type": "pipeline", ...},
    #     {"name": "trade-superset-dashboards.daily_summary", "type": "dashboard", ...},
    #     ...
    #   ]
    # }
```

### 5.3 Breaking Change Detection

Schema changes are detected by comparing ingestion runs:

```python
# File: governance/openmetadata/scripts/schema_change_detector.py

def detect_schema_changes(table_fqn: str) -> list:
    """
    Compares current schema with the previous version to detect
    breaking and non-breaking changes.
    """
    # Get table version history
    resp = requests.get(
        f"{OM_SERVER}/tables/name/{table_fqn}/versions",
        headers=HEADERS,
    )
    versions = resp.json()

    if len(versions) < 2:
        return []

    current = versions[-1]["columns"]
    previous = versions[-2]["columns"]

    current_cols = {c["name"]: c for c in current}
    previous_cols = {c["name"]: c for c in previous}

    changes = []

    # Detect removed columns (BREAKING)
    for name in previous_cols:
        if name not in current_cols:
            changes.append({
                "type": "COLUMN_REMOVED",
                "severity": "BREAKING",
                "column": name,
                "message": f"Column '{name}' was removed",
            })

    # Detect type changes (BREAKING)
    for name in current_cols:
        if name in previous_cols:
            if current_cols[name]["dataType"] != previous_cols[name]["dataType"]:
                changes.append({
                    "type": "TYPE_CHANGED",
                    "severity": "BREAKING",
                    "column": name,
                    "message": (
                        f"Column '{name}' type changed from "
                        f"'{previous_cols[name]['dataType']}' to "
                        f"'{current_cols[name]['dataType']}'"
                    ),
                })

    # Detect added columns (NON-BREAKING)
    for name in current_cols:
        if name not in previous_cols:
            changes.append({
                "type": "COLUMN_ADDED",
                "severity": "NON_BREAKING",
                "column": name,
                "message": f"Column '{name}' was added",
            })

    return changes
```

### 5.4 Schema Change Propagation Tracking

When a schema change is detected, an automated workflow:

1. **Identifies downstream assets** via lineage API (Section 5.2).
2. **Classifies the change** as BREAKING or NON_BREAKING.
3. **Notifies owners** of impacted assets via OpenMetadata's built-in notification system.
4. **Creates a task** in OpenMetadata assigned to the impacted asset owner to review compatibility.
5. **Blocks pipeline execution** (optional, for Tier 1 assets) until the change is acknowledged.

```python
# Notification configuration in OpenMetadata UI:
# Settings > Notifications > Add Alert
#
# Alert name: Schema Change Alert
# Trigger: Schema change on any Tier 1 or Tier 2 table
# Destination: Slack webhook / Email to asset owners
# Filter: severity = BREAKING
```

---

## 6. Access Control (RBAC)

### 6.1 Role Definitions

| Role | Description | Scope |
|---|---|---|
| `data_engineer` | Full access to all data assets, pipelines, and system configuration. Can create, modify, and delete metadata. | All layers (Bronze, Silver, Gold), all services |
| `data_analyst` | Read access to Silver and Gold layers. Can add descriptions, tags, and glossary terms. Cannot access Bronze or modify pipelines. | Silver, Gold layers only |
| `data_scientist` | Read access to all layers including Bronze. Write access to a designated sandbox schema for experimentation. | All layers (read), sandbox (write) |
| `business_user` | Read access to Gold layer only, exclusively through the GraphQL API and Superset dashboards. Cannot access catalog directly. | Gold layer via API/Superset |
| `admin` | Full system administration including user management, policy creation, and service configuration. | Unrestricted |

### 6.2 Role Configuration in OpenMetadata

**File**: `governance/openmetadata/config/rbac/roles.json`

```json
[
  {
    "name": "DataEngineer",
    "displayName": "Data Engineer",
    "description": "Full access to all data and pipelines",
    "policies": [
      "DataEngineerPolicy"
    ]
  },
  {
    "name": "DataAnalyst",
    "displayName": "Data Analyst",
    "description": "Read access to Silver/Gold, no Bronze",
    "policies": [
      "DataAnalystPolicy"
    ]
  },
  {
    "name": "DataScientist",
    "displayName": "Data Scientist",
    "description": "Read all layers, write to sandbox",
    "policies": [
      "DataScientistPolicy"
    ]
  },
  {
    "name": "BusinessUser",
    "displayName": "Business User",
    "description": "Read Gold only via API/Superset",
    "policies": [
      "BusinessUserPolicy"
    ]
  }
]
```

### 6.3 Policy Definitions

**File**: `governance/openmetadata/config/rbac/policies.json`

```json
[
  {
    "name": "DataEngineerPolicy",
    "displayName": "Data Engineer Policy",
    "description": "Full access to all data assets and pipelines",
    "rules": [
      {
        "name": "AllDataAccess",
        "resources": ["table", "topic", "pipeline", "dashboard", "mlmodel"],
        "operations": ["ViewAll", "EditAll", "Create", "Delete"],
        "effect": "allow",
        "condition": null
      },
      {
        "name": "AllServiceAccess",
        "resources": ["databaseService", "messagingService", "pipelineService"],
        "operations": ["ViewAll", "EditAll"],
        "effect": "allow",
        "condition": null
      }
    ]
  },
  {
    "name": "DataAnalystPolicy",
    "displayName": "Data Analyst Policy",
    "description": "Read Silver/Gold tables, no Bronze access",
    "rules": [
      {
        "name": "SilverGoldReadAccess",
        "resources": ["table"],
        "operations": ["ViewBasic", "ViewSampleData", "ViewQueries", "ViewUsage"],
        "effect": "allow",
        "condition": "matchAnyTag('DataLayer.Silver', 'DataLayer.Gold')"
      },
      {
        "name": "DenyBronzeAccess",
        "resources": ["table"],
        "operations": ["ViewAll"],
        "effect": "deny",
        "condition": "matchAnyTag('DataLayer.Bronze')"
      },
      {
        "name": "EnrichmentAccess",
        "resources": ["table", "topic"],
        "operations": ["EditDescription", "EditTags", "EditGlossaryTerms"],
        "effect": "allow",
        "condition": null
      },
      {
        "name": "DashboardViewAccess",
        "resources": ["dashboard"],
        "operations": ["ViewBasic", "ViewUsage"],
        "effect": "allow",
        "condition": null
      }
    ]
  },
  {
    "name": "DataScientistPolicy",
    "displayName": "Data Scientist Policy",
    "description": "Read all layers, write to sandbox",
    "rules": [
      {
        "name": "AllLayerReadAccess",
        "resources": ["table", "topic"],
        "operations": ["ViewBasic", "ViewSampleData", "ViewQueries", "ViewUsage", "ViewProfiler"],
        "effect": "allow",
        "condition": null
      },
      {
        "name": "SandboxWriteAccess",
        "resources": ["table"],
        "operations": ["ViewAll", "EditAll", "Create", "Delete"],
        "effect": "allow",
        "condition": "matchAnyTag('DataLayer.Sandbox')"
      },
      {
        "name": "PipelineViewAccess",
        "resources": ["pipeline"],
        "operations": ["ViewBasic", "ViewUsage"],
        "effect": "allow",
        "condition": null
      }
    ]
  },
  {
    "name": "BusinessUserPolicy",
    "displayName": "Business User Policy",
    "description": "Read Gold layer only via API/Superset",
    "rules": [
      {
        "name": "GoldReadAccess",
        "resources": ["table"],
        "operations": ["ViewBasic", "ViewUsage"],
        "effect": "allow",
        "condition": "matchAnyTag('DataLayer.Gold')"
      },
      {
        "name": "DenySilverBronzeAccess",
        "resources": ["table"],
        "operations": ["ViewAll"],
        "effect": "deny",
        "condition": "matchAnyTag('DataLayer.Silver', 'DataLayer.Bronze')"
      },
      {
        "name": "DashboardAccess",
        "resources": ["dashboard"],
        "operations": ["ViewBasic"],
        "effect": "allow",
        "condition": null
      }
    ]
  }
]
```

### 6.4 Team-to-Role Mapping

| Team | Role | Members (Example) |
|---|---|---|
| `platform-engineering` | `admin` | Platform Lead, SRE |
| `data-engineering` | `data_engineer` | DE Lead, Senior DE, DE |
| `analytics` | `data_analyst` | Analytics Lead, Senior Analyst, Analyst |
| `data-science` | `data_scientist` | DS Lead, Senior DS, DS |
| `business-intelligence` | `business_user` | BI Lead, Business Stakeholders |

### 6.5 Environment-Based Access

| Environment | `data_engineer` | `data_analyst` | `data_scientist` | `business_user` |
|---|---|---|---|---|
| **Production** | Read-Write (with approval for DDL) | Read-Only | Read-Only | Read-Only (Gold) |
| **Staging** | Read-Write | Read-Write (Silver/Gold) | Read-Write (sandbox) | No Access |
| **Development** | Unrestricted | Read-Write | Read-Write | No Access |

---

## 7. Row-Level and Column-Level Security

### 7.1 Row-Level Security

Row-level security (RLS) restricts which rows a user can see based on their identity or group membership.

#### 7.1.1 Filter Trades by Account Ownership

Traders should only see trades associated with their own accounts.

**Implementation via DuckDB Views:**

```sql
-- File: governance/security/rls/account_filter_view.sql

-- Create a view that filters trades by the current user's account
CREATE OR REPLACE VIEW silver.trades_user_filtered AS
SELECT *
FROM silver.trades
WHERE account_id IN (
    SELECT account_id
    FROM security.user_account_mapping
    WHERE user_id = current_user()
);

-- User-account mapping table
CREATE TABLE IF NOT EXISTS security.user_account_mapping (
    user_id     VARCHAR NOT NULL,
    account_id  VARCHAR NOT NULL,
    granted_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    granted_by  VARCHAR NOT NULL,
    PRIMARY KEY (user_id, account_id)
);
```

**Implementation via GraphQL API Layer:**

```python
# File: api/graphql/middleware/rls_middleware.py

class RowLevelSecurityMiddleware:
    """
    GraphQL middleware that injects row-level filters based on
    the authenticated user's permissions.
    """

    def __init__(self):
        self.account_service = AccountMappingService()

    def resolve(self, next, root, info, **kwargs):
        user = info.context.get("user")
        if not user:
            raise PermissionError("Authentication required")

        # Inject account filter for trade queries
        if info.field_name in ("trades", "tradeHistory", "traderPerformance"):
            if user.role == "business_user":
                # Business users see only their accounts
                allowed_accounts = self.account_service.get_user_accounts(user.id)
                kwargs["account_filter"] = allowed_accounts
            elif user.role == "data_analyst":
                # Analysts see all accounts but no PII columns
                pass  # Column-level security handles this
            # data_engineer and admin see everything

        return next(root, info, **kwargs)
```

#### 7.1.2 Regional Data Restrictions

For regulatory compliance, trades from certain regions may be restricted.

```sql
-- Trades from EU exchanges are only visible to EU-authorized users
CREATE OR REPLACE VIEW silver.trades_region_filtered AS
SELECT *
FROM silver.trades
WHERE
    CASE
        WHEN current_user_region() = 'EU' THEN TRUE
        WHEN exchange NOT IN ('LSE', 'XETRA', 'EURONEXT') THEN TRUE
        ELSE FALSE
    END;
```

### 7.2 Column-Level Security

Column-level security (CLS) restricts which columns a user can see.

#### 7.2.1 Restrict Access to Sensitive Columns

```sql
-- File: governance/security/cls/masked_views.sql

-- View for data_analyst role: PII columns are hidden
CREATE OR REPLACE VIEW silver.trades_analyst AS
SELECT
    surrogate_key,
    trade_id,
    symbol,
    price,
    quantity,
    trade_timestamp,
    side,
    exchange,
    -- PII columns excluded:
    -- trader_id (hidden)
    -- account_id (hidden)
    valid_from,
    valid_to,
    is_current
FROM silver.trades;

-- View for business_user role: minimal columns from Gold only
CREATE OR REPLACE VIEW gold.daily_summary_business AS
SELECT
    symbol,
    trade_date,
    open_price,
    close_price,
    high_price,
    low_price,
    total_volume,
    trade_count,
    vwap
    -- total_value excluded (confidential)
    -- trader-level metrics excluded
FROM gold.daily_trade_summary;
```

#### 7.2.2 Policy Enforcement in GraphQL API Layer

```python
# File: api/graphql/middleware/cls_middleware.py

# Column visibility rules per role
COLUMN_POLICIES = {
    "silver.trades": {
        "business_user": {
            "denied_columns": ["trader_id", "account_id", "surrogate_key",
                               "valid_from", "valid_to", "is_current"],
        },
        "data_analyst": {
            "denied_columns": ["trader_id", "account_id"],
        },
        "data_scientist": {
            "denied_columns": [],  # Full access
        },
        "data_engineer": {
            "denied_columns": [],  # Full access
        },
    },
    "gold.daily_trade_summary": {
        "business_user": {
            "denied_columns": ["total_value"],
        },
    },
}


class ColumnLevelSecurityMiddleware:
    """
    Strips unauthorized columns from GraphQL query results
    based on the user's role and the table being queried.
    """

    def resolve(self, next, root, info, **kwargs):
        result = next(root, info, **kwargs)
        user = info.context.get("user")
        table = info.context.get("source_table")

        if user and table and table in COLUMN_POLICIES:
            role_policy = COLUMN_POLICIES[table].get(user.role, {})
            denied = set(role_policy.get("denied_columns", []))

            if isinstance(result, dict):
                return {k: v for k, v in result.items() if k not in denied}
            elif isinstance(result, list):
                return [
                    {k: v for k, v in row.items() if k not in denied}
                    for row in result
                ]

        return result
```

---

## 8. Data Masking and Anonymization

### 8.1 Masking Strategies

| Column | Strategy | Input Example | Output Example | Applied To Roles |
|---|---|---|---|---|
| `trader_id` | Hash-based pseudonymization (SHA-256 truncated) | `TRD-12345` | `a1b2c3d4e5f6` | `data_analyst`, `business_user` |
| `account_id` | Partial masking (show last 4 characters) | `ACC-98765432` | `ACC-****5432` | `data_analyst`, `business_user` |
| `price` | Rounding to nearest dollar | `152.3456` | `152.00` | `business_user` only |
| `trader_name` | Full redaction | `John Smith` | `[REDACTED]` | All non-engineer roles |

### 8.2 Implementation

#### 8.2.1 View-Based Masking in DuckDB

```sql
-- File: governance/security/masking/masked_views.sql

-- ============================================================
-- Masking functions
-- ============================================================

-- Hash-based pseudonymization
CREATE OR REPLACE MACRO mask_hash(value) AS
    SUBSTRING(MD5(CAST(value AS VARCHAR) || 'salt_key_2024'), 1, 12);

-- Partial masking (show last N characters)
CREATE OR REPLACE MACRO mask_partial(value, visible_chars) AS
    CONCAT(
        REPEAT('*', GREATEST(LENGTH(CAST(value AS VARCHAR)) - visible_chars, 0)),
        RIGHT(CAST(value AS VARCHAR), visible_chars)
    );

-- Rounding
CREATE OR REPLACE MACRO mask_round(value, precision) AS
    ROUND(CAST(value AS DOUBLE), precision);

-- ============================================================
-- Masked views per role
-- ============================================================

-- View for data_analyst: PII masked, prices visible
CREATE OR REPLACE VIEW silver.trades_masked_analyst AS
SELECT
    surrogate_key,
    trade_id,
    symbol,
    price,                                    -- Full precision
    quantity,
    trade_timestamp,
    mask_hash(trader_id) AS trader_id,        -- Pseudonymized
    mask_partial(account_id, 4) AS account_id, -- Last 4 visible
    side,
    exchange,
    valid_from,
    valid_to,
    is_current
FROM silver.trades;

-- View for business_user: PII hidden, prices rounded
CREATE OR REPLACE VIEW gold.daily_summary_masked_business AS
SELECT
    symbol,
    trade_date,
    mask_round(open_price, 0) AS open_price,
    mask_round(close_price, 0) AS close_price,
    mask_round(high_price, 0) AS high_price,
    mask_round(low_price, 0) AS low_price,
    total_volume,
    trade_count,
    mask_round(vwap, 2) AS vwap
FROM gold.daily_trade_summary;
```

#### 8.2.2 API-Level Masking in GraphQL Resolvers

```python
# File: api/graphql/resolvers/masking.py

import hashlib
from functools import wraps


class DataMasker:
    """Applies data masking transformations based on user role."""

    SALT = "governance_mask_salt_2024"

    @staticmethod
    def hash_pseudonymize(value: str) -> str:
        """SHA-256 based pseudonymization, truncated to 12 chars."""
        if value is None:
            return None
        hashed = hashlib.sha256(
            f"{value}{DataMasker.SALT}".encode()
        ).hexdigest()
        return hashed[:12]

    @staticmethod
    def partial_mask(value: str, visible_chars: int = 4) -> str:
        """Show only the last N characters, mask the rest."""
        if value is None:
            return None
        value_str = str(value)
        if len(value_str) <= visible_chars:
            return value_str
        masked_len = len(value_str) - visible_chars
        return ("*" * masked_len) + value_str[-visible_chars:]

    @staticmethod
    def round_value(value: float, precision: int = 0) -> float:
        """Round numeric value to given precision."""
        if value is None:
            return None
        return round(float(value), precision)

    @staticmethod
    def redact(value) -> str:
        """Fully redact a value."""
        return "[REDACTED]" if value is not None else None


# Masking rules per role and column
MASKING_RULES = {
    "data_analyst": {
        "trader_id": DataMasker.hash_pseudonymize,
        "account_id": lambda v: DataMasker.partial_mask(v, 4),
        "trader_name": DataMasker.redact,
    },
    "business_user": {
        "trader_id": DataMasker.redact,
        "account_id": DataMasker.redact,
        "trader_name": DataMasker.redact,
        "price": lambda v: DataMasker.round_value(v, 0),
    },
    "data_scientist": {
        "trader_id": DataMasker.hash_pseudonymize,
        "account_id": lambda v: DataMasker.partial_mask(v, 4),
        "trader_name": DataMasker.redact,
    },
}


def apply_masking(data: dict, user_role: str) -> dict:
    """Apply masking rules to a data record based on user role."""
    rules = MASKING_RULES.get(user_role, {})
    masked = {}
    for key, value in data.items():
        if key in rules:
            masked[key] = rules[key](value)
        else:
            masked[key] = value
    return masked
```

#### 8.2.3 Iceberg Views for Pre-Masked Datasets

For high-volume batch consumption, pre-masked Iceberg tables avoid runtime masking overhead:

```python
# File: orchestration/dagster/assets/governance/masked_tables.py

from dagster import asset, AssetIn

@asset(
    ins={"silver_trades": AssetIn(key="silver_trades")},
    description="Pre-masked Silver trades for analyst consumption. "
                "PII columns are pseudonymized. Refreshed hourly.",
    metadata={
        "dagster/column_schema": ...,
        "openmetadata_tags": ["DataLayer.Silver", "Sensitivity.Masked"],
    },
)
def silver_trades_masked(context, silver_trades):
    """
    Creates a pre-masked version of silver.trades where:
    - trader_id is hash-pseudonymized
    - account_id is partially masked
    - trader_name is redacted
    """
    query = """
    CREATE OR REPLACE TABLE silver.trades_masked AS
    SELECT
        surrogate_key,
        trade_id,
        symbol,
        price,
        quantity,
        trade_timestamp,
        SUBSTRING(MD5(CAST(trader_id AS VARCHAR) || 'salt'), 1, 12) AS trader_id,
        CONCAT(REPEAT('*', LENGTH(account_id) - 4), RIGHT(account_id, 4)) AS account_id,
        side,
        exchange,
        valid_from,
        valid_to,
        is_current
    FROM silver.trades
    WHERE is_current = TRUE
    """
    context.resources.duckdb.execute(query)
```

---

## 9. PII Handling

### 9.1 PII Inventory

| Column | PII Type | Classification | Present In Tables | Justification |
|---|---|---|---|---|
| `trader_id` | Indirect PII | Quasi-identifier | `bronze.raw_trades`, `silver.trades`, `gold.trader_performance` | Can identify individuals when combined with external data |
| `account_id` | Indirect PII | Quasi-identifier | `bronze.raw_trades`, `silver.trades` | Linked to specific trading accounts, can identify individuals |
| `trader_name` | Direct PII | Personal name | `silver.traders` (if reference data includes names) | Directly identifies a natural person |

### 9.2 PII Classification Tags in OpenMetadata

```python
# File: governance/openmetadata/scripts/pii_classification.py

import requests

OM_SERVER = "http://localhost:8585/api/v1"
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json",
}

# Create PII classification hierarchy
CLASSIFICATIONS = [
    {
        "name": "PII",
        "displayName": "Personally Identifiable Information",
        "description": "Data that can identify a natural person directly or indirectly",
        "mutuallyExclusive": False,
    },
]

PII_TAGS = [
    {
        "classification": "PII",
        "name": "DirectPII",
        "displayName": "Direct PII",
        "description": "Data that directly identifies a person (name, email, SSN, etc.)",
    },
    {
        "classification": "PII",
        "name": "IndirectPII",
        "displayName": "Indirect PII",
        "description": "Data that can identify a person when combined with other data (trader_id, account_id, IP address, etc.)",
    },
    {
        "classification": "PII",
        "name": "SensitivePII",
        "displayName": "Sensitive PII",
        "description": "Highly sensitive PII requiring additional protection (financial account numbers, biometric data, etc.)",
    },
]

# Column-to-PII-tag mapping
COLUMN_PII_TAGS = {
    "trade-iceberg-lakehouse.bronze.raw_trades.trader_id": "PII.IndirectPII",
    "trade-iceberg-lakehouse.bronze.raw_trades.account_id": "PII.IndirectPII",
    "trade-iceberg-lakehouse.silver.trades.trader_id": "PII.IndirectPII",
    "trade-iceberg-lakehouse.silver.trades.account_id": "PII.IndirectPII",
    "trade-iceberg-lakehouse.gold.trader_performance.trader_id": "PII.IndirectPII",
}


def create_classifications():
    for cls in CLASSIFICATIONS:
        requests.put(
            f"{OM_SERVER}/classifications",
            headers=HEADERS,
            json=cls,
        )

def create_tags():
    for tag in PII_TAGS:
        requests.put(
            f"{OM_SERVER}/tags",
            headers=HEADERS,
            json=tag,
        )

def apply_pii_tags():
    for column_fqn, tag_fqn in COLUMN_PII_TAGS.items():
        # Parse table FQN and column name
        parts = column_fqn.rsplit(".", 1)
        table_fqn, col_name = parts[0], parts[1]

        resp = requests.get(
            f"{OM_SERVER}/tables/name/{table_fqn}",
            headers=HEADERS,
        )
        table = resp.json()

        for i, col in enumerate(table.get("columns", [])):
            if col["name"] == col_name:
                requests.patch(
                    f"{OM_SERVER}/tables/{table['id']}",
                    headers={**HEADERS, "Content-Type": "application/json-patch+json"},
                    json=[{
                        "op": "add",
                        "path": f"/columns/{i}/tags/-",
                        "value": {
                            "tagFQN": tag_fqn,
                            "source": "Classification",
                            "labelType": "Manual",
                        },
                    }],
                )
                print(f"Tagged {column_fqn} as {tag_fqn}")
                break
```

### 9.3 Automated PII Detection Scanning

OpenMetadata supports automated PII scanning during profiling ingestion:

```yaml
# File: governance/openmetadata/config/ingestion/dags/pii_scanner.yaml

source:
  type: iceberg
  serviceName: trade-iceberg-lakehouse
  sourceConfig:
    config:
      type: Profiler
      generateSampleData: true
      processPiiSensitive: true  # Enable automated PII detection
      confidence: 0.8            # Minimum confidence threshold
      schemaFilterPattern:
        includes:
          - "bronze"
          - "silver"
          - "gold"
      # PII detection uses NLP-based column name analysis and
      # sample data pattern matching to identify:
      # - Email addresses
      # - Phone numbers
      # - Names (first, last, full)
      # - Social Security Numbers
      # - Credit card numbers
      # - IP addresses
      # - Custom patterns (trader_id, account_id)

sink:
  type: metadata-rest
  config: {}

workflowConfig:
  openMetadataServerConfig:
    hostPort: "http://openmetadata-server:8585/api"
    authProvider: openmetadata
    securityConfig:
      jwtToken: "${OM_INGESTION_BOT_JWT}"
```

### 9.4 PII Access Audit Logging

Every access to PII-tagged columns is logged for compliance:

```python
# File: governance/audit/pii_access_logger.py

import logging
import json
from datetime import datetime, timezone

# Structured audit logger
audit_logger = logging.getLogger("pii_audit")
audit_logger.setLevel(logging.INFO)
handler = logging.FileHandler("/var/log/governance/pii_access_audit.jsonl")
handler.setFormatter(logging.Formatter("%(message)s"))
audit_logger.addHandler(handler)


def log_pii_access(
    user_id: str,
    user_role: str,
    table_fqn: str,
    columns_accessed: list[str],
    pii_columns_accessed: list[str],
    access_type: str,  # "query", "export", "api_call"
    was_masked: bool,
    request_id: str,
    source_ip: str,
):
    """
    Log every access to PII-tagged columns for compliance audit trail.
    """
    event = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": "PII_ACCESS",
        "user_id": user_id,
        "user_role": user_role,
        "table": table_fqn,
        "columns_accessed": columns_accessed,
        "pii_columns_accessed": pii_columns_accessed,
        "access_type": access_type,
        "was_masked": was_masked,
        "request_id": request_id,
        "source_ip": source_ip,
    }
    audit_logger.info(json.dumps(event))
```

### 9.5 Data Retention Policies for PII

| Data Layer | PII Columns | Retention Period | Action After Expiry |
|---|---|---|---|
| Bronze | `trader_id`, `account_id` | 90 days | Delete raw records older than 90 days |
| Silver | `trader_id`, `account_id` | 1 year | Anonymize PII columns (replace with hashes) |
| Gold | `trader_id` (in `trader_performance`) | 2 years | Aggregate to remove individual identity |

**Retention enforcement** (Dagster scheduled job):

```python
# File: orchestration/dagster/assets/governance/pii_retention.py

from dagster import asset, ScheduleDefinition, define_asset_job
from datetime import datetime, timedelta

@asset(
    description="Enforces PII data retention policies. "
                "Deletes or anonymizes PII data beyond retention period.",
)
def enforce_pii_retention(context):
    """
    Retention policy enforcement:
    1. Bronze: Delete records older than 90 days
    2. Silver: Anonymize PII in records older than 1 year
    3. Gold: Aggregate trader_performance older than 2 years
    """
    now = datetime.utcnow()

    # Bronze: Hard delete
    bronze_cutoff = (now - timedelta(days=90)).strftime("%Y-%m-%d")
    context.resources.duckdb.execute(f"""
        DELETE FROM bronze.raw_trades
        WHERE ingestion_ts < '{bronze_cutoff}'
    """)
    context.log.info(f"Bronze: Deleted records before {bronze_cutoff}")

    # Silver: Anonymize PII
    silver_cutoff = (now - timedelta(days=365)).strftime("%Y-%m-%d")
    context.resources.duckdb.execute(f"""
        UPDATE silver.trades
        SET
            trader_id = MD5(trader_id || 'retention_salt'),
            account_id = MD5(account_id || 'retention_salt')
        WHERE valid_from < '{silver_cutoff}'
          AND trader_id NOT LIKE 'anon_%'
    """)
    context.log.info(f"Silver: Anonymized PII before {silver_cutoff}")

    # Gold: Remove individual-level data
    gold_cutoff = (now - timedelta(days=730)).strftime("%Y-%m-%d")
    context.resources.duckdb.execute(f"""
        DELETE FROM gold.trader_performance
        WHERE performance_date < '{gold_cutoff}'
    """)
    context.log.info(f"Gold: Removed trader performance before {gold_cutoff}")


# Schedule: Run daily at 02:00 UTC
pii_retention_job = define_asset_job(
    name="pii_retention_enforcement",
    selection=[enforce_pii_retention],
)

pii_retention_schedule = ScheduleDefinition(
    job=pii_retention_job,
    cron_schedule="0 2 * * *",
    execution_timezone="UTC",
)
```

### 9.6 Right to Deletion Support

When a trader requests deletion of their data (GDPR Article 17 / CCPA equivalent):

```python
# File: governance/audit/right_to_deletion.py

def process_deletion_request(trader_id: str, request_id: str):
    """
    Process a right-to-deletion request for a specific trader.

    Steps:
    1. Identify all tables containing the trader's data (via lineage)
    2. Delete or anonymize data in each table
    3. Verify deletion completeness
    4. Log the deletion event for compliance
    5. Update OpenMetadata with deletion record
    """
    tables_with_pii = [
        "bronze.raw_trades",
        "silver.trades",
        "gold.trader_performance",
    ]

    deletion_log = []

    for table in tables_with_pii:
        # Count records before deletion
        count_before = db.execute(
            f"SELECT COUNT(*) FROM {table} WHERE trader_id = ?",
            [trader_id],
        ).fetchone()[0]

        if count_before == 0:
            continue

        # Anonymize rather than delete to preserve aggregate integrity
        db.execute(f"""
            UPDATE {table}
            SET trader_id = MD5(? || 'deletion_salt'),
                account_id = MD5(account_id || 'deletion_salt')
            WHERE trader_id = ?
        """, [trader_id, trader_id])

        deletion_log.append({
            "table": table,
            "records_anonymized": count_before,
            "method": "hash_anonymization",
        })

    # Log the deletion for compliance audit
    log_deletion_event(
        request_id=request_id,
        trader_id_hash=hashlib.sha256(trader_id.encode()).hexdigest(),
        tables_processed=deletion_log,
        completed_at=datetime.utcnow().isoformat(),
    )

    return deletion_log
```

---

## 10. Data Glossary

### 10.1 Business Terms

The data glossary provides a shared vocabulary for the trade data domain. Each term is defined in OpenMetadata and linked to relevant columns and tables.

#### Trading Domain Terms

| Term | Definition | Related Columns/Tables |
|---|---|---|
| **Trade** | A completed transaction where a financial instrument changes ownership. Includes buyer, seller, price, quantity, and timestamp. | `*.trades`, `*.raw_trades` |
| **Order** | An instruction to buy or sell a financial instrument at a specified price or market price. Orders become trades when matched. | `*.orders`, `*.raw_orders` |
| **Fill** | The execution of an order, either partial or complete. A single order may result in multiple fills. | `silver.trades.fill_type` |
| **Symbol** | A unique ticker identifier for a financial instrument on an exchange (e.g., AAPL, MSFT, GOOGL). | `*.symbol` columns |
| **Exchange** | A marketplace where financial instruments are traded. Examples: NYSE, NASDAQ, LSE. | `*.exchange` columns |
| **Side** | The direction of a trade or order: BUY (acquiring) or SELL (disposing). | `*.side` columns |
| **Bid** | The highest price a buyer is willing to pay for an instrument at a given time. | `silver.orders.bid_price` |
| **Ask** | The lowest price a seller is willing to accept for an instrument at a given time. | `silver.orders.ask_price` |

#### Analytics Terms

| Term | Definition | Formula | Related Columns |
|---|---|---|---|
| **VWAP** | Volume-Weighted Average Price. The average price weighted by trade volume over a period. Used as a benchmark for trade execution quality. | `SUM(price * quantity) / SUM(quantity)` | `gold.daily_trade_summary.vwap`, `gold.vwap_analysis.*` |
| **Bid-Ask Spread** | The difference between the best ask and best bid price. Indicates market liquidity; narrow spreads suggest high liquidity. | `ask_price - bid_price` | `gold.market_depth.spread` |
| **Market Depth** | The volume of buy and sell orders at various price levels. Shows the supply and demand structure of an instrument. | Aggregation of order book levels | `gold.market_depth.*` |
| **Total Value** | The monetary value of all trades: sum of individual trade values (price times quantity). | `SUM(price * quantity)` | `gold.daily_trade_summary.total_value` |
| **Trade Count** | The total number of individual trades executed in a period. | `COUNT(*)` | `gold.daily_trade_summary.trade_count` |

#### Technical / Data Engineering Terms

| Term | Definition | Related Assets |
|---|---|---|
| **SCD Type 1** | Slowly Changing Dimension Type 1: overwrites old data with new data. No history is preserved. Used for correcting errors. | `silver.instruments` |
| **SCD Type 2** | Slowly Changing Dimension Type 2: preserves full history by creating new rows with `valid_from`, `valid_to`, and `is_current` columns. | `silver.trades`, `silver.orders` |
| **SCD Type 3** | Slowly Changing Dimension Type 3: preserves limited history by adding `previous_value` columns. Trade-off between space and history depth. | (Not currently used; reserved for future needs) |
| **Surrogate Key** | A system-generated unique identifier (typically a hash or UUID) that serves as the primary key, independent of business keys. | `silver.trades.surrogate_key` |
| **Partition** | A physical division of table data based on a column value (e.g., trade_date). Enables partition pruning for query performance. | All Iceberg tables partitioned by date |
| **Bronze Layer** | The raw landing zone of the data lakehouse. Data is stored as-is from the source with no transformation. | `bronze.*` |
| **Silver Layer** | The cleaned and conformed layer. Data is deduplicated, validated, typed, and enriched with SCD tracking. | `silver.*` |
| **Gold Layer** | The business-ready aggregation layer. Data is modeled for specific use cases and consumption patterns. | `gold.*` |

### 10.2 Term Relationships and Hierarchy

```
Trading Domain
+-- Trade
|   +-- Fill (a Trade consists of one or more Fills)
|   +-- Side (every Trade has a Side)
|
+-- Order
|   +-- Fill (an Order produces Fills)
|   +-- Side (every Order has a Side)
|   +-- Bid (a BUY Order at limit creates a Bid)
|   +-- Ask (a SELL Order at limit creates an Ask)
|
+-- Instrument
|   +-- Symbol (every Instrument has a Symbol)
|   +-- Exchange (every Instrument is listed on one or more Exchanges)
|
+-- Analytics
|   +-- VWAP (derived from Trade data)
|   +-- Bid-Ask Spread (derived from Order/Bid/Ask data)
|   +-- Market Depth (derived from Order book data)
|
+-- Data Architecture
    +-- Bronze Layer
    +-- Silver Layer
    +-- Gold Layer
    +-- SCD Type 1 / 2 / 3
    +-- Surrogate Key
    +-- Partition
```

### 10.3 Glossary-to-Column Mapping

Glossary terms are linked to columns in OpenMetadata so that searching for "VWAP" shows all columns and tables related to the concept:

```python
# File: governance/openmetadata/scripts/glossary_setup.py

GLOSSARY = {
    "name": "TradeDataGlossary",
    "displayName": "Trade Data Glossary",
    "description": "Business glossary for the stock trade data pipeline",
}

GLOSSARY_TERMS = [
    {
        "name": "VWAP",
        "displayName": "Volume-Weighted Average Price",
        "description": (
            "The average price of an instrument weighted by trade volume over a period. "
            "Formula: SUM(price * quantity) / SUM(quantity). "
            "Used as a benchmark for trade execution quality."
        ),
        "synonyms": ["Volume Weighted Average Price", "volume-weighted average"],
        "relatedTerms": ["Trade", "Total Value"],
        "references": [
            {"name": "Investopedia VWAP", "endpoint": "https://www.investopedia.com/terms/v/vwap.asp"}
        ],
        # Columns tagged with this glossary term
        "assets": [
            "trade-iceberg-lakehouse.gold.daily_trade_summary.vwap",
            "trade-iceberg-lakehouse.gold.vwap_analysis.vwap_1m",
            "trade-iceberg-lakehouse.gold.vwap_analysis.vwap_5m",
            "trade-iceberg-lakehouse.gold.vwap_analysis.vwap_15m",
        ],
    },
    {
        "name": "BidAskSpread",
        "displayName": "Bid-Ask Spread",
        "description": (
            "The difference between the lowest ask price and the highest bid price. "
            "Indicates market liquidity for an instrument. "
            "Narrow spreads suggest high liquidity; wide spreads suggest low liquidity."
        ),
        "synonyms": ["Spread", "Bid-Offer Spread"],
        "relatedTerms": ["Bid", "Ask", "Market Depth"],
        "assets": [
            "trade-iceberg-lakehouse.gold.market_depth.spread",
        ],
    },
    {
        "name": "Trade",
        "displayName": "Trade",
        "description": (
            "A completed transaction where a financial instrument changes ownership. "
            "A trade record includes the symbol, price, quantity, timestamp, "
            "buyer/seller identifiers, and the exchange where it occurred."
        ),
        "synonyms": ["Execution", "Transaction"],
        "relatedTerms": ["Order", "Fill", "Side"],
        "assets": [
            "trade-iceberg-lakehouse.bronze.raw_trades",
            "trade-iceberg-lakehouse.silver.trades",
            "trade-iceberg-lakehouse.gold.daily_trade_summary",
        ],
    },
]
```

---

## 11. Implementation Steps

### 11.1 File Structure

```
DE-project/
+-- governance/
|   +-- openmetadata/
|   |   +-- docker-compose.yml                    # OpenMetadata stack deployment
|   |   +-- .env                                   # Environment variables (secrets)
|   |   +-- config/
|   |   |   +-- openmetadata/
|   |   |   |   +-- openmetadata.yaml              # Server configuration
|   |   |   +-- mysql/
|   |   |   |   +-- init.sql                       # Database initialization
|   |   |   +-- ingestion/
|   |   |   |   +-- bot-config.yaml                # Ingestion bot auth config
|   |   |   |   +-- dags/
|   |   |   |       +-- kafka_metadata_ingestion.yaml
|   |   |   |       +-- iceberg_metadata_ingestion.yaml
|   |   |   |       +-- dagster_metadata_ingestion.yaml
|   |   |   |       +-- superset_metadata_ingestion.yaml
|   |   |   |       +-- pii_scanner.yaml
|   |   |   +-- rbac/
|   |   |       +-- roles.json                     # Role definitions
|   |   |       +-- policies.json                  # Policy definitions
|   |   |       +-- teams.json                     # Team-to-role mappings
|   |   +-- scripts/
|   |       +-- bootstrap.sh                       # Initial service registration
|   |       +-- enrich_descriptions.py             # Add business descriptions
|   |       +-- apply_tags.py                      # Apply classification tags
|   |       +-- pii_classification.py              # PII tag setup and application
|   |       +-- glossary_setup.py                  # Create glossary terms
|   |       +-- impact_analysis.py                 # Impact analysis utility
|   |       +-- schema_change_detector.py          # Breaking change detection
|   |
|   +-- security/
|   |   +-- rls/
|   |   |   +-- account_filter_view.sql            # Row-level security views
|   |   |   +-- region_filter_view.sql             # Regional data restrictions
|   |   +-- cls/
|   |   |   +-- masked_views.sql                   # Column-level security views
|   |   +-- masking/
|   |       +-- masked_views.sql                   # DuckDB masking functions and views
|   |
|   +-- audit/
|       +-- pii_access_logger.py                   # PII access audit logging
|       +-- right_to_deletion.py                   # GDPR/CCPA deletion support
|
+-- orchestration/
|   +-- dagster/
|       +-- openlineage_config.py                  # Dagster OpenLineage integration
|       +-- assets/
|           +-- governance/
|               +-- masked_tables.py               # Pre-masked Iceberg tables
|               +-- pii_retention.py               # PII retention enforcement
|
+-- processing/
|   +-- flink/
|       +-- src/main/java/com/trades/lineage/
|           +-- FlinkOpenLineageListener.java       # Flink OpenLineage emitter
|
+-- api/
    +-- graphql/
        +-- middleware/
        |   +-- rls_middleware.py                   # Row-level security middleware
        |   +-- cls_middleware.py                   # Column-level security middleware
        +-- resolvers/
            +-- masking.py                         # API-level data masking
```

### 11.2 Implementation Phases

#### Phase 1: Foundation (Week 1-2)

| Step | Task | Dependencies | Output |
|---|---|---|---|
| 1.1 | Deploy OpenMetadata stack via Docker Compose | Docker, network access | Running OM server, MySQL, Elasticsearch |
| 1.2 | Configure authentication (basic auth for dev) | Step 1.1 | Admin user, ingestion bot JWT |
| 1.3 | Run bootstrap script to register services | Step 1.2 | Kafka, Iceberg, Dagster, Superset services in catalog |
| 1.4 | Run Kafka metadata ingestion | Step 1.3 | All Kafka topics discovered in catalog |
| 1.5 | Run Iceberg metadata ingestion | Step 1.3 | All Iceberg tables discovered in catalog |
| 1.6 | Run Dagster metadata ingestion | Step 1.3 | All Dagster pipelines discovered in catalog |

#### Phase 2: Enrichment (Week 2-3)

| Step | Task | Dependencies | Output |
|---|---|---|---|
| 2.1 | Create classification taxonomy (Sensitivity, DataLayer, DataDomain) | Phase 1 | Tag hierarchy in OM |
| 2.2 | Apply tags to all discovered assets | Step 2.1 | All tables/topics tagged |
| 2.3 | Add business descriptions to tables and columns | Phase 1 | Rich metadata in catalog |
| 2.4 | Create data glossary with all business terms | Phase 1 | Glossary linked to columns |
| 2.5 | Assign data owners and tier classifications | Phase 1 | Ownership and SLA metadata |
| 2.6 | Run PII scanner on all tables | Step 2.1 | Automated PII detection |
| 2.7 | Review and confirm PII tags, apply manual PII tags | Step 2.6 | Verified PII inventory |

#### Phase 3: Lineage (Week 3-4)

| Step | Task | Dependencies | Output |
|---|---|---|---|
| 3.1 | Configure Dagster OpenLineage integration | Phase 1 | Dagster emits OpenLineage events |
| 3.2 | Implement Flink OpenLineage listener | Phase 1 | Flink emits OpenLineage events |
| 3.3 | Verify table-level lineage end-to-end | Steps 3.1, 3.2 | Full pipeline lineage in OM |
| 3.4 | Add column-level lineage for Bronze-to-Silver | Step 3.3 | Column-level mappings visible |
| 3.5 | Add column-level lineage for Silver-to-Gold | Step 3.4 | Complete column lineage |
| 3.6 | Implement impact analysis script | Step 3.3 | Programmatic impact queries |
| 3.7 | Implement schema change detector | Step 3.3 | Breaking change alerts |

#### Phase 4: Security (Week 4-5)

| Step | Task | Dependencies | Output |
|---|---|---|---|
| 4.1 | Define RBAC roles and policies in OpenMetadata | Phase 1 | Roles configured |
| 4.2 | Create teams and assign roles | Step 4.1 | Team-role mappings |
| 4.3 | Implement DuckDB masking functions and views | Phase 1 | SQL-level masking |
| 4.4 | Implement row-level security views | Phase 1 | RLS in DuckDB |
| 4.5 | Implement column-level security views | Phase 1 | CLS in DuckDB |
| 4.6 | Implement GraphQL RLS/CLS middleware | Phase 1 | API-level security |
| 4.7 | Implement GraphQL masking resolvers | Phase 1 | API-level masking |
| 4.8 | Create pre-masked Iceberg tables (Dagster asset) | Steps 4.3, 4.5 | Masked datasets |

#### Phase 5: Compliance (Week 5-6)

| Step | Task | Dependencies | Output |
|---|---|---|---|
| 5.1 | Implement PII access audit logging | Phase 4 | Audit trail for PII access |
| 5.2 | Implement data retention policies (Dagster job) | Phase 2 | Scheduled PII cleanup |
| 5.3 | Implement right-to-deletion workflow | Step 5.1 | GDPR compliance capability |
| 5.4 | Configure schema change notifications | Phase 3 | Alerts for breaking changes |
| 5.5 | Document all governance processes | Phases 1-5 | Runbooks and procedures |

### 11.3 Integration Points with Other Services

| Service | Integration Type | Description |
|---|---|---|
| **Kafka** | Metadata ingestion connector | Auto-discovers topics, schemas, and consumer groups |
| **Iceberg** | Metadata ingestion connector + profiling | Discovers tables, columns, partitions; profiles data statistics |
| **Dagster** | Metadata ingestion connector + OpenLineage emitter | Discovers pipelines; emits lineage events on every run |
| **Flink** | Custom OpenLineage listener | Emits lineage events from streaming jobs |
| **DuckDB** | Masking views + RLS/CLS views | SQL-level security enforcement |
| **GraphQL API** | Middleware + resolvers | Runtime security and masking enforcement |
| **Superset** | Metadata ingestion connector | Discovers dashboards, charts, and their data sources |
| **Prometheus/Grafana** | Metrics from OM server | Monitor ingestion health, catalog completeness, lineage coverage |

---

## 12. Testing Strategy

### 12.1 Lineage Accuracy Verification

```python
# File: tests/governance/test_lineage.py

import pytest
import requests

OM_SERVER = "http://localhost:8585/api/v1"


class TestTableLevelLineage:
    """Verify that table-level lineage is complete and accurate."""

    def test_bronze_raw_trades_has_upstream_kafka(self, om_client):
        """Bronze raw_trades should have Kafka raw.trades as upstream."""
        lineage = om_client.get_lineage(
            "table", "trade-iceberg-lakehouse.bronze.raw_trades",
            upstream_depth=1
        )
        upstream_names = [
            e["fullyQualifiedName"] for e in lineage["upstreamEdges"]
        ]
        assert "trade-kafka-cluster.raw.trades" in upstream_names

    def test_silver_trades_has_upstream_bronze(self, om_client):
        """Silver trades should have Bronze raw_trades as upstream."""
        lineage = om_client.get_lineage(
            "table", "trade-iceberg-lakehouse.silver.trades",
            upstream_depth=1
        )
        upstream_names = [
            e["fullyQualifiedName"] for e in lineage["upstreamEdges"]
        ]
        assert "trade-iceberg-lakehouse.bronze.raw_trades" in upstream_names

    def test_gold_daily_summary_has_upstream_silver(self, om_client):
        """Gold daily_trade_summary should have Silver trades as upstream."""
        lineage = om_client.get_lineage(
            "table", "trade-iceberg-lakehouse.gold.daily_trade_summary",
            upstream_depth=1
        )
        upstream_names = [
            e["fullyQualifiedName"] for e in lineage["upstreamEdges"]
        ]
        assert "trade-iceberg-lakehouse.silver.trades" in upstream_names

    def test_end_to_end_lineage_depth(self, om_client):
        """From Gold daily_trade_summary, full upstream should reach Kafka."""
        lineage = om_client.get_lineage(
            "table", "trade-iceberg-lakehouse.gold.daily_trade_summary",
            upstream_depth=10
        )
        all_upstream = collect_all_upstream_names(lineage)
        assert "trade-kafka-cluster.raw.trades" in all_upstream

    def test_graphql_api_has_downstream_from_gold(self, om_client):
        """GraphQL API should appear as downstream consumer of Gold tables."""
        lineage = om_client.get_lineage(
            "table", "trade-iceberg-lakehouse.gold.daily_trade_summary",
            downstream_depth=2
        )
        downstream_types = [
            e["type"] for e in lineage["downstreamEdges"]
        ]
        assert "dashboard" in downstream_types or "pipeline" in downstream_types


class TestColumnLevelLineage:
    """Verify column-level lineage mappings."""

    def test_bronze_price_maps_to_silver_price(self, om_client):
        """bronze.raw_trades.price should map to silver.trades.price."""
        col_lineage = om_client.get_column_lineage(
            "trade-iceberg-lakehouse.silver.trades"
        )
        price_sources = col_lineage.get("price", {}).get("sources", [])
        assert any(
            s["column"] == "price" and "bronze.raw_trades" in s["table"]
            for s in price_sources
        )

    def test_silver_price_quantity_maps_to_gold_total_value(self, om_client):
        """silver.trades.price * quantity should map to gold.daily_trade_summary.total_value."""
        col_lineage = om_client.get_column_lineage(
            "trade-iceberg-lakehouse.gold.daily_trade_summary"
        )
        total_value_sources = col_lineage.get("total_value", {}).get("sources", [])
        source_columns = {s["column"] for s in total_value_sources}
        assert "price" in source_columns
        assert "quantity" in source_columns

    def test_vwap_column_lineage(self, om_client):
        """gold.daily_trade_summary.vwap should derive from silver price and quantity."""
        col_lineage = om_client.get_column_lineage(
            "trade-iceberg-lakehouse.gold.daily_trade_summary"
        )
        vwap_sources = col_lineage.get("vwap", {}).get("sources", [])
        source_columns = {s["column"] for s in vwap_sources}
        assert "price" in source_columns
        assert "quantity" in source_columns


class TestLineageCompleteness:
    """Verify no orphaned nodes or missing edges."""

    def test_all_silver_tables_have_upstream(self, om_client):
        """Every Silver table must have at least one upstream Bronze table."""
        silver_tables = om_client.list_tables(schema="silver")
        for table in silver_tables:
            lineage = om_client.get_lineage("table", table["fqn"], upstream_depth=1)
            assert len(lineage.get("upstreamEdges", [])) > 0, \
                f"Silver table {table['fqn']} has no upstream lineage"

    def test_all_gold_tables_have_upstream(self, om_client):
        """Every Gold table must have at least one upstream Silver table."""
        gold_tables = om_client.list_tables(schema="gold")
        for table in gold_tables:
            lineage = om_client.get_lineage("table", table["fqn"], upstream_depth=1)
            assert len(lineage.get("upstreamEdges", [])) > 0, \
                f"Gold table {table['fqn']} has no upstream lineage"
```

### 12.2 RBAC Policy Tests

```python
# File: tests/governance/test_rbac.py

import pytest


class TestDataEngineerRole:
    """Verify data_engineer role has full access."""

    def test_can_read_bronze_tables(self, om_client_as_engineer):
        resp = om_client_as_engineer.get_table("trade-iceberg-lakehouse.bronze.raw_trades")
        assert resp.status_code == 200

    def test_can_read_silver_tables(self, om_client_as_engineer):
        resp = om_client_as_engineer.get_table("trade-iceberg-lakehouse.silver.trades")
        assert resp.status_code == 200

    def test_can_read_gold_tables(self, om_client_as_engineer):
        resp = om_client_as_engineer.get_table("trade-iceberg-lakehouse.gold.daily_trade_summary")
        assert resp.status_code == 200

    def test_can_edit_table_description(self, om_client_as_engineer):
        resp = om_client_as_engineer.patch_table(
            "trade-iceberg-lakehouse.bronze.raw_trades",
            [{"op": "add", "path": "/description", "value": "test"}]
        )
        assert resp.status_code == 200

    def test_can_access_pipelines(self, om_client_as_engineer):
        resp = om_client_as_engineer.get_pipeline("trade-dagster-orchestrator.bronze_to_silver")
        assert resp.status_code == 200


class TestDataAnalystRole:
    """Verify data_analyst role: Silver/Gold read, no Bronze."""

    def test_cannot_read_bronze_tables(self, om_client_as_analyst):
        resp = om_client_as_analyst.get_table("trade-iceberg-lakehouse.bronze.raw_trades")
        assert resp.status_code == 403

    def test_can_read_silver_tables(self, om_client_as_analyst):
        resp = om_client_as_analyst.get_table("trade-iceberg-lakehouse.silver.trades")
        assert resp.status_code == 200

    def test_can_read_gold_tables(self, om_client_as_analyst):
        resp = om_client_as_analyst.get_table("trade-iceberg-lakehouse.gold.daily_trade_summary")
        assert resp.status_code == 200

    def test_can_add_descriptions(self, om_client_as_analyst):
        resp = om_client_as_analyst.patch_table(
            "trade-iceberg-lakehouse.silver.trades",
            [{"op": "add", "path": "/description", "value": "analyst note"}]
        )
        assert resp.status_code == 200

    def test_cannot_delete_tables(self, om_client_as_analyst):
        resp = om_client_as_analyst.delete_table("trade-iceberg-lakehouse.silver.trades")
        assert resp.status_code == 403


class TestBusinessUserRole:
    """Verify business_user role: Gold only, no Silver/Bronze."""

    def test_cannot_read_bronze_tables(self, om_client_as_business):
        resp = om_client_as_business.get_table("trade-iceberg-lakehouse.bronze.raw_trades")
        assert resp.status_code == 403

    def test_cannot_read_silver_tables(self, om_client_as_business):
        resp = om_client_as_business.get_table("trade-iceberg-lakehouse.silver.trades")
        assert resp.status_code == 403

    def test_can_read_gold_tables(self, om_client_as_business):
        resp = om_client_as_business.get_table("trade-iceberg-lakehouse.gold.daily_trade_summary")
        assert resp.status_code == 200

    def test_can_view_dashboards(self, om_client_as_business):
        resp = om_client_as_business.get_dashboard("trade-superset-dashboards.daily_summary")
        assert resp.status_code == 200


class TestDataScientistRole:
    """Verify data_scientist role: read all, write sandbox."""

    def test_can_read_bronze_tables(self, om_client_as_scientist):
        resp = om_client_as_scientist.get_table("trade-iceberg-lakehouse.bronze.raw_trades")
        assert resp.status_code == 200

    def test_can_read_silver_tables(self, om_client_as_scientist):
        resp = om_client_as_scientist.get_table("trade-iceberg-lakehouse.silver.trades")
        assert resp.status_code == 200

    def test_can_read_gold_tables(self, om_client_as_scientist):
        resp = om_client_as_scientist.get_table("trade-iceberg-lakehouse.gold.daily_trade_summary")
        assert resp.status_code == 200

    def test_cannot_write_silver_tables(self, om_client_as_scientist):
        resp = om_client_as_scientist.delete_table("trade-iceberg-lakehouse.silver.trades")
        assert resp.status_code == 403

    def test_can_write_sandbox_tables(self, om_client_as_scientist):
        resp = om_client_as_scientist.create_table("trade-iceberg-lakehouse.sandbox.experiment_1")
        assert resp.status_code == 200
```

### 12.3 Masking Effectiveness Tests

```python
# File: tests/governance/test_masking.py

import pytest
import re
import hashlib


class TestHashPseudonymization:
    """Verify hash-based pseudonymization for trader_id."""

    def test_trader_id_is_masked(self, masked_trade):
        """Masked trader_id should be a 12-char hex string, not the original."""
        assert masked_trade["trader_id"] != "TRD-12345"
        assert re.match(r"^[a-f0-9]{12}$", masked_trade["trader_id"])

    def test_same_input_same_hash(self, masker):
        """Same trader_id should always produce the same pseudonym (deterministic)."""
        result1 = masker.hash_pseudonymize("TRD-12345")
        result2 = masker.hash_pseudonymize("TRD-12345")
        assert result1 == result2

    def test_different_input_different_hash(self, masker):
        """Different trader_ids should produce different pseudonyms."""
        result1 = masker.hash_pseudonymize("TRD-12345")
        result2 = masker.hash_pseudonymize("TRD-67890")
        assert result1 != result2

    def test_hash_is_not_reversible(self, masker):
        """Should not be possible to derive original from pseudonym."""
        pseudonym = masker.hash_pseudonymize("TRD-12345")
        # Pseudonym should not contain the original value
        assert "12345" not in pseudonym
        assert "TRD" not in pseudonym


class TestPartialMasking:
    """Verify partial masking for account_id."""

    def test_account_id_partially_masked(self, masked_trade):
        """Only last 4 characters should be visible."""
        account = masked_trade["account_id"]
        assert account.endswith("5432")
        assert account.startswith("*")

    def test_mask_preserves_last_n_chars(self, masker):
        result = masker.partial_mask("ACC-98765432", 4)
        assert result == "********5432"

    def test_mask_short_value(self, masker):
        """Values shorter than visible_chars should be returned as-is."""
        result = masker.partial_mask("AB", 4)
        assert result == "AB"

    def test_mask_none_value(self, masker):
        """None values should remain None."""
        result = masker.partial_mask(None, 4)
        assert result is None


class TestRoundingMask:
    """Verify rounding mask for price column."""

    def test_price_rounded_for_business_user(self, masked_trade_business):
        """Business users should see rounded prices."""
        assert masked_trade_business["price"] == 152.0  # Not 152.3456

    def test_price_full_precision_for_analyst(self, masked_trade_analyst):
        """Analysts should see full price precision."""
        assert masked_trade_analyst["price"] == 152.3456


class TestRedaction:
    """Verify full redaction for trader_name."""

    def test_trader_name_redacted(self, masked_trade):
        assert masked_trade["trader_name"] == "[REDACTED]"

    def test_null_value_stays_null(self, masker):
        result = masker.redact(None)
        assert result is None


class TestRoleMaskingCombination:
    """Verify correct masking combinations per role."""

    def test_data_engineer_sees_everything(self, unmasked_trade, raw_trade):
        """Data engineers should see raw, unmasked data."""
        assert unmasked_trade["trader_id"] == raw_trade["trader_id"]
        assert unmasked_trade["account_id"] == raw_trade["account_id"]
        assert unmasked_trade["price"] == raw_trade["price"]

    def test_analyst_sees_masked_pii_full_price(self, masked_trade_analyst, raw_trade):
        """Analysts: masked PII, full-precision prices."""
        assert masked_trade_analyst["trader_id"] != raw_trade["trader_id"]
        assert masked_trade_analyst["price"] == raw_trade["price"]

    def test_business_user_sees_masked_everything(self, masked_trade_business, raw_trade):
        """Business users: redacted PII, rounded prices."""
        assert masked_trade_business["trader_id"] == "[REDACTED]"
        assert masked_trade_business["account_id"] == "[REDACTED]"
        assert masked_trade_business["price"] != raw_trade["price"]


class TestDuckDBMaskedViews:
    """Verify DuckDB view-based masking works correctly."""

    def test_analyst_view_excludes_raw_pii(self, duckdb_conn):
        """silver.trades_analyst view should not contain trader_id or account_id columns."""
        columns = duckdb_conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'trades_analyst'"
        ).fetchall()
        col_names = {c[0] for c in columns}
        assert "trader_id" not in col_names
        assert "account_id" not in col_names

    def test_masked_analyst_view_has_hashed_trader(self, duckdb_conn):
        """silver.trades_masked_analyst should have hashed trader_id."""
        row = duckdb_conn.execute(
            "SELECT trader_id FROM silver.trades_masked_analyst LIMIT 1"
        ).fetchone()
        assert re.match(r"^[a-f0-9]{12}$", row[0])

    def test_business_view_has_rounded_prices(self, duckdb_conn):
        """gold.daily_summary_masked_business should have integer prices."""
        row = duckdb_conn.execute(
            "SELECT open_price FROM gold.daily_summary_masked_business LIMIT 1"
        ).fetchone()
        assert row[0] == int(row[0])  # Should be a whole number
```

### 12.4 Integration Test: End-to-End Governance

```python
# File: tests/governance/test_e2e_governance.py

class TestEndToEndGovernance:
    """
    Integration tests that verify the complete governance flow:
    catalog -> lineage -> RBAC -> masking -> audit
    """

    def test_new_table_is_discovered_and_tagged(self, om_client, iceberg_catalog):
        """
        When a new Iceberg table is created, it should:
        1. Be discovered by the metadata ingestion
        2. Get PII tags from automated scanning
        3. Appear in the lineage graph
        """
        # Create a test table
        iceberg_catalog.create_table("silver.test_governance_table", schema=...)

        # Trigger ingestion
        om_client.trigger_ingestion("iceberg_metadata_ingestion")
        om_client.trigger_ingestion("pii_scanner")

        # Verify discovery
        table = om_client.get_table("trade-iceberg-lakehouse.silver.test_governance_table")
        assert table is not None

        # Verify PII scan ran
        assert any(
            tag["tagFQN"].startswith("PII.")
            for col in table["columns"]
            for tag in col.get("tags", [])
            if col["name"] in ("trader_id", "account_id")
        )

    def test_pii_access_is_audited(self, graphql_client, audit_log):
        """
        When a user queries PII columns via GraphQL, the access should
        be logged in the PII audit trail.
        """
        # Query trades as analyst (PII is masked but access is logged)
        graphql_client.login_as("analyst_user", role="data_analyst")
        graphql_client.query("{ trades(limit: 10) { traderId accountId } }")

        # Verify audit log entry
        recent_entries = audit_log.get_recent(event_type="PII_ACCESS", limit=1)
        assert len(recent_entries) == 1
        assert recent_entries[0]["user_role"] == "data_analyst"
        assert "trader_id" in recent_entries[0]["pii_columns_accessed"]
        assert recent_entries[0]["was_masked"] is True

    def test_schema_change_triggers_impact_alert(self, om_client, alert_sink):
        """
        When a Bronze table schema changes (column removed),
        downstream owners should be notified.
        """
        # Simulate schema change by re-ingesting with modified schema
        om_client.trigger_ingestion("iceberg_metadata_ingestion")

        # Check that alerts were generated
        alerts = alert_sink.get_alerts(type="SCHEMA_CHANGE")
        breaking_alerts = [a for a in alerts if a["severity"] == "BREAKING"]
        assert len(breaking_alerts) > 0
        assert any(
            "silver.trades" in a["impacted_asset"]
            for a in breaking_alerts
        )
```

---

## Appendix: Quick Reference

### OpenMetadata API Endpoints

| Operation | Method | Endpoint |
|---|---|---|
| List tables | GET | `/api/v1/tables` |
| Get table by FQN | GET | `/api/v1/tables/name/{fqn}` |
| Patch table | PATCH | `/api/v1/tables/{id}` |
| Get lineage | GET | `/api/v1/lineage/{entity}/{fqn}` |
| Add lineage edge | PUT | `/api/v1/lineage` |
| OpenLineage event | POST | `/api/v1/lineage/openlineage` |
| List glossary terms | GET | `/api/v1/glossaryTerms` |
| Create classification | PUT | `/api/v1/classifications` |
| Create tag | PUT | `/api/v1/tags` |
| List policies | GET | `/api/v1/policies` |
| Create role | PUT | `/api/v1/roles` |
| Create team | PUT | `/api/v1/teams` |
| Trigger ingestion | POST | `/api/v1/services/ingestionPipelines/trigger/{id}` |

### Environment Variables

| Variable | Description | Default |
|---|---|---|
| `OM_MYSQL_ROOT_PASSWORD` | MySQL root password | `openmetadata_root` |
| `OM_MYSQL_PASSWORD` | MySQL user password | `openmetadata_password` |
| `OM_INGESTION_BOT_JWT` | JWT token for ingestion bot | (generated in UI) |
| `OM_ADMIN_TOKEN` | Admin JWT for bootstrap scripts | (generated in UI) |
| `SUPERSET_PASSWORD` | Superset admin password for connector | (from Superset config) |

### Key Commands

```bash
# Start the governance stack
cd governance/openmetadata && docker compose up -d

# Check server health
curl http://localhost:8585/api/v1/system/version

# Run bootstrap
./governance/openmetadata/scripts/bootstrap.sh

# Run metadata ingestion (via Airflow UI)
# Navigate to http://localhost:8080 and trigger DAGs

# Access OpenMetadata UI
# Navigate to http://localhost:8585
```
