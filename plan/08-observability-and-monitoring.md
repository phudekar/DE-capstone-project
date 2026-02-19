# 08 - Data Observability & Monitoring Implementation Plan

## Table of Contents

1. [Overview](#1-overview)
2. [Monitoring Stack Setup](#2-monitoring-stack-setup)
3. [Metrics Collection by Component](#3-metrics-collection-by-component)
4. [Data Observability Metrics (Custom)](#4-data-observability-metrics-custom)
5. [Grafana Dashboards (as code)](#5-grafana-dashboards-as-code-json-provisioning)
6. [Alerting Rules](#6-alerting-rules)
7. [Log Aggregation (Optional Enhancement)](#7-log-aggregation-optional-enhancement)
8. [Implementation Steps](#8-implementation-steps)
9. [Testing Strategy](#9-testing-strategy)
10. [File Tree Summary](#10-file-tree-summary)

---

## 1. Overview

### Purpose

This component provides end-to-end observability for the stock market trade data pipeline.
It answers five key questions continuously:

| Pillar       | Question                                      |
|--------------|-----------------------------------------------|
| Freshness    | Is data arriving on time?                     |
| Volume       | Are row counts within the expected range?     |
| Schema       | Have columns changed unexpectedly?            |
| Distribution | Are value distributions anomalous?            |
| Lineage      | Where did breakages originate?                |

### Architecture Diagram (ASCII)

```
+------------------+     +------------------+     +------------------+
| Trade Generator  |     | Kafka Brokers    |     | Flink Jobs       |
| (custom metrics) |     | (JMX Exporter)   |     | (Prom Reporter)  |
+--------+---------+     +--------+---------+     +--------+---------+
         |                        |                        |
         v                        v                        v
+------------------------------------------------------------------------+
|                        Prometheus Server                               |
|   - Scrape targets every 15s                                           |
|   - 15-day retention                                                   |
|   - Recording rules for aggregations                                   |
+----------------------------+-------------------------------------------+
                             |
              +--------------+--------------+
              |                             |
              v                             v
    +---------+----------+       +----------+---------+
    |   Grafana          |       |   Alertmanager     |
    | - 6 dashboards     |       | - Slack webhook    |
    | - Provisioned as   |       | - Email (SMTP)     |
    |   code (JSON)      |       | - Silence rules    |
    +--------------------+       +--------------------+

+------------------+     +------------------+
| Dagster          |     | GraphQL API      |
| (Pushgateway)    |     | (custom /metrics)|
+--------+---------+     +--------+---------+
         |                        |
         +-------> Prometheus <---+

+------------------+     +------------------+
| Node Exporter    |     | cAdvisor         |
| (host metrics)   |     | (container)      |
+--------+---------+     +--------+---------+
         |                        |
         +-------> Prometheus <---+
```

### Technology Choices

| Tool           | Role                             | Version  |
|----------------|----------------------------------|----------|
| Prometheus     | Metrics collection & storage     | 2.51+    |
| Grafana        | Visualization & dashboarding     | 10.4+    |
| Alertmanager   | Alert routing & notification     | 0.27+    |
| Pushgateway    | Batch job metric bridge          | 1.8+     |
| JMX Exporter   | Kafka JMX to Prometheus bridge   | 0.20+    |
| Node Exporter  | Host-level metrics               | 1.8+     |
| cAdvisor       | Container-level metrics          | 0.49+    |

---

## 2. Monitoring Stack Setup

### 2.1 Prometheus Configuration

**File**: `monitoring/prometheus/prometheus.yml`

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s
  scrape_timeout: 10s

rule_files:
  - "/etc/prometheus/rules/*.yml"

alerting:
  alertmanagers:
    - static_configs:
        - targets:
            - "alertmanager:9093"

scrape_configs:
  # -----------------------------------------------------------
  # Prometheus self-monitoring
  # -----------------------------------------------------------
  - job_name: "prometheus"
    static_configs:
      - targets: ["localhost:9090"]

  # -----------------------------------------------------------
  # Kafka Brokers (JMX Exporter sidecar on each broker)
  # -----------------------------------------------------------
  - job_name: "kafka"
    static_configs:
      - targets:
          - "kafka-broker-1:7071"
          - "kafka-broker-2:7071"
          - "kafka-broker-3:7071"
        labels:
          cluster: "trade-pipeline"

  # -----------------------------------------------------------
  # Flink JobManager & TaskManagers (Prometheus Reporter)
  # -----------------------------------------------------------
  - job_name: "flink"
    static_configs:
      - targets:
          - "flink-jobmanager:9249"
          - "flink-taskmanager-1:9249"
          - "flink-taskmanager-2:9249"

  # -----------------------------------------------------------
  # Dagster metrics via Pushgateway
  # -----------------------------------------------------------
  - job_name: "pushgateway"
    honor_labels: true
    static_configs:
      - targets: ["pushgateway:9091"]

  # -----------------------------------------------------------
  # Trade Generator application
  # -----------------------------------------------------------
  - job_name: "trade-generator"
    static_configs:
      - targets: ["trade-generator:8000"]
    metrics_path: "/metrics"

  # -----------------------------------------------------------
  # Kafka Consumer / Bridge application
  # -----------------------------------------------------------
  - job_name: "kafka-consumer"
    static_configs:
      - targets: ["kafka-consumer:8001"]
    metrics_path: "/metrics"

  # -----------------------------------------------------------
  # GraphQL API
  # -----------------------------------------------------------
  - job_name: "graphql-api"
    static_configs:
      - targets: ["graphql-api:8002"]
    metrics_path: "/metrics"

  # -----------------------------------------------------------
  # Node Exporter (host metrics)
  # -----------------------------------------------------------
  - job_name: "node-exporter"
    static_configs:
      - targets: ["node-exporter:9100"]

  # -----------------------------------------------------------
  # cAdvisor (container metrics)
  # -----------------------------------------------------------
  - job_name: "cadvisor"
    static_configs:
      - targets: ["cadvisor:8080"]
```

**Storage & Retention** (command-line flags):

```
--storage.tsdb.path=/prometheus/data
--storage.tsdb.retention.time=15d
--storage.tsdb.retention.size=10GB
--web.enable-lifecycle
--web.enable-admin-api
```

### 2.2 Grafana Provisioning

#### Datasource Provisioning

**File**: `monitoring/grafana/provisioning/datasources/prometheus.yml`

```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: false
    jsonData:
      timeInterval: "15s"
      httpMethod: POST
```

#### Dashboard Provisioning

**File**: `monitoring/grafana/provisioning/dashboards/dashboards.yml`

```yaml
apiVersion: 1

providers:
  - name: "Pipeline Overview"
    orgId: 1
    folder: "Pipeline"
    type: file
    disableDeletion: true
    editable: false
    options:
      path: /var/lib/grafana/dashboards/pipeline
      foldersFromFilesStructure: true

  - name: "Kafka"
    orgId: 1
    folder: "Kafka"
    type: file
    disableDeletion: true
    editable: false
    options:
      path: /var/lib/grafana/dashboards/kafka

  - name: "Flink"
    orgId: 1
    folder: "Flink"
    type: file
    disableDeletion: true
    editable: false
    options:
      path: /var/lib/grafana/dashboards/flink

  - name: "Data Quality"
    orgId: 1
    folder: "Data Quality"
    type: file
    disableDeletion: true
    editable: false
    options:
      path: /var/lib/grafana/dashboards/data-quality

  - name: "Infrastructure"
    orgId: 1
    folder: "Infrastructure"
    type: file
    disableDeletion: true
    editable: false
    options:
      path: /var/lib/grafana/dashboards/infrastructure

  - name: "API"
    orgId: 1
    folder: "API"
    type: file
    disableDeletion: true
    editable: false
    options:
      path: /var/lib/grafana/dashboards/api
```

#### Dashboard Folder Organization

```
monitoring/grafana/dashboards/
  pipeline/
    01-pipeline-overview.json
  kafka/
    02-kafka-deep-dive.json
  flink/
    03-flink-jobs.json
  data-quality/
    04-data-quality.json
  infrastructure/
    05-infrastructure.json
  api/
    06-api-performance.json
```

#### User & Role Setup

**File**: `monitoring/grafana/grafana.ini` (relevant sections)

```ini
[security]
admin_user = admin
admin_password = ${GF_SECURITY_ADMIN_PASSWORD}
disable_gravatar = true

[users]
allow_sign_up = false
auto_assign_org = true
auto_assign_org_role = Viewer

[auth.anonymous]
enabled = false
```

Roles mapping:

| Role    | Access                                                   |
|---------|----------------------------------------------------------|
| Admin   | Full dashboard CRUD, datasource management, user admin   |
| Editor  | Create/edit dashboards, cannot modify datasources        |
| Viewer  | Read-only access to all dashboards and alerts            |

### 2.3 Alertmanager Configuration

**File**: `monitoring/alertmanager/alertmanager.yml`

```yaml
global:
  resolve_timeout: 5m
  slack_api_url: "${SLACK_WEBHOOK_URL}"
  smtp_smarthost: "smtp.example.com:587"
  smtp_from: "alertmanager@trade-pipeline.local"
  smtp_auth_username: "${SMTP_USERNAME}"
  smtp_auth_password: "${SMTP_PASSWORD}"
  smtp_require_tls: true

templates:
  - "/etc/alertmanager/templates/*.tmpl"

route:
  receiver: "slack-warnings"
  group_by: ["alertname", "severity", "service"]
  group_wait: 30s
  group_interval: 5m
  repeat_interval: 4h

  routes:
    # -----------------------------------------------------------
    # Critical alerts -> Slack #critical + email on-call
    # -----------------------------------------------------------
    - receiver: "critical-alerts"
      match:
        severity: critical
      group_wait: 10s
      repeat_interval: 1h
      continue: false

    # -----------------------------------------------------------
    # Warning alerts -> Slack #warnings
    # -----------------------------------------------------------
    - receiver: "slack-warnings"
      match:
        severity: warning
      group_wait: 30s
      repeat_interval: 4h
      continue: false

    # -----------------------------------------------------------
    # Info alerts -> no notification (dashboard only)
    # -----------------------------------------------------------
    - receiver: "null"
      match:
        severity: info

receivers:
  - name: "critical-alerts"
    slack_configs:
      - channel: "#pipeline-critical"
        send_resolved: true
        title: '{{ template "slack.title" . }}'
        text: '{{ template "slack.text" . }}'
        actions:
          - type: button
            text: "View in Grafana"
            url: "http://grafana:3000/alerting/list"
    email_configs:
      - to: "oncall@trade-pipeline.local"
        send_resolved: true

  - name: "slack-warnings"
    slack_configs:
      - channel: "#pipeline-warnings"
        send_resolved: true
        title: '{{ template "slack.title" . }}'
        text: '{{ template "slack.text" . }}'

  - name: "null"

# -----------------------------------------------------------
# Inhibition rules: suppress warnings when critical fires
# -----------------------------------------------------------
inhibit_rules:
  - source_match:
      severity: "critical"
    target_match:
      severity: "warning"
    equal: ["alertname", "service"]

  # Suppress downstream alerts when Kafka broker is down
  - source_match:
      alertname: "KafkaBrokerDown"
    target_match_re:
      alertname: "ConsumerLag.*|FlinkJob.*|FreshnessViolation.*"
    equal: ["cluster"]
```

**Silence Rules** (applied via API or UI for planned maintenance):

```bash
# Example: silence all alerts for Kafka during planned maintenance window
amtool silence add \
  --alertmanager.url=http://alertmanager:9093 \
  --author="ops-team" \
  --comment="Planned Kafka maintenance window" \
  --duration=2h \
  service="kafka"
```

---

## 3. Metrics Collection by Component

### 3.1 Kafka Metrics (via JMX Exporter)

**File**: `monitoring/kafka/jmx-exporter-config.yml`

```yaml
startDelaySeconds: 0
ssl: false
lowercaseOutputName: true
lowercaseOutputLabelNames: true

rules:
  # -----------------------------------------------------------
  # Broker-level metrics
  # -----------------------------------------------------------
  - pattern: "kafka.server<type=ReplicaManager, name=UnderReplicatedPartitions><>Value"
    name: "kafka_server_replica_manager_under_replicated_partitions"
    type: GAUGE
    help: "Number of under-replicated partitions"

  - pattern: "kafka.server<type=ReplicaManager, name=IsrShrinksPerSec><>Count"
    name: "kafka_server_replica_manager_isr_shrinks_total"
    type: COUNTER
    help: "Total ISR shrink events"

  - pattern: "kafka.server<type=ReplicaManager, name=IsrExpandsPerSec><>Count"
    name: "kafka_server_replica_manager_isr_expands_total"
    type: COUNTER
    help: "Total ISR expand events"

  - pattern: "kafka.controller<type=KafkaController, name=ActiveControllerCount><>Value"
    name: "kafka_controller_active_controller_count"
    type: GAUGE
    help: "Number of active controllers (should be 1)"

  - pattern: "kafka.controller<type=KafkaController, name=OfflinePartitionsCount><>Value"
    name: "kafka_controller_offline_partitions_count"
    type: GAUGE
    help: "Number of offline partitions"

  # -----------------------------------------------------------
  # Topic-level metrics
  # -----------------------------------------------------------
  - pattern: "kafka.server<type=BrokerTopicMetrics, name=MessagesInPerSec, topic=(.+)><>Count"
    name: "kafka_server_broker_topic_metrics_messages_in_total"
    type: COUNTER
    labels:
      topic: "$1"
    help: "Total messages received per topic"

  - pattern: "kafka.server<type=BrokerTopicMetrics, name=BytesInPerSec, topic=(.+)><>Count"
    name: "kafka_server_broker_topic_metrics_bytes_in_total"
    type: COUNTER
    labels:
      topic: "$1"
    help: "Total bytes received per topic"

  - pattern: "kafka.server<type=BrokerTopicMetrics, name=BytesOutPerSec, topic=(.+)><>Count"
    name: "kafka_server_broker_topic_metrics_bytes_out_total"
    type: COUNTER
    labels:
      topic: "$1"
    help: "Total bytes sent per topic"

  # -----------------------------------------------------------
  # Consumer group lag (via kafka_consumer_group_exporter or burrow)
  # -----------------------------------------------------------
  - pattern: "kafka.consumer<type=consumer-fetch-manager-metrics, client-id=(.+), topic=(.+), partition=(.+)><>records-lag"
    name: "kafka_consumer_records_lag"
    type: GAUGE
    labels:
      client_id: "$1"
      topic: "$2"
      partition: "$3"
    help: "Consumer records lag per partition"

  # -----------------------------------------------------------
  # Producer metrics
  # -----------------------------------------------------------
  - pattern: "kafka.producer<type=producer-metrics, client-id=(.+)><>request-rate"
    name: "kafka_producer_request_rate"
    type: GAUGE
    labels:
      client_id: "$1"
    help: "Producer request rate"

  - pattern: "kafka.producer<type=producer-metrics, client-id=(.+)><>response-rate"
    name: "kafka_producer_response_rate"
    type: GAUGE
    labels:
      client_id: "$1"

  - pattern: "kafka.producer<type=producer-metrics, client-id=(.+)><>record-error-rate"
    name: "kafka_producer_record_error_rate"
    type: GAUGE
    labels:
      client_id: "$1"
    help: "Producer record error rate"

  - pattern: "kafka.producer<type=producer-metrics, client-id=(.+)><>batch-size-avg"
    name: "kafka_producer_batch_size_avg"
    type: GAUGE
    labels:
      client_id: "$1"
    help: "Average batch size in bytes"
```

**Kafka broker Dockerfile snippet** (JMX exporter agent):

```dockerfile
ENV KAFKA_OPTS="-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent.jar=7071:/opt/jmx-exporter/config.yml"
```

**Key Kafka Metrics Summary**:

| Metric                                        | Type    | Labels              | Alert Threshold              |
|-----------------------------------------------|---------|---------------------|------------------------------|
| `kafka_server_replica_manager_under_replicated_partitions` | Gauge   | broker              | > 0 for 5 min => CRITICAL    |
| `kafka_server_broker_topic_metrics_messages_in_total`      | Counter | topic               | rate() = 0 for 5 min => WARN |
| `kafka_consumer_records_lag`                  | Gauge   | client_id, topic, partition | > 10000 => WARN         |
| `kafka_producer_record_error_rate`            | Gauge   | client_id           | > 0.01 => WARN              |
| `kafka_controller_offline_partitions_count`   | Gauge   | -                   | > 0 => CRITICAL              |

### 3.2 Flink Metrics (via Prometheus Reporter)

**File**: `services/flink/conf/flink-conf.yaml` (metrics section)

```yaml
metrics.reporter.prometheus.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
metrics.reporter.prometheus.port: 9249
metrics.scope.jm: jobmanager
metrics.scope.jm.job: jobmanager.<job_name>
metrics.scope.tm: taskmanager.<tm_id>
metrics.scope.tm.job: taskmanager.<tm_id>.<job_name>
metrics.scope.tm.task: taskmanager.<tm_id>.<job_name>.<task_name>.<subtask_index>
metrics.scope.tm.operator: taskmanager.<tm_id>.<job_name>.<operator_name>.<subtask_index>
```

**Key Flink Metrics**:

| Metric                                              | Type    | Description                           |
|-----------------------------------------------------|---------|---------------------------------------|
| `flink_jobmanager_job_uptime`                       | Gauge   | Job uptime in milliseconds            |
| `flink_jobmanager_job_numRestarts`                  | Gauge   | Total restart count for a job         |
| `flink_taskmanager_job_task_numRecordsInPerSecond`  | Gauge   | Records ingested per second per task  |
| `flink_taskmanager_job_task_numRecordsOutPerSecond` | Gauge   | Records emitted per second per task   |
| `flink_taskmanager_job_task_isBackPressured`        | Gauge   | 1 if backpressured, 0 otherwise       |
| `flink_jobmanager_job_lastCheckpointDuration`       | Gauge   | Last checkpoint duration (ms)         |
| `flink_jobmanager_job_lastCheckpointSize`           | Gauge   | Last checkpoint size (bytes)          |
| `flink_jobmanager_job_numberOfFailedCheckpoints`    | Gauge   | Total failed checkpoints              |
| `flink_taskmanager_job_task_currentInputWatermark`  | Gauge   | Current watermark (epoch ms)          |

**Watermark lag** (computed via recording rule):

```yaml
# monitoring/prometheus/rules/flink_rules.yml
groups:
  - name: flink_derived
    interval: 15s
    rules:
      - record: flink:watermark_lag_seconds
        expr: |
          (time() * 1000 - flink_taskmanager_job_task_currentInputWatermark) / 1000
        labels:
          component: "flink"
```

### 3.3 Dagster Metrics (Custom Prometheus Pushgateway)

Since Dagster runs batch-style operations (sensors, schedules, asset materializations) rather than a long-lived HTTP server, metrics are pushed to the Prometheus Pushgateway after each run.

**File**: `services/dagster/metrics/dagster_metrics.py`

```python
"""
Dagster Prometheus metrics helper.
Pushes metrics to Pushgateway after each run / sensor tick / schedule tick.
"""

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    push_to_gateway,
)

PUSHGATEWAY_URL = "pushgateway:9091"

registry = CollectorRegistry()

# ---------------------------------------------------------------
# Run metrics
# ---------------------------------------------------------------
dagster_run_success_total = Counter(
    "dagster_run_success_total",
    "Total successful Dagster runs",
    ["job_name"],
    registry=registry,
)

dagster_run_failure_total = Counter(
    "dagster_run_failure_total",
    "Total failed Dagster runs",
    ["job_name"],
    registry=registry,
)

dagster_run_duration_seconds = Histogram(
    "dagster_run_duration_seconds",
    "Duration of Dagster runs in seconds",
    ["job_name"],
    buckets=[5, 15, 30, 60, 120, 300, 600, 1800],
    registry=registry,
)

# ---------------------------------------------------------------
# Asset materialization metrics
# ---------------------------------------------------------------
dagster_asset_materialization_total = Counter(
    "dagster_asset_materialization_total",
    "Total asset materializations",
    ["asset_key", "layer"],
    registry=registry,
)

dagster_asset_materialization_duration_seconds = Histogram(
    "dagster_asset_materialization_duration_seconds",
    "Duration of asset materialization in seconds",
    ["asset_key", "layer"],
    buckets=[1, 5, 15, 30, 60, 120, 300],
    registry=registry,
)

dagster_asset_freshness_seconds = Gauge(
    "dagster_asset_freshness_seconds",
    "Seconds since last successful materialization",
    ["asset_key", "layer"],
    registry=registry,
)

# ---------------------------------------------------------------
# Sensor metrics
# ---------------------------------------------------------------
dagster_sensor_tick_total = Counter(
    "dagster_sensor_tick_total",
    "Total sensor ticks",
    ["sensor_name"],
    registry=registry,
)

dagster_sensor_evaluation_total = Counter(
    "dagster_sensor_evaluation_total",
    "Total sensor evaluations that yielded run requests",
    ["sensor_name"],
    registry=registry,
)

dagster_sensor_skip_total = Counter(
    "dagster_sensor_skip_total",
    "Total sensor ticks that were skipped",
    ["sensor_name"],
    registry=registry,
)

# ---------------------------------------------------------------
# Schedule metrics
# ---------------------------------------------------------------
dagster_schedule_tick_total = Counter(
    "dagster_schedule_tick_total",
    "Total schedule ticks",
    ["schedule_name"],
    registry=registry,
)

dagster_schedule_late_total = Counter(
    "dagster_schedule_late_total",
    "Total schedule ticks that started late (> 60s past scheduled time)",
    ["schedule_name"],
    registry=registry,
)


def push_metrics(job_label: str = "dagster"):
    """Push all collected metrics to the Pushgateway."""
    push_to_gateway(
        PUSHGATEWAY_URL,
        job=job_label,
        registry=registry,
    )
```

**Usage in a Dagster hook** (`services/dagster/hooks/metrics_hook.py`):

```python
from dagster import HookContext, success_hook, failure_hook
import time
from metrics.dagster_metrics import (
    dagster_run_success_total,
    dagster_run_failure_total,
    dagster_run_duration_seconds,
    push_metrics,
)


@success_hook
def on_run_success(context: HookContext):
    job_name = context.op.name
    duration = time.time() - context.run.start_time.timestamp()
    dagster_run_success_total.labels(job_name=job_name).inc()
    dagster_run_duration_seconds.labels(job_name=job_name).observe(duration)
    push_metrics(job_label=f"dagster_{job_name}")


@failure_hook
def on_run_failure(context: HookContext):
    job_name = context.op.name
    dagster_run_failure_total.labels(job_name=job_name).inc()
    push_metrics(job_label=f"dagster_{job_name}")
```

### 3.4 Application Metrics (Custom)

Each custom service exposes a `/metrics` endpoint using the `prometheus_client` Python library.

#### Trade Generator Metrics

**File**: `services/trade-generator/metrics.py`

```python
from prometheus_client import Counter, Gauge, Histogram

trades_generated_total = Counter(
    "trade_generator_trades_total",
    "Total trades generated",
    ["symbol", "trade_type"],
)

trades_per_second = Gauge(
    "trade_generator_trades_per_second",
    "Current rate of trades generated per second",
)

websocket_connections_active = Gauge(
    "trade_generator_websocket_connections_active",
    "Number of active WebSocket connections",
)

trade_generation_latency_seconds = Histogram(
    "trade_generator_latency_seconds",
    "Time to generate and publish a single trade",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1],
)
```

#### Kafka Consumer / Bridge Metrics

**File**: `services/kafka-consumer/metrics.py`

```python
from prometheus_client import Counter, Gauge, Histogram

messages_bridged_total = Counter(
    "kafka_bridge_messages_total",
    "Total messages bridged from WebSocket to Kafka",
    ["topic"],
)

bridge_errors_total = Counter(
    "kafka_bridge_errors_total",
    "Total errors during message bridging",
    ["error_type"],
)

bridge_latency_seconds = Histogram(
    "kafka_bridge_latency_seconds",
    "End-to-end latency from WS receive to Kafka produce ack",
    ["topic"],
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

messages_per_second = Gauge(
    "kafka_bridge_messages_per_second",
    "Current throughput of messages bridged per second",
)
```

#### GraphQL API Metrics

**File**: `services/graphql-api/metrics.py`

```python
from prometheus_client import Counter, Histogram, Gauge

graphql_requests_total = Counter(
    "graphql_requests_total",
    "Total GraphQL requests",
    ["operation_type", "operation_name", "status"],
)

graphql_request_duration_seconds = Histogram(
    "graphql_request_duration_seconds",
    "GraphQL request duration in seconds",
    ["operation_type", "operation_name"],
    buckets=[0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
)

graphql_errors_total = Counter(
    "graphql_errors_total",
    "Total GraphQL errors",
    ["error_type"],
)

graphql_active_requests = Gauge(
    "graphql_active_requests",
    "Number of in-flight GraphQL requests",
)

graphql_cache_hits_total = Counter(
    "graphql_cache_hits_total",
    "Total cache hits for query results",
)

graphql_cache_misses_total = Counter(
    "graphql_cache_misses_total",
    "Total cache misses for query results",
)
```

### 3.5 Infrastructure Metrics (Node Exporter, cAdvisor)

**Docker Compose snippet** (`docker-compose.monitoring.yml`):

```yaml
services:
  node-exporter:
    image: prom/node-exporter:v1.8.1
    container_name: node-exporter
    restart: unless-stopped
    volumes:
      - /proc:/host/proc:ro
      - /sys:/host/sys:ro
      - /:/rootfs:ro
    command:
      - "--path.procfs=/host/proc"
      - "--path.sysfs=/host/sys"
      - "--path.rootfs=/rootfs"
      - "--collector.filesystem.mount-points-exclude=^/(sys|proc|dev|host|etc)($$|/)"
    ports:
      - "9100:9100"
    networks:
      - monitoring

  cadvisor:
    image: gcr.io/cadvisor/cadvisor:v0.49.1
    container_name: cadvisor
    restart: unless-stopped
    privileged: true
    volumes:
      - /:/rootfs:ro
      - /var/run:/var/run:ro
      - /sys:/sys:ro
      - /var/lib/docker/:/var/lib/docker:ro
      - /dev/disk/:/dev/disk:ro
    ports:
      - "8080:8080"
    networks:
      - monitoring
```

**Key Infrastructure Metrics**:

| Metric                                    | Source        | Description                         |
|-------------------------------------------|---------------|-------------------------------------|
| `node_cpu_seconds_total`                  | Node Exporter | CPU time by mode                    |
| `node_memory_MemAvailable_bytes`          | Node Exporter | Available memory                    |
| `node_filesystem_avail_bytes`             | Node Exporter | Available disk space                |
| `node_network_receive_bytes_total`        | Node Exporter | Network bytes received              |
| `node_network_transmit_bytes_total`       | Node Exporter | Network bytes transmitted           |
| `container_cpu_usage_seconds_total`       | cAdvisor      | CPU usage per container             |
| `container_memory_usage_bytes`            | cAdvisor      | Memory usage per container          |
| `container_network_receive_bytes_total`   | cAdvisor      | Network rx per container            |
| `container_network_transmit_bytes_total`  | cAdvisor      | Network tx per container            |
| `container_last_seen`                     | cAdvisor      | Container health (last heartbeat)   |

---

## 4. Data Observability Metrics (Custom)

These are the five core data observability pillars implemented as custom Prometheus metrics, computed by Dagster sensors and post-materialization hooks.

### 4.1 Freshness Monitoring

**Metric**: `data_freshness_seconds{table, layer}`

**Implementation** (`services/dagster/sensors/freshness_sensor.py`):

```python
"""
Dagster sensor that checks the freshness of each table in each layer.
Computes the elapsed time since the last partition was written to Iceberg.
Pushes the metric to Prometheus Pushgateway.
"""

from dagster import sensor, SensorEvaluationContext, RunRequest
from pyiceberg.catalog import load_catalog
from datetime import datetime, timezone
from prometheus_client import Gauge, CollectorRegistry, push_to_gateway

PUSHGATEWAY_URL = "pushgateway:9091"

registry = CollectorRegistry()
data_freshness_seconds = Gauge(
    "data_freshness_seconds",
    "Seconds since the last data update for a given table and layer",
    ["table", "layer"],
    registry=registry,
)

TABLES = {
    "bronze": ["raw_trades", "raw_orders", "raw_market_data"],
    "silver": ["cleaned_trades", "enriched_orders", "aggregated_market_data"],
    "gold": ["trade_summary_daily", "portfolio_performance", "market_indicators"],
}


@sensor(minimum_interval_seconds=60)
def freshness_sensor(context: SensorEvaluationContext):
    catalog = load_catalog("default")

    for layer, tables in TABLES.items():
        for table_name in tables:
            try:
                table = catalog.load_table(f"{layer}.{table_name}")
                snapshots = table.metadata.snapshots
                if snapshots:
                    latest_snapshot = max(snapshots, key=lambda s: s.timestamp_ms)
                    last_update = datetime.fromtimestamp(
                        latest_snapshot.timestamp_ms / 1000, tz=timezone.utc
                    )
                    freshness = (datetime.now(timezone.utc) - last_update).total_seconds()
                else:
                    freshness = float("inf")

                data_freshness_seconds.labels(
                    table=table_name, layer=layer
                ).set(freshness)

            except Exception as e:
                context.log.warning(
                    f"Failed to check freshness for {layer}.{table_name}: {e}"
                )
                data_freshness_seconds.labels(
                    table=table_name, layer=layer
                ).set(float("inf"))

    push_to_gateway(PUSHGATEWAY_URL, job="freshness_sensor", registry=registry)
    context.log.info("Freshness metrics pushed to Pushgateway")
```

**Alert Rules** (`monitoring/prometheus/rules/freshness_rules.yml`):

```yaml
groups:
  - name: data_freshness
    interval: 30s
    rules:
      - alert: BronzeFreshnessViolation
        expr: data_freshness_seconds{layer="bronze"} > 120
        for: 2m
        labels:
          severity: warning
          service: data-quality
        annotations:
          summary: "Bronze layer freshness SLA violation"
          description: >
            Table {{ $labels.table }} in bronze layer has not received
            data for {{ $value | humanizeDuration }}.
            Threshold: 2 minutes.

      - alert: SilverFreshnessViolation
        expr: data_freshness_seconds{layer="silver"} > 600
        for: 5m
        labels:
          severity: warning
          service: data-quality
        annotations:
          summary: "Silver layer freshness SLA violation"
          description: >
            Table {{ $labels.table }} in silver layer has not been
            updated for {{ $value | humanizeDuration }}.
            Threshold: 10 minutes.

      - alert: GoldFreshnessViolation
        expr: data_freshness_seconds{layer="gold"} > 7200
        for: 15m
        labels:
          severity: warning
          service: data-quality
        annotations:
          summary: "Gold layer freshness SLA violation"
          description: >
            Table {{ $labels.table }} in gold layer has not been
            updated for {{ $value | humanizeDuration }}.
            Threshold: 2 hours.

      - alert: FreshnessCriticalFailure
        expr: data_freshness_seconds{layer="bronze"} > 900
        for: 5m
        labels:
          severity: critical
          service: data-quality
        annotations:
          summary: "CRITICAL: Bronze data stale for > 15 minutes"
          description: >
            No data flowing into {{ $labels.table }} for 15+ minutes.
            Pipeline may be completely stalled.
```

**Dashboard Panel**: Freshness heatmap by table -- a Grafana heatmap panel where X axis = time, Y axis = table name, color intensity = freshness seconds.

### 4.2 Volume Monitoring

**Metric**: `data_row_count{table, layer, partition_date}`

**Implementation** (`services/dagster/sensors/volume_sensor.py`):

```python
"""
Post-materialization hook that records row counts per table/layer/partition.
Compares against 7-day rolling average to detect anomalies.
"""

from dagster import asset_sensor, AssetKey, SensorEvaluationContext
from pyiceberg.catalog import load_catalog
from prometheus_client import Gauge, Counter, CollectorRegistry, push_to_gateway
import statistics

PUSHGATEWAY_URL = "pushgateway:9091"

registry = CollectorRegistry()

data_row_count = Gauge(
    "data_row_count",
    "Row count for the latest partition of a table",
    ["table", "layer", "partition_date"],
    registry=registry,
)

data_volume_deviation_percent = Gauge(
    "data_volume_deviation_percent",
    "Percentage deviation of current row count from 7-day average",
    ["table", "layer"],
    registry=registry,
)

data_volume_anomaly_total = Counter(
    "data_volume_anomaly_total",
    "Total volume anomalies detected (>20% deviation)",
    ["table", "layer"],
    registry=registry,
)


def get_recent_row_counts(catalog, layer: str, table_name: str, days: int = 7):
    """
    Query Iceberg metadata to get row counts for last N days of partitions.
    Returns a list of row counts.
    """
    table = catalog.load_table(f"{layer}.{table_name}")
    # Use Iceberg snapshot summary to get record counts
    snapshots = sorted(table.metadata.snapshots, key=lambda s: s.timestamp_ms)
    recent = snapshots[-days:] if len(snapshots) >= days else snapshots
    counts = []
    for snap in recent:
        summary = snap.summary or {}
        count = int(summary.get("total-records", 0))
        counts.append(count)
    return counts


@asset_sensor(asset_key=AssetKey("any_table"), minimum_interval_seconds=60)
def volume_sensor(context: SensorEvaluationContext):
    catalog = load_catalog("default")

    tables = {
        "bronze": ["raw_trades", "raw_orders"],
        "silver": ["cleaned_trades", "enriched_orders"],
        "gold": ["trade_summary_daily"],
    }

    for layer, table_list in tables.items():
        for table_name in table_list:
            try:
                recent_counts = get_recent_row_counts(
                    catalog, layer, table_name, days=7
                )
                if not recent_counts:
                    continue

                current_count = recent_counts[-1]
                avg_count = statistics.mean(recent_counts) if recent_counts else 0

                data_row_count.labels(
                    table=table_name,
                    layer=layer,
                    partition_date="latest",
                ).set(current_count)

                if avg_count > 0:
                    deviation = ((current_count - avg_count) / avg_count) * 100
                    data_volume_deviation_percent.labels(
                        table=table_name, layer=layer
                    ).set(deviation)

                    if abs(deviation) > 20:
                        data_volume_anomaly_total.labels(
                            table=table_name, layer=layer
                        ).inc()
                        context.log.warning(
                            f"Volume anomaly for {layer}.{table_name}: "
                            f"{deviation:.1f}% deviation from 7-day avg"
                        )

            except Exception as e:
                context.log.warning(
                    f"Volume check failed for {layer}.{table_name}: {e}"
                )

    push_to_gateway(PUSHGATEWAY_URL, job="volume_sensor", registry=registry)
```

**Alert Rules** (`monitoring/prometheus/rules/volume_rules.yml`):

```yaml
groups:
  - name: data_volume
    interval: 60s
    rules:
      - alert: VolumeAnomalyDetected
        expr: abs(data_volume_deviation_percent) > 20
        for: 5m
        labels:
          severity: warning
          service: data-quality
        annotations:
          summary: "Volume anomaly detected for {{ $labels.table }}"
          description: >
            {{ $labels.layer }}.{{ $labels.table }} row count deviates
            {{ $value | printf "%.1f" }}% from 7-day average.

      - alert: ZeroRowCount
        expr: data_row_count == 0
        for: 10m
        labels:
          severity: critical
          service: data-quality
        annotations:
          summary: "Zero rows in {{ $labels.table }}"
          description: >
            {{ $labels.layer }}.{{ $labels.table }} has zero rows in
            the latest partition. Data pipeline may be broken.
```

**Dashboard Panel**: Volume trend chart per table with a shaded 7-day-average band and anomaly markers.

### 4.3 Schema Monitoring

**Metric**: `schema_changes_total{table, change_type}`

**Implementation** (`services/dagster/sensors/schema_sensor.py`):

```python
"""
Compares Iceberg table schemas across snapshots to detect drift.
Emits metrics for column additions, removals, and type changes.
"""

from dagster import sensor, SensorEvaluationContext
from pyiceberg.catalog import load_catalog
from prometheus_client import Counter, Gauge, CollectorRegistry, push_to_gateway

PUSHGATEWAY_URL = "pushgateway:9091"

registry = CollectorRegistry()

schema_changes_total = Counter(
    "schema_changes_total",
    "Total schema changes detected",
    ["table", "change_type"],
    registry=registry,
)

schema_column_count = Gauge(
    "schema_column_count",
    "Current number of columns in the table",
    ["table", "layer"],
    registry=registry,
)

# In-memory store for previous schema (reset on restart)
_previous_schemas: dict[str, set[str]] = {}


def detect_schema_changes(
    table_name: str, current_columns: dict[str, str], context
) -> list[dict]:
    """
    Compare current schema against previously seen schema.
    Returns list of change dicts: {type: 'add'|'drop'|'type_change', column: str}
    """
    changes = []
    prev = _previous_schemas.get(table_name)

    if prev is not None:
        current_names = set(current_columns.keys())
        prev_names = set(prev.keys())

        for col in current_names - prev_names:
            changes.append({"type": "column_added", "column": col})
            schema_changes_total.labels(
                table=table_name, change_type="column_added"
            ).inc()
            context.log.info(f"Schema change: column '{col}' added to {table_name}")

        for col in prev_names - current_names:
            changes.append({"type": "column_dropped", "column": col})
            schema_changes_total.labels(
                table=table_name, change_type="column_dropped"
            ).inc()
            context.log.warning(
                f"Schema change: column '{col}' DROPPED from {table_name}"
            )

        for col in current_names & prev_names:
            if current_columns[col] != prev[col]:
                changes.append({"type": "type_changed", "column": col})
                schema_changes_total.labels(
                    table=table_name, change_type="type_changed"
                ).inc()
                context.log.warning(
                    f"Schema change: column '{col}' type changed in {table_name} "
                    f"from {prev[col]} to {current_columns[col]}"
                )

    _previous_schemas[table_name] = current_columns
    return changes


@sensor(minimum_interval_seconds=300)
def schema_sensor(context: SensorEvaluationContext):
    catalog = load_catalog("default")

    tables_to_monitor = [
        ("bronze", "raw_trades"),
        ("bronze", "raw_orders"),
        ("silver", "cleaned_trades"),
        ("silver", "enriched_orders"),
        ("gold", "trade_summary_daily"),
    ]

    for layer, table_name in tables_to_monitor:
        try:
            table = catalog.load_table(f"{layer}.{table_name}")
            current_schema = table.schema()
            columns = {
                field.name: str(field.field_type) for field in current_schema.fields
            }

            schema_column_count.labels(table=table_name, layer=layer).set(len(columns))
            detect_schema_changes(f"{layer}.{table_name}", columns, context)

        except Exception as e:
            context.log.warning(f"Schema check failed for {layer}.{table_name}: {e}")

    push_to_gateway(PUSHGATEWAY_URL, job="schema_sensor", registry=registry)
```

**Alert Rules** (`monitoring/prometheus/rules/schema_rules.yml`):

```yaml
groups:
  - name: schema_drift
    interval: 60s
    rules:
      - alert: UnexpectedColumnDrop
        expr: increase(schema_changes_total{change_type="column_dropped"}[10m]) > 0
        labels:
          severity: critical
          service: data-quality
        annotations:
          summary: "Column dropped from {{ $labels.table }}"
          description: >
            A column was unexpectedly dropped from {{ $labels.table }}.
            This may break downstream consumers. Investigate immediately.

      - alert: ColumnAdded
        expr: increase(schema_changes_total{change_type="column_added"}[10m]) > 0
        labels:
          severity: warning
          service: data-quality
        annotations:
          summary: "New column added to {{ $labels.table }}"
          description: >
            A new column was added to {{ $labels.table }}.
            Verify downstream compatibility.

      - alert: ColumnTypeChanged
        expr: increase(schema_changes_total{change_type="type_changed"}[10m]) > 0
        labels:
          severity: critical
          service: data-quality
        annotations:
          summary: "Column type changed in {{ $labels.table }}"
          description: >
            A column type was changed in {{ $labels.table }}.
            This is a breaking change that may cause downstream failures.
```

**Dashboard Panel**: Schema change timeline -- an annotations-based timeline showing when columns were added, dropped, or had type changes.

### 4.4 Distribution Monitoring

**Metric**: `data_distribution_zscore{table, column, statistic}`

**Implementation** (`services/dagster/sensors/distribution_sensor.py`):

```python
"""
Computes distribution statistics for key columns in each batch.
Detects anomalies using z-score against recent historical distributions.
"""

from dagster import sensor, SensorEvaluationContext
from prometheus_client import Gauge, Counter, CollectorRegistry, push_to_gateway
import duckdb
import statistics
from collections import defaultdict

PUSHGATEWAY_URL = "pushgateway:9091"

registry = CollectorRegistry()

data_distribution_zscore = Gauge(
    "data_distribution_zscore",
    "Z-score of distribution statistic against recent history",
    ["table", "column", "statistic"],
    registry=registry,
)

data_distribution_value = Gauge(
    "data_distribution_value",
    "Current value of distribution statistic",
    ["table", "column", "statistic"],
    registry=registry,
)

data_distribution_anomaly_total = Counter(
    "data_distribution_anomaly_total",
    "Total distribution anomalies detected (z-score > 3)",
    ["table", "column", "statistic"],
    registry=registry,
)

# Rolling window of historical stats (in-memory, last 50 batches)
_history: dict[str, list[float]] = defaultdict(list)
HISTORY_WINDOW = 50

# Columns to monitor per table
MONITORED_COLUMNS = {
    "bronze.raw_trades": {
        "price": ["mean", "stddev", "min", "max", "null_percent"],
        "quantity": ["mean", "stddev", "min", "max", "null_percent"],
    },
    "silver.cleaned_trades": {
        "price": ["mean", "stddev", "min", "max"],
        "total_value": ["mean", "stddev"],
        "quantity": ["mean", "null_percent"],
    },
    "gold.trade_summary_daily": {
        "avg_price": ["mean", "stddev"],
        "total_volume": ["mean"],
        "trade_count": ["mean"],
    },
}


def compute_zscore(current: float, history: list[float]) -> float:
    if len(history) < 5:
        return 0.0
    mean = statistics.mean(history)
    stdev = statistics.stdev(history)
    if stdev == 0:
        return 0.0
    return (current - mean) / stdev


@sensor(minimum_interval_seconds=120)
def distribution_sensor(context: SensorEvaluationContext):
    conn = duckdb.connect()

    for table_fqn, columns in MONITORED_COLUMNS.items():
        for column_name, stats in columns.items():
            try:
                # Build SQL for all stats in one query
                stat_exprs = []
                for stat in stats:
                    if stat == "mean":
                        stat_exprs.append(f"AVG({column_name}) AS stat_mean")
                    elif stat == "stddev":
                        stat_exprs.append(f"STDDEV({column_name}) AS stat_stddev")
                    elif stat == "min":
                        stat_exprs.append(f"MIN({column_name}) AS stat_min")
                    elif stat == "max":
                        stat_exprs.append(f"MAX({column_name}) AS stat_max")
                    elif stat == "null_percent":
                        stat_exprs.append(
                            f"(COUNT(*) - COUNT({column_name})) * 100.0 "
                            f"/ COUNT(*) AS stat_null_percent"
                        )

                query = f"""
                    SELECT {', '.join(stat_exprs)}
                    FROM iceberg_scan('{table_fqn}')
                """
                result = conn.execute(query).fetchone()

                for i, stat in enumerate(stats):
                    value = float(result[i]) if result[i] is not None else 0.0
                    history_key = f"{table_fqn}.{column_name}.{stat}"

                    _history[history_key].append(value)
                    if len(_history[history_key]) > HISTORY_WINDOW:
                        _history[history_key] = _history[history_key][-HISTORY_WINDOW:]

                    data_distribution_value.labels(
                        table=table_fqn, column=column_name, statistic=stat
                    ).set(value)

                    zscore = compute_zscore(value, _history[history_key][:-1])
                    data_distribution_zscore.labels(
                        table=table_fqn, column=column_name, statistic=stat
                    ).set(zscore)

                    if abs(zscore) > 3:
                        data_distribution_anomaly_total.labels(
                            table=table_fqn, column=column_name, statistic=stat
                        ).inc()
                        context.log.warning(
                            f"Distribution anomaly: {table_fqn}.{column_name} "
                            f"{stat}={value:.4f} z-score={zscore:.2f}"
                        )

            except Exception as e:
                context.log.warning(
                    f"Distribution check failed for {table_fqn}.{column_name}: {e}"
                )

    push_to_gateway(PUSHGATEWAY_URL, job="distribution_sensor", registry=registry)
    conn.close()
```

**Alert Rules** (`monitoring/prometheus/rules/distribution_rules.yml`):

```yaml
groups:
  - name: data_distribution
    interval: 60s
    rules:
      - alert: DistributionAnomaly
        expr: abs(data_distribution_zscore) > 3
        for: 5m
        labels:
          severity: warning
          service: data-quality
        annotations:
          summary: "Distribution anomaly for {{ $labels.table }}.{{ $labels.column }}"
          description: >
            {{ $labels.statistic }} for column {{ $labels.column }} in
            {{ $labels.table }} has z-score {{ $value | printf "%.2f" }}.
            This is outside the 3-sigma threshold and may indicate data issues.

      - alert: HighNullPercentage
        expr: data_distribution_value{statistic="null_percent"} > 10
        for: 5m
        labels:
          severity: warning
          service: data-quality
        annotations:
          summary: "High null percentage in {{ $labels.table }}.{{ $labels.column }}"
          description: >
            {{ $labels.column }} in {{ $labels.table }} has
            {{ $value | printf "%.1f" }}% null values. Expected < 10%.
```

**Dashboard Panel**: Distribution histograms and box plots per column, with z-score overlays.

### 4.5 Lineage Monitoring

**Metric**: `pipeline_stage_success{stage, job}`

**Implementation** (`services/dagster/sensors/lineage_sensor.py`):

```python
"""
Tracks end-to-end pipeline stage completion.
Each stage of the pipeline (ingest -> bronze -> silver -> gold -> serve)
is monitored for success/failure, with downstream impact analysis.
"""

from dagster import sensor, SensorEvaluationContext
from prometheus_client import Gauge, Counter, CollectorRegistry, push_to_gateway
from enum import Enum

PUSHGATEWAY_URL = "pushgateway:9091"

registry = CollectorRegistry()

pipeline_stage_success = Gauge(
    "pipeline_stage_success",
    "Whether a pipeline stage completed successfully (1=success, 0=failure)",
    ["stage", "job"],
    registry=registry,
)

pipeline_stage_duration_seconds = Gauge(
    "pipeline_stage_duration_seconds",
    "Duration of the last pipeline stage execution",
    ["stage", "job"],
    registry=registry,
)

pipeline_end_to_end_success = Gauge(
    "pipeline_end_to_end_success",
    "Whether the full pipeline completed end-to-end (1=success, 0=failure)",
    ["pipeline_name"],
    registry=registry,
)

pipeline_stage_failure_total = Counter(
    "pipeline_stage_failure_total",
    "Total failures per pipeline stage",
    ["stage", "job"],
    registry=registry,
)

# Pipeline stage DAG definition
PIPELINE_STAGES = {
    "trade_pipeline": [
        {"stage": "ingest", "job": "websocket_to_kafka"},
        {"stage": "stream", "job": "flink_processing"},
        {"stage": "bronze", "job": "kafka_to_iceberg_bronze"},
        {"stage": "silver", "job": "bronze_to_silver_transform"},
        {"stage": "gold", "job": "silver_to_gold_aggregate"},
        {"stage": "serve", "job": "refresh_api_cache"},
    ]
}

# Downstream dependency map (stage -> list of stages that depend on it)
DOWNSTREAM_MAP = {
    "ingest": ["stream", "bronze", "silver", "gold", "serve"],
    "stream": ["bronze", "silver", "gold", "serve"],
    "bronze": ["silver", "gold", "serve"],
    "silver": ["gold", "serve"],
    "gold": ["serve"],
    "serve": [],
}

pipeline_downstream_impact_count = Gauge(
    "pipeline_downstream_impact_count",
    "Number of downstream stages impacted by a failure at this stage",
    ["stage"],
    registry=registry,
)


@sensor(minimum_interval_seconds=60)
def lineage_sensor(context: SensorEvaluationContext):
    """
    Check the status of each pipeline stage using Dagster run history.
    In a real implementation this would query the Dagster GraphQL API
    or the run storage directly.
    """
    from dagster._core.instance import DagsterInstance

    instance = DagsterInstance.get()

    for pipeline_name, stages in PIPELINE_STAGES.items():
        all_success = True

        for stage_def in stages:
            stage = stage_def["stage"]
            job = stage_def["job"]

            # Query last run for this job
            runs = instance.get_runs(
                filters={"job_name": job},
                limit=1,
            )

            if runs:
                last_run = runs[0]
                is_success = last_run.status.value == "SUCCESS"
                pipeline_stage_success.labels(stage=stage, job=job).set(
                    1 if is_success else 0
                )

                if hasattr(last_run, "start_time") and hasattr(last_run, "end_time"):
                    if last_run.start_time and last_run.end_time:
                        duration = (
                            last_run.end_time - last_run.start_time
                        ).total_seconds()
                        pipeline_stage_duration_seconds.labels(
                            stage=stage, job=job
                        ).set(duration)

                if not is_success:
                    all_success = False
                    pipeline_stage_failure_total.labels(stage=stage, job=job).inc()
                    downstream_count = len(DOWNSTREAM_MAP.get(stage, []))
                    pipeline_downstream_impact_count.labels(stage=stage).set(
                        downstream_count
                    )
                    context.log.warning(
                        f"Pipeline stage {stage} ({job}) FAILED. "
                        f"Impacts {downstream_count} downstream stages."
                    )
            else:
                pipeline_stage_success.labels(stage=stage, job=job).set(0)
                all_success = False

        pipeline_end_to_end_success.labels(pipeline_name=pipeline_name).set(
            1 if all_success else 0
        )

    push_to_gateway(PUSHGATEWAY_URL, job="lineage_sensor", registry=registry)
```

**Alert Rules** (`monitoring/prometheus/rules/lineage_rules.yml`):

```yaml
groups:
  - name: pipeline_lineage
    interval: 30s
    rules:
      - alert: PipelineStageFailure
        expr: pipeline_stage_success == 0
        for: 2m
        labels:
          severity: critical
          service: pipeline
        annotations:
          summary: "Pipeline stage {{ $labels.stage }} failed"
          description: >
            Stage {{ $labels.stage }} (job: {{ $labels.job }}) has failed.
            Downstream stages may be impacted.
            Check Dagster UI for details.

      - alert: PipelineEndToEndFailure
        expr: pipeline_end_to_end_success == 0
        for: 5m
        labels:
          severity: critical
          service: pipeline
        annotations:
          summary: "End-to-end pipeline failure: {{ $labels.pipeline_name }}"
          description: >
            The full {{ $labels.pipeline_name }} pipeline has not completed
            successfully. One or more stages have failed.

      - alert: HighDownstreamImpact
        expr: pipeline_downstream_impact_count > 2
        for: 1m
        labels:
          severity: critical
          service: pipeline
        annotations:
          summary: "High downstream impact from {{ $labels.stage }} failure"
          description: >
            Failure at stage {{ $labels.stage }} is impacting
            {{ $value }} downstream stages.
```

**Dashboard Panel**: Pipeline DAG visualization with color-coded status overlay (green = success, red = failure, gray = no data).

---

## 5. Grafana Dashboards (as code, JSON provisioning)

All dashboards are provisioned as JSON files under `monitoring/grafana/dashboards/`. Below are the detailed panel specifications for each dashboard.

### Dashboard 1: Pipeline Overview

**File**: `monitoring/grafana/dashboards/pipeline/01-pipeline-overview.json`

**Panels**:

| Panel                          | Type       | PromQL / Data Source                                                    |
|--------------------------------|------------|-------------------------------------------------------------------------|
| End-to-End Pipeline Health     | Stat       | `pipeline_end_to_end_success{pipeline_name="trade_pipeline"}`           |
| Data Flow Diagram              | Node Graph | Custom plugin: stage nodes with `rate(messages_in[5m])` as edge labels  |
| Active Alerts Summary          | Alert List | Grafana built-in alert list panel, filtered by `service=~".*"`          |
| SLA Compliance Scorecard       | Stat (multi)| `avg_over_time(data_freshness_seconds{layer="bronze"}[1h]) < 120`      |
| Stage Status (per stage)       | Stat row   | `pipeline_stage_success{stage=~"ingest|stream|bronze|silver|gold|serve"}`|
| Throughput (end-to-end)        | Time series| `rate(kafka_server_broker_topic_metrics_messages_in_total[5m])`          |
| Recent Failures                | Table      | Last 10 failures from `pipeline_stage_failure_total`                    |

**Refresh**: 15 seconds auto-refresh.

**Variables**: `$pipeline` (dropdown selector), `$timerange`.

### Dashboard 2: Kafka Deep Dive

**File**: `monitoring/grafana/dashboards/kafka/02-kafka-deep-dive.json`

**Panels**:

| Panel                          | Type        | PromQL                                                                                    |
|--------------------------------|-------------|-------------------------------------------------------------------------------------------|
| Messages In/Out per Topic      | Time series | `rate(kafka_server_broker_topic_metrics_messages_in_total{topic=~"$topic"}[5m])`          |
| Bytes In/Out per Topic         | Time series | `rate(kafka_server_broker_topic_metrics_bytes_in_total{topic=~"$topic"}[5m])`             |
| Consumer Group Lag             | Time series | `kafka_consumer_records_lag{topic=~"$topic"}`                                             |
| Consumer Lag by Partition      | Heatmap     | `kafka_consumer_records_lag{topic=~"$topic"}` grouped by partition                        |
| Under-Replicated Partitions    | Stat        | `kafka_server_replica_manager_under_replicated_partitions`                                 |
| ISR Shrink/Expand Rate         | Time series | `rate(kafka_server_replica_manager_isr_shrinks_total[5m])`                                |
| Active Controller Count        | Stat        | `kafka_controller_active_controller_count`                                                |
| Offline Partitions             | Stat        | `kafka_controller_offline_partitions_count`                                               |
| Producer Request Rate          | Time series | `kafka_producer_request_rate{client_id=~"$client_id"}`                                   |
| Producer Error Rate            | Time series | `kafka_producer_record_error_rate{client_id=~"$client_id"}`                               |
| Broker Disk Usage              | Gauge       | `node_filesystem_avail_bytes{mountpoint="/kafka-data"}`                                   |

**Variables**: `$topic` (multi-select, query: `label_values(kafka_server_broker_topic_metrics_messages_in_total, topic)`), `$client_id`.

### Dashboard 3: Flink Jobs

**File**: `monitoring/grafana/dashboards/flink/03-flink-jobs.json`

**Panels**:

| Panel                          | Type        | PromQL                                                                               |
|--------------------------------|-------------|--------------------------------------------------------------------------------------|
| Job Status Overview            | Stat (multi)| `flink_jobmanager_job_uptime` (color: green if > 0)                                  |
| Job Uptime                     | Time series | `flink_jobmanager_job_uptime / 1000 / 3600` (hours)                                 |
| Restart Count                  | Stat        | `flink_jobmanager_job_numRestarts`                                                    |
| Records In per Second          | Time series | `flink_taskmanager_job_task_numRecordsInPerSecond`                                    |
| Records Out per Second         | Time series | `flink_taskmanager_job_task_numRecordsOutPerSecond`                                   |
| Backpressure Indicators        | Stat row    | `flink_taskmanager_job_task_isBackPressured` (red if == 1)                            |
| Checkpoint Duration            | Time series | `flink_jobmanager_job_lastCheckpointDuration`                                         |
| Checkpoint Size                | Time series | `flink_jobmanager_job_lastCheckpointSize`                                             |
| Failed Checkpoints             | Stat        | `flink_jobmanager_job_numberOfFailedCheckpoints`                                      |
| Watermark Lag                  | Time series | `flink:watermark_lag_seconds` (from recording rule)                                   |
| TaskManager CPU Usage          | Time series | Container CPU for flink-taskmanager containers                                        |
| TaskManager Memory             | Time series | Container memory for flink-taskmanager containers                                     |

**Variables**: `$job_name`, `$task_name`.

### Dashboard 4: Data Quality

**File**: `monitoring/grafana/dashboards/data-quality/04-data-quality.json`

**Panels**:

| Panel                          | Type        | PromQL / Data Source                                                                  |
|--------------------------------|-------------|--------------------------------------------------------------------------------------|
| Data Quality Score (composite) | Gauge       | Custom: weighted average of freshness, volume, schema, distribution scores            |
| Freshness Heatmap              | Heatmap     | `data_freshness_seconds{layer=~"$layer"}` by table                                   |
| Freshness SLA Compliance       | Stat (multi)| `data_freshness_seconds < bool (threshold)` per layer                                |
| Volume Trend                   | Time series | `data_row_count{layer=~"$layer", table=~"$table"}`                                   |
| Volume Deviation               | Bar gauge   | `data_volume_deviation_percent{layer=~"$layer"}`                                      |
| Volume Anomaly Count           | Stat        | `increase(data_volume_anomaly_total[24h])`                                            |
| Schema Changes Timeline        | Annotations | `increase(schema_changes_total[1h]) > 0`                                              |
| Schema Column Count            | Table       | `schema_column_count` by table/layer                                                  |
| Distribution Z-Scores          | Time series | `data_distribution_zscore{table=~"$table"}` with threshold lines at +/-3              |
| Distribution Values            | Time series | `data_distribution_value{table=~"$table", statistic=~"$statistic"}`                   |
| Null Percentage                | Bar gauge   | `data_distribution_value{statistic="null_percent"}`                                   |
| Failed Expectations            | Table       | From Dagster/Great Expectations if integrated                                         |

**Variables**: `$layer`, `$table`, `$column`, `$statistic`.

### Dashboard 5: Infrastructure

**File**: `monitoring/grafana/dashboards/infrastructure/05-infrastructure.json`

**Panels**:

| Panel                          | Type        | PromQL                                                                                |
|--------------------------------|-------------|--------------------------------------------------------------------------------------|
| Container CPU Usage            | Time series | `rate(container_cpu_usage_seconds_total{name=~".+"}[5m]) * 100`                      |
| Container Memory Usage         | Time series | `container_memory_usage_bytes{name=~".+"}`                                            |
| Container Memory Limit %       | Bar gauge   | `container_memory_usage_bytes / container_spec_memory_limit_bytes * 100`              |
| Host CPU Usage                 | Time series | `100 - (avg(rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)`                   |
| Host Memory Usage              | Gauge       | `(1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes) * 100`            |
| Disk Usage Trend               | Time series | `100 - (node_filesystem_avail_bytes / node_filesystem_size_bytes * 100)`             |
| Disk Usage per Mount           | Table       | Filesystem stats by mountpoint                                                        |
| Network Throughput (rx/tx)     | Time series | `rate(node_network_receive_bytes_total[5m])` / `rate(node_network_transmit_bytes_total[5m])` |
| Container Network I/O          | Time series | `rate(container_network_receive_bytes_total[5m])` per container                       |
| Container Restarts             | Stat        | `increase(container_restart_count[1h])` per container                                 |
| Container Health               | Stat row    | `time() - container_last_seen < 60` (green/red per container)                         |

**Variables**: `$container` (multi-select).

### Dashboard 6: API Performance

**File**: `monitoring/grafana/dashboards/api/06-api-performance.json`

**Panels**:

| Panel                          | Type        | PromQL                                                                                |
|--------------------------------|-------------|--------------------------------------------------------------------------------------|
| Request Rate                   | Time series | `rate(graphql_requests_total[5m])`                                                   |
| Error Rate                     | Time series | `rate(graphql_requests_total{status="error"}[5m])`                                   |
| Error Percentage               | Gauge       | `rate(graphql_requests_total{status="error"}[5m]) / rate(graphql_requests_total[5m]) * 100` |
| Latency p50                    | Time series | `histogram_quantile(0.50, rate(graphql_request_duration_seconds_bucket[5m]))`        |
| Latency p95                    | Time series | `histogram_quantile(0.95, rate(graphql_request_duration_seconds_bucket[5m]))`        |
| Latency p99                    | Time series | `histogram_quantile(0.99, rate(graphql_request_duration_seconds_bucket[5m]))`        |
| Latency Heatmap                | Heatmap     | `rate(graphql_request_duration_seconds_bucket[5m])`                                   |
| Active Requests                | Stat        | `graphql_active_requests`                                                             |
| Cache Hit Rate                 | Gauge       | `rate(graphql_cache_hits_total[5m]) / (rate(graphql_cache_hits_total[5m]) + rate(graphql_cache_misses_total[5m])) * 100` |
| Top Operations by Latency      | Table       | Top 10 by `histogram_quantile(0.95, ...{operation_name=~".+"})`                      |
| Requests by Operation Type     | Pie chart   | `sum by (operation_type)(rate(graphql_requests_total[5m]))`                           |

**Variables**: `$operation_name`, `$operation_type`.

---

## 6. Alerting Rules

All alert rules are stored in `monitoring/prometheus/rules/` and loaded by Prometheus via the `rule_files` directive.

### 6.1 Critical Alerts (Immediate Response)

**File**: `monitoring/prometheus/rules/critical_alerts.yml`

```yaml
groups:
  - name: critical_alerts
    interval: 15s
    rules:
      # -----------------------------------------------------------
      # Pipeline complete failure
      # -----------------------------------------------------------
      - alert: PipelineCompleteFailure
        expr: |
          rate(kafka_server_broker_topic_metrics_messages_in_total{topic="raw-trades"}[15m]) == 0
          and
          rate(kafka_server_broker_topic_metrics_messages_in_total{topic="raw-trades"}[15m] offset 30m) > 0
        for: 5m
        labels:
          severity: critical
          service: pipeline
        annotations:
          summary: "CRITICAL: No data processed for 15+ minutes"
          description: >
            The raw-trades topic has received zero messages for the last 15 minutes
            but was receiving data 30 minutes ago. Pipeline is completely stalled.
          runbook_url: "https://wiki.internal/runbooks/pipeline-stall"

      # -----------------------------------------------------------
      # Kafka broker down
      # -----------------------------------------------------------
      - alert: KafkaBrokerDown
        expr: up{job="kafka"} == 0
        for: 1m
        labels:
          severity: critical
          service: kafka
        annotations:
          summary: "Kafka broker {{ $labels.instance }} is DOWN"
          description: >
            Kafka broker at {{ $labels.instance }} has been unreachable
            for more than 1 minute. Check container health and logs.

      # -----------------------------------------------------------
      # Flink job crash without auto-restart
      # -----------------------------------------------------------
      - alert: FlinkJobCrashNoRestart
        expr: |
          flink_jobmanager_job_uptime == 0
          and
          flink_jobmanager_job_numRestarts > 3
        for: 2m
        labels:
          severity: critical
          service: flink
        annotations:
          summary: "Flink job crashed and exhausted restart attempts"
          description: >
            Flink job has restarted {{ $value }} times and is currently
            not running. Manual intervention required.

      # -----------------------------------------------------------
      # Storage near capacity
      # -----------------------------------------------------------
      - alert: StorageNearCapacity
        expr: |
          (1 - node_filesystem_avail_bytes{fstype!="tmpfs"} / node_filesystem_size_bytes{fstype!="tmpfs"}) * 100 > 90
        for: 5m
        labels:
          severity: critical
          service: infrastructure
        annotations:
          summary: "Disk usage > 90% on {{ $labels.mountpoint }}"
          description: >
            Filesystem at {{ $labels.mountpoint }} is {{ $value | printf "%.1f" }}% full.
            Immediate cleanup or expansion required.

      # -----------------------------------------------------------
      # Data quality hard failure (zero rows or freshness critical)
      # -----------------------------------------------------------
      - alert: DataQualityHardFailure
        expr: |
          data_row_count{layer="bronze"} == 0
          or
          data_freshness_seconds{layer="bronze"} > 900
        for: 5m
        labels:
          severity: critical
          service: data-quality
        annotations:
          summary: "Data quality hard failure for {{ $labels.table }}"
          description: >
            Critical data quality issue detected. Either zero rows in
            bronze layer or data is stale for > 15 minutes.
```

### 6.2 Warning Alerts (Slack Notification)

**File**: `monitoring/prometheus/rules/warning_alerts.yml`

```yaml
groups:
  - name: warning_alerts
    interval: 30s
    rules:
      # -----------------------------------------------------------
      # Consumer lag above threshold
      # -----------------------------------------------------------
      - alert: ConsumerLagHigh
        expr: kafka_consumer_records_lag > 10000
        for: 5m
        labels:
          severity: warning
          service: kafka
        annotations:
          summary: "Consumer lag high for {{ $labels.topic }}/{{ $labels.partition }}"
          description: >
            Consumer {{ $labels.client_id }} has lag of {{ $value }}
            on topic {{ $labels.topic }} partition {{ $labels.partition }}.

      # -----------------------------------------------------------
      # Freshness SLA approaching breach
      # -----------------------------------------------------------
      - alert: FreshnessSLAApproaching
        expr: |
          (data_freshness_seconds{layer="bronze"} > 90 and data_freshness_seconds{layer="bronze"} < 120)
          or
          (data_freshness_seconds{layer="silver"} > 480 and data_freshness_seconds{layer="silver"} < 600)
          or
          (data_freshness_seconds{layer="gold"} > 5400 and data_freshness_seconds{layer="gold"} < 7200)
        for: 1m
        labels:
          severity: warning
          service: data-quality
        annotations:
          summary: "Freshness SLA approaching for {{ $labels.table }}"
          description: >
            {{ $labels.layer }}.{{ $labels.table }} freshness is at
            {{ $value | humanizeDuration }}. SLA breach imminent.

      # -----------------------------------------------------------
      # Volume anomaly
      # -----------------------------------------------------------
      - alert: VolumeAnomalyWarning
        expr: abs(data_volume_deviation_percent) > 20
        for: 5m
        labels:
          severity: warning
          service: data-quality
        annotations:
          summary: "Volume anomaly: {{ $labels.table }} ({{ $value | printf \"%.1f\" }}% deviation)"

      # -----------------------------------------------------------
      # Checkpoint duration increasing trend
      # -----------------------------------------------------------
      - alert: CheckpointDurationIncreasing
        expr: |
          flink_jobmanager_job_lastCheckpointDuration > 30000
          and
          deriv(flink_jobmanager_job_lastCheckpointDuration[30m]) > 0
        for: 10m
        labels:
          severity: warning
          service: flink
        annotations:
          summary: "Flink checkpoint duration increasing"
          description: >
            Checkpoint duration is {{ $value | humanize }}ms and trending upward.
            May indicate growing state size or I/O bottleneck.

      # -----------------------------------------------------------
      # Error rate above 1%
      # -----------------------------------------------------------
      - alert: HighErrorRate
        expr: |
          rate(graphql_requests_total{status="error"}[5m])
          / rate(graphql_requests_total[5m]) > 0.01
        for: 5m
        labels:
          severity: warning
          service: api
        annotations:
          summary: "API error rate > 1%"
          description: >
            GraphQL API error rate is {{ $value | printf "%.2f" }}%.

      # -----------------------------------------------------------
      # Backpressure detected
      # -----------------------------------------------------------
      - alert: FlinkBackpressure
        expr: flink_taskmanager_job_task_isBackPressured == 1
        for: 3m
        labels:
          severity: warning
          service: flink
        annotations:
          summary: "Backpressure detected on Flink task {{ $labels.task_name }}"
          description: >
            Task {{ $labels.task_name }} is backpressured for 3+ minutes.
            Processing throughput may be degraded.
```

### 6.3 Info Alerts (Dashboard Only)

**File**: `monitoring/prometheus/rules/info_alerts.yml`

```yaml
groups:
  - name: info_alerts
    interval: 60s
    rules:
      - alert: SchemaChangeDetected
        expr: increase(schema_changes_total[10m]) > 0
        labels:
          severity: info
          service: data-quality
        annotations:
          summary: "Schema change: {{ $labels.change_type }} on {{ $labels.table }}"

      - alert: NewTopicCreated
        expr: |
          count(kafka_server_broker_topic_metrics_messages_in_total) by (topic)
          unless
          count(kafka_server_broker_topic_metrics_messages_in_total offset 1h) by (topic)
        labels:
          severity: info
          service: kafka
        annotations:
          summary: "New Kafka topic detected: {{ $labels.topic }}"

      - alert: ConfigurationChange
        expr: changes(prometheus_config_last_reload_successful[5m]) > 0
        labels:
          severity: info
          service: monitoring
        annotations:
          summary: "Prometheus configuration reloaded"
```

---

## 7. Log Aggregation (Optional Enhancement)

### 7.1 Structured JSON Logging

All services must emit structured JSON logs to stdout. This enables downstream collection by Docker log drivers and optional aggregation systems.

**Standard log format**:

```json
{
  "timestamp": "2025-01-15T10:30:45.123Z",
  "level": "INFO",
  "logger": "trade-generator",
  "message": "Trade generated",
  "correlation_id": "uuid-v4-here",
  "service": "trade-generator",
  "trace_id": "abc123",
  "span_id": "def456",
  "context": {
    "symbol": "AAPL",
    "trade_type": "BUY",
    "price": 150.25,
    "quantity": 100
  }
}
```

**Python logging configuration** (`services/common/logging_config.py`):

```python
import logging
import json
import sys
from datetime import datetime, timezone
import uuid


class JSONFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": getattr(record, "service", "unknown"),
            "correlation_id": getattr(record, "correlation_id", str(uuid.uuid4())),
        }
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        if hasattr(record, "context"):
            log_entry["context"] = record.context
        return json.dumps(log_entry)


def setup_logging(service_name: str, level: str = "INFO"):
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())

    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level))
    logger.addHandler(handler)

    # Inject service name into all records
    old_factory = logging.getLogRecordFactory()
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.service = service_name
        return record
    logging.setLogRecordFactory(record_factory)
```

### 7.2 Log Collection Architecture

```
+------------------+     +------------------+
| Service stdout   | --> | Docker log driver | --> stdout (default)
+------------------+     +------------------+
                                |
                    (Optional enhancement)
                                |
                                v
                    +-------------------+
                    | Grafana Loki      |  <-- lightweight alternative to ELK
                    | (or Promtail +    |
                    |  Loki stack)      |
                    +-------------------+
                                |
                                v
                    +-------------------+
                    | Grafana           |
                    | (Explore > Loki)  |
                    +-------------------+
```

**Optional Loki Docker Compose**:

```yaml
services:
  loki:
    image: grafana/loki:2.9.5
    container_name: loki
    ports:
      - "3100:3100"
    volumes:
      - ./monitoring/loki/loki-config.yml:/etc/loki/local-config.yaml
      - loki-data:/loki
    command: -config.file=/etc/loki/local-config.yaml
    networks:
      - monitoring

  promtail:
    image: grafana/promtail:2.9.5
    container_name: promtail
    volumes:
      - /var/log:/var/log:ro
      - /var/lib/docker/containers:/var/lib/docker/containers:ro
      - ./monitoring/promtail/promtail-config.yml:/etc/promtail/config.yml
    command: -config.file=/etc/promtail/config.yml
    networks:
      - monitoring
```

### 7.3 Correlation IDs

Every request entering the system receives a correlation ID that propagates through all services:

```
WebSocket -> Trade Generator (generates correlation_id)
  -> Kafka message header: x-correlation-id
    -> Flink (preserves in output records)
      -> Iceberg (stored as metadata column)
        -> GraphQL API (returned in response headers)
```

This enables cross-service tracing by searching for the correlation ID in Loki/logs.

### 7.4 Log-Based Metrics Extraction

Promtail can extract metrics from structured logs without adding custom instrumentation:

```yaml
# monitoring/promtail/promtail-config.yml (pipeline_stages excerpt)
pipeline_stages:
  - json:
      expressions:
        level: level
        service: service
        message: message
  - metrics:
      log_lines_total:
        type: Counter
        description: "Total log lines by level and service"
        source: level
        config:
          action: inc
          match_all: true
      error_log_lines_total:
        type: Counter
        description: "Total error log lines"
        source: level
        config:
          action: inc
          match_all: false
          value: "ERROR"
```

---

## 8. Implementation Steps

### Phase 1: Core Monitoring Stack (Week 1)

| Step | Task                                                        | Files                                                    |
|------|-------------------------------------------------------------|----------------------------------------------------------|
| 1.1  | Create directory structure                                  | `monitoring/` tree (see File Tree below)                 |
| 1.2  | Write Prometheus configuration                              | `monitoring/prometheus/prometheus.yml`                    |
| 1.3  | Write Alertmanager configuration                            | `monitoring/alertmanager/alertmanager.yml`                |
| 1.4  | Write Grafana provisioning files                            | `monitoring/grafana/provisioning/`                        |
| 1.5  | Write `docker-compose.monitoring.yml`                       | Root: `docker-compose.monitoring.yml`                    |
| 1.6  | Deploy Prometheus + Grafana + Alertmanager + Pushgateway    | `docker compose -f docker-compose.monitoring.yml up -d`  |
| 1.7  | Verify all targets are UP in Prometheus UI                  | `http://localhost:9090/targets`                          |

### Phase 2: Kafka & Flink Metrics (Week 1-2)

| Step | Task                                                        | Files                                                    |
|------|-------------------------------------------------------------|----------------------------------------------------------|
| 2.1  | Add JMX Exporter agent to Kafka broker containers           | `services/kafka/Dockerfile` (ENV KAFKA_OPTS)             |
| 2.2  | Write JMX Exporter config                                   | `monitoring/kafka/jmx-exporter-config.yml`               |
| 2.3  | Configure Flink Prometheus reporter                         | `services/flink/conf/flink-conf.yaml`                    |
| 2.4  | Write Flink recording rules                                 | `monitoring/prometheus/rules/flink_rules.yml`            |
| 2.5  | Verify Kafka and Flink metrics in Prometheus                | Query `kafka_*` and `flink_*` in Prometheus UI           |
| 2.6  | Build Kafka Deep Dive dashboard                             | `monitoring/grafana/dashboards/kafka/02-kafka-deep-dive.json`   |
| 2.7  | Build Flink Jobs dashboard                                  | `monitoring/grafana/dashboards/flink/03-flink-jobs.json`        |

### Phase 3: Application Metrics (Week 2)

| Step | Task                                                        | Files                                                    |
|------|-------------------------------------------------------------|----------------------------------------------------------|
| 3.1  | Add `prometheus_client` dependency to all Python services   | `requirements.txt` or `pyproject.toml` per service       |
| 3.2  | Implement Trade Generator metrics + /metrics endpoint       | `services/trade-generator/metrics.py`                    |
| 3.3  | Implement Kafka Consumer metrics + /metrics endpoint        | `services/kafka-consumer/metrics.py`                     |
| 3.4  | Implement GraphQL API metrics + /metrics endpoint           | `services/graphql-api/metrics.py`                        |
| 3.5  | Verify all application metrics in Prometheus                | Query `trade_generator_*`, `kafka_bridge_*`, `graphql_*` |
| 3.6  | Build API Performance dashboard                             | `monitoring/grafana/dashboards/api/06-api-performance.json`     |

### Phase 4: Dagster Metrics & Data Observability (Week 2-3)

| Step | Task                                                        | Files                                                    |
|------|-------------------------------------------------------------|----------------------------------------------------------|
| 4.1  | Implement Dagster Pushgateway helper                        | `services/dagster/metrics/dagster_metrics.py`            |
| 4.2  | Implement success/failure hooks                             | `services/dagster/hooks/metrics_hook.py`                 |
| 4.3  | Implement Freshness sensor                                  | `services/dagster/sensors/freshness_sensor.py`           |
| 4.4  | Implement Volume sensor                                     | `services/dagster/sensors/volume_sensor.py`              |
| 4.5  | Implement Schema sensor                                     | `services/dagster/sensors/schema_sensor.py`              |
| 4.6  | Implement Distribution sensor                               | `services/dagster/sensors/distribution_sensor.py`        |
| 4.7  | Implement Lineage sensor                                    | `services/dagster/sensors/lineage_sensor.py`             |
| 4.8  | Write all alert rule files                                  | `monitoring/prometheus/rules/*.yml`                      |
| 4.9  | Build Data Quality dashboard                                | `monitoring/grafana/dashboards/data-quality/04-data-quality.json` |

### Phase 5: Infrastructure Metrics & Dashboards (Week 3)

| Step | Task                                                        | Files                                                    |
|------|-------------------------------------------------------------|----------------------------------------------------------|
| 5.1  | Add Node Exporter and cAdvisor to compose                   | `docker-compose.monitoring.yml`                          |
| 5.2  | Build Infrastructure dashboard                              | `monitoring/grafana/dashboards/infrastructure/05-infrastructure.json` |
| 5.3  | Build Pipeline Overview dashboard                           | `monitoring/grafana/dashboards/pipeline/01-pipeline-overview.json`    |

### Phase 6: Testing & Hardening (Week 3-4)

| Step | Task                                                        | Files                                                    |
|------|-------------------------------------------------------------|----------------------------------------------------------|
| 6.1  | Unit test alert rules with promtool                         | `monitoring/prometheus/tests/`                            |
| 6.2  | Validate dashboard JSON files                               | CI script using Grafana API                              |
| 6.3  | Verify all /metrics endpoints                               | Integration test script                                  |
| 6.4  | Test alert routing (fire test alerts)                       | Manual + automated test                                  |
| 6.5  | Load test to verify metric cardinality is manageable        | Stress test script                                       |
| 6.6  | Document runbooks for each critical alert                   | `monitoring/runbooks/`                                   |

### Phase 7: Optional Enhancements (Week 4+)

| Step | Task                                                        | Files                                                    |
|------|-------------------------------------------------------------|----------------------------------------------------------|
| 7.1  | Set up Loki for log aggregation                             | `monitoring/loki/`, `monitoring/promtail/`               |
| 7.2  | Implement structured JSON logging in all services           | `services/common/logging_config.py`                      |
| 7.3  | Add correlation ID propagation                              | All services                                             |
| 7.4  | Configure log-based metric extraction                       | `monitoring/promtail/promtail-config.yml`                |

---

## 9. Testing Strategy

### 9.1 Alert Rule Unit Tests (promtool)

**File**: `monitoring/prometheus/tests/test_alerts.yml`

```yaml
# Test cases for promtool test rules
rule_files:
  - ../rules/critical_alerts.yml
  - ../rules/warning_alerts.yml
  - ../rules/freshness_rules.yml
  - ../rules/volume_rules.yml
  - ../rules/schema_rules.yml
  - ../rules/distribution_rules.yml
  - ../rules/lineage_rules.yml

evaluation_interval: 15s

tests:
  # -----------------------------------------------------------
  # Test: Pipeline complete failure fires after 5m
  # -----------------------------------------------------------
  - interval: 1m
    input_series:
      - series: 'kafka_server_broker_topic_metrics_messages_in_total{topic="raw-trades"}'
        values: "100+100x30 100x20"  # stops increasing at minute 30
    alert_rule_test:
      - eval_time: 50m
        alertname: PipelineCompleteFailure
        exp_alerts:
          - exp_labels:
              severity: critical
              service: pipeline
              topic: raw-trades

  # -----------------------------------------------------------
  # Test: Kafka broker down fires after 1m
  # -----------------------------------------------------------
  - interval: 15s
    input_series:
      - series: 'up{job="kafka", instance="kafka-broker-1:7071"}'
        values: "1 1 1 1 0 0 0 0 0"
    alert_rule_test:
      - eval_time: 2m
        alertname: KafkaBrokerDown
        exp_alerts:
          - exp_labels:
              severity: critical
              service: kafka
              instance: kafka-broker-1:7071

  # -----------------------------------------------------------
  # Test: Bronze freshness violation at 2 min
  # -----------------------------------------------------------
  - interval: 15s
    input_series:
      - series: 'data_freshness_seconds{table="raw_trades", layer="bronze"}'
        values: "60 90 121 130 140 150 160"
    alert_rule_test:
      - eval_time: 3m
        alertname: BronzeFreshnessViolation
        exp_alerts:
          - exp_labels:
              severity: warning
              service: data-quality
              table: raw_trades
              layer: bronze

  # -----------------------------------------------------------
  # Test: Volume anomaly fires when deviation > 20%
  # -----------------------------------------------------------
  - interval: 1m
    input_series:
      - series: 'data_volume_deviation_percent{table="raw_trades", layer="bronze"}'
        values: "5 10 15 25 25 25 25 25"
    alert_rule_test:
      - eval_time: 8m
        alertname: VolumeAnomalyWarning
        exp_alerts:
          - exp_labels:
              severity: warning
              service: data-quality
              table: raw_trades
              layer: bronze

  # -----------------------------------------------------------
  # Test: Consumer lag warning
  # -----------------------------------------------------------
  - interval: 15s
    input_series:
      - series: 'kafka_consumer_records_lag{client_id="flink-consumer", topic="raw-trades", partition="0"}'
        values: "5000 8000 11000 12000 13000 14000 15000 16000 17000 18000 19000 20000 21000 22000"
    alert_rule_test:
      - eval_time: 5m
        alertname: ConsumerLagHigh
        exp_alerts:
          - exp_labels:
              severity: warning
              service: kafka

  # -----------------------------------------------------------
  # Test: Distribution anomaly fires when z-score > 3
  # -----------------------------------------------------------
  - interval: 1m
    input_series:
      - series: 'data_distribution_zscore{table="bronze.raw_trades", column="price", statistic="mean"}'
        values: "0.5 1.0 1.5 3.5 3.5 3.5 3.5 3.5 3.5 3.5"
    alert_rule_test:
      - eval_time: 8m
        alertname: DistributionAnomaly
        exp_alerts:
          - exp_labels:
              severity: warning
              service: data-quality

  # -----------------------------------------------------------
  # Test: Storage near capacity
  # -----------------------------------------------------------
  - interval: 1m
    input_series:
      - series: 'node_filesystem_avail_bytes{fstype="ext4", mountpoint="/"}'
        values: "5000000000 5000000000 5000000000"
      - series: 'node_filesystem_size_bytes{fstype="ext4", mountpoint="/"}'
        values: "50000000000 50000000000 50000000000"
    alert_rule_test:
      - eval_time: 6m
        alertname: StorageNearCapacity
        exp_alerts:
          - exp_labels:
              severity: critical
              service: infrastructure
```

**Run tests**:

```bash
promtool test rules monitoring/prometheus/tests/test_alerts.yml
```

**Validate Prometheus config**:

```bash
promtool check config monitoring/prometheus/prometheus.yml
```

**Validate recording and alert rules**:

```bash
promtool check rules monitoring/prometheus/rules/*.yml
```

### 9.2 Dashboard Validation

**Script**: `monitoring/scripts/validate_dashboards.sh`

```bash
#!/bin/bash
# Validate all Grafana dashboard JSON files.

set -euo pipefail

DASHBOARD_DIR="monitoring/grafana/dashboards"
ERRORS=0

echo "Validating Grafana dashboards..."

for json_file in $(find "$DASHBOARD_DIR" -name "*.json" -type f); do
    echo -n "  Checking $json_file ... "

    # 1. Valid JSON
    if ! python3 -c "import json; json.load(open('$json_file'))" 2>/dev/null; then
        echo "FAIL (invalid JSON)"
        ERRORS=$((ERRORS + 1))
        continue
    fi

    # 2. Has required top-level keys
    REQUIRED_KEYS=("title" "panels" "uid")
    for key in "${REQUIRED_KEYS[@]}"; do
        if ! python3 -c "
import json, sys
d = json.load(open('$json_file'))
if '$key' not in d:
    sys.exit(1)
" 2>/dev/null; then
            echo "FAIL (missing key: $key)"
            ERRORS=$((ERRORS + 1))
            continue 2
        fi
    done

    # 3. No duplicate panel IDs
    DUPLICATE_IDS=$(python3 -c "
import json
d = json.load(open('$json_file'))
ids = [p.get('id') for p in d.get('panels', []) if 'id' in p]
dups = [x for x in ids if ids.count(x) > 1]
if dups:
    print(','.join(map(str, set(dups))))
" 2>/dev/null)

    if [ -n "$DUPLICATE_IDS" ]; then
        echo "FAIL (duplicate panel IDs: $DUPLICATE_IDS)"
        ERRORS=$((ERRORS + 1))
        continue
    fi

    echo "OK"
done

if [ $ERRORS -gt 0 ]; then
    echo ""
    echo "FAILED: $ERRORS dashboard(s) have errors"
    exit 1
else
    echo ""
    echo "All dashboards validated successfully."
fi
```

### 9.3 Metric Endpoint Verification

**Script**: `monitoring/scripts/verify_metrics.sh`

```bash
#!/bin/bash
# Verify that all expected /metrics endpoints are reachable and returning data.

set -euo pipefail

ENDPOINTS=(
    "http://localhost:9090/metrics|prometheus"
    "http://localhost:7071/metrics|kafka-broker-1"
    "http://localhost:9249/metrics|flink-jobmanager"
    "http://localhost:9091/metrics|pushgateway"
    "http://localhost:8000/metrics|trade-generator"
    "http://localhost:8001/metrics|kafka-consumer"
    "http://localhost:8002/metrics|graphql-api"
    "http://localhost:9100/metrics|node-exporter"
    "http://localhost:8080/metrics|cadvisor"
)

ERRORS=0

echo "Verifying metric endpoints..."
echo ""

for entry in "${ENDPOINTS[@]}"; do
    IFS='|' read -r url name <<< "$entry"
    echo -n "  $name ($url) ... "

    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 5 "$url" 2>/dev/null || echo "000")

    if [ "$HTTP_CODE" == "200" ]; then
        # Count number of metric lines
        METRIC_COUNT=$(curl -s --connect-timeout 5 "$url" 2>/dev/null | grep -v "^#" | grep -c "." || echo "0")
        echo "OK ($METRIC_COUNT metrics)"
    else
        echo "FAIL (HTTP $HTTP_CODE)"
        ERRORS=$((ERRORS + 1))
    fi
done

echo ""
if [ $ERRORS -gt 0 ]; then
    echo "FAILED: $ERRORS endpoint(s) unreachable"
    exit 1
else
    echo "All metric endpoints healthy."
fi
```

### 9.4 Integration Test: Alert Firing

**Script**: `monitoring/scripts/test_alert_firing.py`

```python
"""
Integration test: push a synthetic metric that should trigger an alert,
then verify via Alertmanager API that the alert fired.
"""

import requests
import time

PROMETHEUS_URL = "http://localhost:9090"
ALERTMANAGER_URL = "http://localhost:9093"
PUSHGATEWAY_URL = "http://localhost:9091"


def push_test_metric():
    """Push a metric that will trigger BronzeFreshnessViolation."""
    metric_data = (
        '# TYPE data_freshness_seconds gauge\n'
        'data_freshness_seconds{table="test_table",layer="bronze",job="test"} 300\n'
    )
    response = requests.post(
        f"{PUSHGATEWAY_URL}/metrics/job/integration_test",
        data=metric_data,
        headers={"Content-Type": "text/plain"},
    )
    assert response.status_code == 200, f"Pushgateway returned {response.status_code}"
    print("Pushed test metric to Pushgateway")


def wait_for_alert(alert_name: str, timeout: int = 180):
    """Wait for the alert to appear in Alertmanager."""
    start = time.time()
    while time.time() - start < timeout:
        response = requests.get(f"{ALERTMANAGER_URL}/api/v2/alerts")
        alerts = response.json()
        for alert in alerts:
            if alert["labels"].get("alertname") == alert_name:
                print(f"Alert '{alert_name}' fired successfully!")
                return True
        time.sleep(10)
    print(f"TIMEOUT: Alert '{alert_name}' did not fire within {timeout}s")
    return False


def cleanup_test_metric():
    """Remove the test metric from Pushgateway."""
    requests.delete(f"{PUSHGATEWAY_URL}/metrics/job/integration_test")
    print("Cleaned up test metric from Pushgateway")


if __name__ == "__main__":
    try:
        push_test_metric()
        success = wait_for_alert("BronzeFreshnessViolation", timeout=180)
        if not success:
            exit(1)
    finally:
        cleanup_test_metric()
```

---

## 10. File Tree Summary

```
monitoring/
├── prometheus/
│   ├── prometheus.yml                          # Main Prometheus config
│   ├── rules/
│   │   ├── critical_alerts.yml                 # Critical alert rules
│   │   ├── warning_alerts.yml                  # Warning alert rules
│   │   ├── info_alerts.yml                     # Info alert rules
│   │   ├── freshness_rules.yml                 # Data freshness alerts
│   │   ├── volume_rules.yml                    # Data volume alerts
│   │   ├── schema_rules.yml                    # Schema drift alerts
│   │   ├── distribution_rules.yml              # Distribution anomaly alerts
│   │   ├── lineage_rules.yml                   # Pipeline lineage alerts
│   │   └── flink_rules.yml                     # Flink recording rules
│   └── tests/
│       └── test_alerts.yml                     # promtool test cases
│
├── alertmanager/
│   ├── alertmanager.yml                        # Alertmanager config
│   └── templates/
│       └── slack.tmpl                          # Slack message template
│
├── grafana/
│   ├── grafana.ini                             # Grafana server config
│   ├── provisioning/
│   │   ├── datasources/
│   │   │   └── prometheus.yml                  # Prometheus datasource
│   │   └── dashboards/
│   │       └── dashboards.yml                  # Dashboard provider config
│   └── dashboards/
│       ├── pipeline/
│       │   └── 01-pipeline-overview.json       # Dashboard 1
│       ├── kafka/
│       │   └── 02-kafka-deep-dive.json         # Dashboard 2
│       ├── flink/
│       │   └── 03-flink-jobs.json              # Dashboard 3
│       ├── data-quality/
│       │   └── 04-data-quality.json            # Dashboard 4
│       ├── infrastructure/
│       │   └── 05-infrastructure.json          # Dashboard 5
│       └── api/
│           └── 06-api-performance.json         # Dashboard 6
│
├── kafka/
│   └── jmx-exporter-config.yml                # JMX Exporter rules
│
├── loki/                                       # (Optional)
│   └── loki-config.yml
│
├── promtail/                                   # (Optional)
│   └── promtail-config.yml
│
├── scripts/
│   ├── validate_dashboards.sh                  # Dashboard JSON validation
│   ├── verify_metrics.sh                       # Metric endpoint checks
│   └── test_alert_firing.py                    # Integration test
│
└── runbooks/                                   # Alert runbooks (Phase 6)
    ├── pipeline-stall.md
    ├── kafka-broker-down.md
    └── flink-job-crash.md

services/
├── dagster/
│   ├── metrics/
│   │   └── dagster_metrics.py                  # Pushgateway metric definitions
│   ├── hooks/
│   │   └── metrics_hook.py                     # Success/failure hooks
│   └── sensors/
│       ├── freshness_sensor.py                 # Freshness monitoring
│       ├── volume_sensor.py                    # Volume monitoring
│       ├── schema_sensor.py                    # Schema drift monitoring
│       ├── distribution_sensor.py              # Distribution monitoring
│       └── lineage_sensor.py                   # Pipeline lineage monitoring
│
├── trade-generator/
│   └── metrics.py                              # Custom Prometheus metrics
│
├── kafka-consumer/
│   └── metrics.py                              # Custom Prometheus metrics
│
├── graphql-api/
│   └── metrics.py                              # Custom Prometheus metrics
│
├── common/
│   └── logging_config.py                       # Structured JSON logging
│
└── flink/
    └── conf/
        └── flink-conf.yaml                     # Flink metrics reporter config

docker-compose.monitoring.yml                   # Monitoring stack compose file
```

---

## Appendix A: Docker Compose for Monitoring Stack

**File**: `docker-compose.monitoring.yml`

```yaml
version: "3.8"

services:
  prometheus:
    image: prom/prometheus:v2.51.0
    container_name: prometheus
    restart: unless-stopped
    command:
      - "--config.file=/etc/prometheus/prometheus.yml"
      - "--storage.tsdb.path=/prometheus/data"
      - "--storage.tsdb.retention.time=15d"
      - "--storage.tsdb.retention.size=10GB"
      - "--web.enable-lifecycle"
      - "--web.enable-admin-api"
    volumes:
      - ./monitoring/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - ./monitoring/prometheus/rules:/etc/prometheus/rules:ro
      - prometheus-data:/prometheus/data
    ports:
      - "9090:9090"
    networks:
      - monitoring
      - default

  alertmanager:
    image: prom/alertmanager:v0.27.0
    container_name: alertmanager
    restart: unless-stopped
    command:
      - "--config.file=/etc/alertmanager/alertmanager.yml"
      - "--storage.path=/alertmanager"
    volumes:
      - ./monitoring/alertmanager/alertmanager.yml:/etc/alertmanager/alertmanager.yml:ro
      - ./monitoring/alertmanager/templates:/etc/alertmanager/templates:ro
      - alertmanager-data:/alertmanager
    ports:
      - "9093:9093"
    networks:
      - monitoring
    environment:
      - SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_URL:-}
      - SMTP_USERNAME=${SMTP_USERNAME:-}
      - SMTP_PASSWORD=${SMTP_PASSWORD:-}

  grafana:
    image: grafana/grafana:10.4.1
    container_name: grafana
    restart: unless-stopped
    volumes:
      - ./monitoring/grafana/grafana.ini:/etc/grafana/grafana.ini:ro
      - ./monitoring/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./monitoring/grafana/dashboards:/var/lib/grafana/dashboards:ro
      - grafana-data:/var/lib/grafana
    ports:
      - "3000:3000"
    networks:
      - monitoring
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=${GRAFANA_ADMIN_PASSWORD:-admin}
    depends_on:
      - prometheus

  pushgateway:
    image: prom/pushgateway:v1.8.0
    container_name: pushgateway
    restart: unless-stopped
    ports:
      - "9091:9091"
    networks:
      - monitoring

  node-exporter:
    image: prom/node-exporter:v1.8.1
    container_name: node-exporter
    restart: unless-stopped
    volumes:
      - /proc:/host/proc:ro
      - /sys:/host/sys:ro
      - /:/rootfs:ro
    command:
      - "--path.procfs=/host/proc"
      - "--path.sysfs=/host/sys"
      - "--path.rootfs=/rootfs"
      - "--collector.filesystem.mount-points-exclude=^/(sys|proc|dev|host|etc)($$|/)"
    ports:
      - "9100:9100"
    networks:
      - monitoring

  cadvisor:
    image: gcr.io/cadvisor/cadvisor:v0.49.1
    container_name: cadvisor
    restart: unless-stopped
    privileged: true
    volumes:
      - /:/rootfs:ro
      - /var/run:/var/run:ro
      - /sys:/sys:ro
      - /var/lib/docker/:/var/lib/docker:ro
      - /dev/disk/:/dev/disk:ro
    ports:
      - "8080:8080"
    networks:
      - monitoring

volumes:
  prometheus-data:
  alertmanager-data:
  grafana-data:

networks:
  monitoring:
    driver: bridge
```

---

## Appendix B: Metric Cardinality Budget

To prevent Prometheus storage issues, the following cardinality limits apply:

| Source              | Estimated Series Count | Notes                                     |
|---------------------|------------------------|-------------------------------------------|
| Kafka (3 brokers)   | ~500                   | 3 brokers x ~15 topics x ~10 metrics     |
| Flink               | ~300                   | 2 TMs x ~50 metrics per task             |
| Dagster (Pushgateway)| ~200                  | ~20 tables x ~10 metrics                 |
| Trade Generator      | ~50                   | Low cardinality (few label combos)        |
| Kafka Consumer       | ~50                   | Low cardinality                           |
| GraphQL API          | ~150                  | Operation name labels (bounded set)       |
| Node Exporter        | ~500                  | Host metrics with filesystem labels       |
| cAdvisor             | ~800                  | Per-container metrics (~15 containers)    |
| Data Observability   | ~400                  | 5 pillars x ~20 tables x ~4 labels       |
| **TOTAL**            | **~2,950**            | Well within Prometheus capacity           |

**Guardrails**:
- Use `metric_relabel_configs` in Prometheus to drop high-cardinality labels if needed
- Monitor `prometheus_tsdb_symbol_table_size_bytes` for early warning
- Set `--storage.tsdb.retention.size=10GB` as a hard cap
