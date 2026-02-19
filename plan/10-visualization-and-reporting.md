# 10 - Visualization & Consumption (Apache Superset)

## Overview

This document details the setup, configuration, and implementation of Apache Superset as the
primary visualization and reporting layer for the stock market trade data platform. Superset
provides interactive dashboards, ad-hoc SQL exploration, and scheduled reports on top of the
Gold/Silver layer data accessible via DuckDB and Iceberg.

---

## 1. Technology Choice: Apache Superset

### Why Apache Superset

| Criterion                   | Apache Superset                 | Alternatives Considered               |
|-----------------------------|---------------------------------|---------------------------------------|
| **Cost**                    | Free, open-source (Apache 2.0)  | Tableau ($$$$), Looker ($$$), Metabase |
| **Visualization Library**   | 40+ chart types, extensible     | Tableau has richer defaults            |
| **SQL Support**             | First-class SQL Lab              | Metabase has basic SQL mode            |
| **DuckDB Support**          | Via SQLAlchemy + duckdb-engine   | Tableau requires JDBC connector        |
| **Dashboard Sharing**       | URL sharing, embedding, export   | Tableau/Looker have better collab      |
| **Role-Based Access**       | Built-in RBAC                   | All alternatives offer RBAC            |
| **API**                     | Full REST API                   | Tableau has REST API too               |
| **Self-Hosted**             | Docker-native                   | Metabase also easy to self-host        |
| **Custom Plugins**          | React-based viz plugin system    | Tableau has Extensions API             |
| **Community**               | 55k+ GitHub stars, active       | Metabase: 35k+ stars                   |

### Key Advantages for This Project

- **Open-source, no licensing cost**: Critical for a data engineering project where the budget
  should go to compute and storage, not BI tool licenses.
- **SQL-first approach**: Analysts can write raw SQL queries against the Iceberg tables through
  SQL Lab, bridging the gap between data engineering and analytics.
- **DuckDB integration**: Superset connects to DuckDB via SQLAlchemy, enabling direct queries
  against Iceberg tables without needing a separate serving database.
- **Extensible**: The React-based plugin system allows building custom visualizations (e.g.,
  candlestick charts, order book depth charts) that are not available out of the box.
- **Embedding**: Dashboards can be embedded in external applications via iframe or the Superset
  Embedded SDK, enabling integration with the broader trading platform.

### Version Requirements

```
apache-superset >= 3.1.0
duckdb-engine >= 0.11.0
sqlalchemy-duckdb >= 0.1.0
redis >= 5.0.0
```

---

## 2. Superset Setup

### 2.1 Docker Deployment Configuration

```yaml
# services/superset/docker-compose.yml

version: "3.9"

x-superset-common: &superset-common
  image: apache/superset:3.1.0
  env_file: .env
  volumes:
    - ./superset_config.py:/app/pythonpath/superset_config.py
    - ./custom_plugins:/app/superset/static/assets/custom_plugins
    - superset-data:/app/superset_home
  depends_on:
    superset-db:
      condition: service_healthy
    superset-redis:
      condition: service_healthy

services:
  # ─── Superset Web Server ──────────────────────────────────
  superset:
    <<: *superset-common
    container_name: superset-web
    ports:
      - "8088:8088"
    command: >
      /bin/bash -c "
        superset db upgrade &&
        superset init &&
        gunicorn
          --bind 0.0.0.0:8088
          --workers 4
          --worker-class gevent
          --timeout 120
          --limit-request-line 8190
          'superset.app:create_app()'
      "
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8088/health"]
      interval: 30s
      timeout: 10s
      retries: 5

  # ─── Celery Worker (async queries, reports) ───────────────
  superset-worker:
    <<: *superset-common
    container_name: superset-worker
    command: >
      celery
        --app=superset.tasks.celery_app:app
        worker
        --pool=prefork
        --concurrency=4
        --max-tasks-per-child=128
        -Ofair
        --loglevel=INFO
    healthcheck:
      test: ["CMD", "celery", "-A", "superset.tasks.celery_app:app", "inspect", "ping"]
      interval: 30s
      timeout: 10s
      retries: 5

  # ─── Celery Beat (scheduled reports) ──────────────────────
  superset-beat:
    <<: *superset-common
    container_name: superset-beat
    command: >
      celery
        --app=superset.tasks.celery_app:app
        beat
        --schedule=/tmp/celerybeat-schedule
        --pidfile=/tmp/celerybeat.pid
        --loglevel=INFO

  # ─── Metadata Database (PostgreSQL) ───────────────────────
  superset-db:
    image: postgres:16-alpine
    container_name: superset-db
    environment:
      POSTGRES_DB: superset
      POSTGRES_USER: superset
      POSTGRES_PASSWORD: ${SUPERSET_DB_PASSWORD}
    volumes:
      - superset-db-data:/var/lib/postgresql/data
    ports:
      - "5433:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U superset"]
      interval: 10s
      timeout: 5s
      retries: 5

  # ─── Redis (caching + Celery broker) ──────────────────────
  superset-redis:
    image: redis:7-alpine
    container_name: superset-redis
    ports:
      - "6380:6379"
    volumes:
      - superset-redis-data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  superset-data:
  superset-db-data:
  superset-redis-data:
```

### 2.2 Superset Configuration

```python
# services/superset/superset_config.py

import os
from datetime import timedelta

# ─── General ────────────────────────────────────────────────
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY")
APP_NAME = "Trade Data Analytics"
APP_ICON = "/static/assets/images/trade-logo.png"

# ─── Metadata Database ─────────────────────────────────────
SQLALCHEMY_DATABASE_URI = (
    f"postgresql://{os.environ['SUPERSET_DB_USER']}:"
    f"{os.environ['SUPERSET_DB_PASSWORD']}@"
    f"superset-db:5432/superset"
)

# ─── Redis / Cache ─────────────────────────────────────────
REDIS_HOST = os.environ.get("REDIS_HOST", "superset-redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,          # 5 min default
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": REDIS_DB,
}

DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 600,          # 10 min for query results
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 1,
}

FILTER_STATE_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,        # 24h for filter states
    "CACHE_KEY_PREFIX": "superset_filter_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 2,
}

# ─── Celery (async queries + reports) ──────────────────────
class CeleryConfig:
    broker_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/3"
    result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/4"
    imports = ("superset.sql_lab", "superset.tasks.scheduler")
    task_annotations = {
        "sql_lab.get_sql_results": {"rate_limit": "100/m"},
        "email_reports.send": {"rate_limit": "50/m"},
    }
    beat_schedule = {
        "email-reports": {
            "task": "email_reports.send",
            "schedule": timedelta(minutes=10),
        },
        "cache-warmup": {
            "task": "cache.warm",
            "schedule": timedelta(minutes=30),
        },
    }

CELERY_CONFIG = CeleryConfig

# ─── Feature Flags ─────────────────────────────────────────
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_NATIVE_FILTERS_SET": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "EMBEDDED_SUPERSET": True,
    "ALERT_REPORTS": True,
    "GLOBAL_ASYNC_QUERIES": True,
    "ESTIMATE_QUERY_COST": True,
    "SCHEDULED_QUERIES": True,
    "SQL_VALIDATORS_BY_ENGINE": True,
}

# ─── SQL Lab ────────────────────────────────────────────────
SQL_MAX_ROW = 100000
SQLLAB_TIMEOUT = 300
SQLLAB_DEFAULT_DBID = 1
SQLLAB_ASYNC_TIME_LIMIT_SEC = 600

# ─── Row Level Security ────────────────────────────────────
ENABLE_ROW_LEVEL_SECURITY = True

# ─── Thumbnail generation for dashboards ───────────────────
THUMBNAILS = True
THUMBNAIL_SELENIUM_USER = "admin"
```

### 2.3 Database Connections

**DuckDB Connection (for Iceberg/Lakehouse data):**

```python
# Connection string for DuckDB via SQLAlchemy
# Configured in Superset UI: Settings > Database Connections > + Database

DUCKDB_CONNECTION = {
    "engine": "duckdb",
    "display_name": "Trade Lakehouse (DuckDB/Iceberg)",
    "sqlalchemy_uri": "duckdb:///tmp/trade_analytics.duckdb",
    "extra": {
        "engine_params": {
            "connect_args": {
                "read_only": True,
                "config": {
                    "threads": 4,
                    "memory_limit": "4GB",
                    "enable_object_cache": True,
                },
            }
        },
        "allows_virtual_table_explore": True,
    },
}
```

**DuckDB Initialization Script (run on connection):**

```sql
-- services/superset/init_duckdb.sql
-- Executed on each new DuckDB connection to register Iceberg tables

INSTALL iceberg;
LOAD iceberg;
INSTALL httpfs;
LOAD httpfs;

SET s3_region = 'us-east-1';
SET s3_access_key_id = '${AWS_ACCESS_KEY_ID}';
SET s3_secret_access_key = '${AWS_SECRET_ACCESS_KEY}';

-- Register Gold layer views for Superset discovery
CREATE OR REPLACE VIEW daily_trade_summary AS
SELECT * FROM iceberg_scan('s3://trade-lakehouse/gold/daily_trade_summary');

CREATE OR REPLACE VIEW trader_performance_metrics AS
SELECT * FROM iceberg_scan('s3://trade-lakehouse/gold/trader_performance_metrics');

CREATE OR REPLACE VIEW symbol_reference AS
SELECT * FROM iceberg_scan('s3://trade-lakehouse/gold/symbol_reference');

CREATE OR REPLACE VIEW sector_daily_summary AS
SELECT * FROM iceberg_scan('s3://trade-lakehouse/gold/sector_daily_summary');

CREATE OR REPLACE VIEW market_daily_overview AS
SELECT * FROM iceberg_scan('s3://trade-lakehouse/gold/market_daily_overview');

CREATE OR REPLACE VIEW portfolio_positions AS
SELECT * FROM iceberg_scan('s3://trade-lakehouse/gold/portfolio_positions');

-- Silver layer views (for detailed queries)
CREATE OR REPLACE VIEW trades_enriched AS
SELECT * FROM iceberg_scan('s3://trade-lakehouse/silver/trades_enriched');

CREATE OR REPLACE VIEW order_book_snapshots AS
SELECT * FROM iceberg_scan('s3://trade-lakehouse/silver/order_book_snapshots');
```

**PostgreSQL Connection (for pipeline metadata):**

```python
POSTGRES_CONNECTION = {
    "engine": "postgresql",
    "display_name": "Pipeline Metadata (PostgreSQL)",
    "sqlalchemy_uri": (
        "postgresql://analyst:${ANALYST_PASSWORD}"
        "@pipeline-db:5432/pipeline_metadata"
    ),
    "extra": {
        "allows_virtual_table_explore": False,
    },
}
```

### 2.4 Authentication Configuration

```python
# services/superset/superset_config.py (continued)

# ─── Authentication (OAuth2 via Keycloak/Auth0) ────────────
from flask_appbuilder.security.manager import AUTH_OAUTH

AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Viewer"   # Default role for new users

OAUTH_PROVIDERS = [
    {
        "name": "keycloak",
        "icon": "fa-key",
        "token_key": "access_token",
        "remote_app": {
            "client_id": os.environ["OAUTH_CLIENT_ID"],
            "client_secret": os.environ["OAUTH_CLIENT_SECRET"],
            "api_base_url": f"{os.environ['KEYCLOAK_URL']}/realms/trade-platform/protocol/openid-connect/",
            "access_token_url": f"{os.environ['KEYCLOAK_URL']}/realms/trade-platform/protocol/openid-connect/token",
            "authorize_url": f"{os.environ['KEYCLOAK_URL']}/realms/trade-platform/protocol/openid-connect/auth",
            "client_kwargs": {
                "scope": "openid profile email roles",
            },
            "jwks_uri": f"{os.environ['KEYCLOAK_URL']}/realms/trade-platform/protocol/openid-connect/certs",
        },
    },
]

# ─── Role Mapping ──────────────────────────────────────────
AUTH_ROLES_MAPPING = {
    "admin": ["Admin"],
    "analyst": ["Alpha", "sql_lab"],
    "trader": ["Gamma"],
    "viewer": ["Viewer"],
}

# Custom roles
CUSTOM_ROLES = {
    "Viewer": {
        "all_datasource_access",
        "can_read on Dashboard",
        "can_read on Chart",
    },
    "Analyst": {
        "all_datasource_access",
        "can_read on Dashboard",
        "can_read on Chart",
        "can_write on SavedQuery",
        "can_read on Query",
        "can_sql_lab",
    },
}
```

---

## 3. Pre-built Dashboards

### Dashboard 1: Executive Market Overview

**Purpose:** High-level summary of market activity for executives and portfolio managers.
Refreshes every 5 minutes. Shows the latest trading day by default with a date range selector.

```
+─────────────────────────────────────────────────────────────────────────+
|  EXECUTIVE MARKET OVERVIEW                            [Date Selector]   |
+─────────────────────────────────────────────────────────────────────────+
|                          |                            |                 |
|  TOTAL TRADES TODAY      |  TOTAL VOLUME              | TOTAL VALUE     |
|  1,247,893               |  342.5M shares             | $48.2B          |
|  +12.3% vs yesterday     |  +8.1% vs yesterday        | +5.4%           |
|                          |                            |                 |
+──────────────────────────+────────────────────────────+-----------------+
|                                                       |                 |
|  MARKET-WIDE TRADING VOLUME (30 Days)                 | MARKET BREADTH  |
|  ╔═══════════════════════════════════════╗             | ┌─────────────┐ |
|  ║  [Bar chart: daily volume bars        ║             | │ Advancing:  │ |
|  ║   with 10-day moving average line]    ║             | │   312       │ |
|  ║                                       ║             | │ Declining:  │ |
|  ║                                       ║             | │   188       │ |
|  ╚═══════════════════════════════════════╝             | │ Unchanged:  │ |
|                                                       | │    47       │ |
|                                                       | └─────────────┘ |
+───────────────────────────────────────────────────────+-----------------+
|                                      |                                  |
|  TOP 10 MOST TRADED SYMBOLS          |  SECTOR PERFORMANCE HEATMAP     |
|  ┌──────────────────────────────┐    |  ┌──────────────────────────┐   |
|  │ AAPL  ████████████████ 45M   │    |  │ Tech    +2.1%  ██████   │   |
|  │ MSFT  ██████████████ 38M     │    |  │ Health  +1.4%  ████     │   |
|  │ TSLA  ████████████ 31M       │    |  │ Financ  +0.8%  ██       │   |
|  │ AMZN  ██████████ 27M         │    |  │ Energy  -0.3%  ██       │   |
|  │ GOOGL █████████ 24M          │    |  │ Utils   -1.2%  ████     │   |
|  │ META  ████████ 22M           │    |  │ Indust  +0.5%  █        │   |
|  │ NVDA  ███████ 19M            │    |  │ Consum  +1.8%  █████    │   |
|  │ JPM   ██████ 16M             │    |  │ Materi  -0.1%  █        │   |
|  │ BAC   █████ 14M              │    |  │ Teleco  +0.3%  █        │   |
|  │ V     ████ 11M               │    |  │ RE      -0.5%  ██       │   |
|  └──────────────────────────────┘    |  └──────────────────────────┘   |
+--------------------------------------+---------------------------------+
```

**Charts & SQL:**

| Chart                          | Type                  | SQL Source View            |
|-------------------------------|-----------------------|----------------------------|
| Total Trades Today            | Big Number with Trend | `market_daily_overview`    |
| Total Volume                  | Big Number with Trend | `market_daily_overview`    |
| Daily Value Traded            | Big Number with Trend | `market_daily_overview`    |
| Market-wide Volume (30 days)  | Mixed (Bar + Line)    | `market_daily_overview`    |
| Market Breadth                | Big Number            | `market_daily_overview`    |
| Top 10 Most Traded            | Horizontal Bar        | `daily_trade_summary`      |
| Sector Performance Heatmap   | Heatmap               | `sector_daily_summary`     |

**Key SQL for "Top 10 Most Traded Symbols":**

```sql
SELECT
    s.ticker,
    s.company_name,
    d.volume,
    d.trade_count,
    d.total_value_traded
FROM daily_trade_summary d
JOIN symbol_reference s ON d.symbol = s.ticker
WHERE d.trade_date = '{{ from_dttm }}'
ORDER BY d.volume DESC
LIMIT 10
```

---

### Dashboard 2: Symbol Deep Dive

**Purpose:** Detailed view of a single symbol's trading activity. Analysts use this to
investigate price movements, volume patterns, and order flow for a specific stock. Includes
a symbol filter at the top that drives all charts via cross-filtering.

```
+─────────────────────────────────────────────────────────────────────────+
|  SYMBOL DEEP DIVE               [Symbol: AAPL ▼]  [Date Range: 90d]   |
+─────────────────────────────────────────────────────────────────────────+
|  AAPL - Apple Inc.  | Price: $178.50 | Change: +2.31% | Vol: 45.2M    |
+─────────────────────────────────────────────────────────────────────────+
|                                                                         |
|  OHLCV CANDLESTICK CHART                                               |
|  ┌─────────────────────────────────────────────────────────────────┐   |
|  │   ┃    ┃                                                        │   |
|  │   ┃    ┃╻  ╻                    ╻  ╻                            │   |
|  │   ╋    ╋┃  ┃╻   ╻              ┃  ┃   [Candlestick chart       │   |
|  │   ┃    ┃┃  ┃┃   ┃  ╻   ╻      ┃  ┃    with volume bars        │   |
|  │   ╹    ╹╹  ╹┃   ┃  ┃   ┃      ╹  ╹    overlaid below]         │   |
|  │              ╹   ╹  ╹   ╹                                       │   |
|  │  ▇▇  ▇▇  ▇  ▇▇  ▇  ▇▇  ▇▇  ▇  ▇▇  ▇  ▇▇  (Volume bars)    │   |
|  └─────────────────────────────────────────────────────────────────┘   |
|                                                                         |
+──────────────────────────────────+──────────────────────────────────────+
|                                  |                                      |
|  INTRADAY PRICE MOVEMENT         |  TRADE DISTRIBUTION (BUY vs SELL)   |
|  ┌──────────────────────────┐    |  ┌──────────────────────────────┐   |
|  │  Line chart showing      │    |  │         ╭─────╮              │   |
|  │  tick-by-tick price      │    |  │       ╭─╯ BUY ╰─╮           │   |
|  │  movement for current    │    |  │      │  52.3%    │           │   |
|  │  trading day with VWAP   │    |  │       ╰─╮     ╭─╯           │   |
|  │  overlay line            │    |  │     SELL ╰─────╯             │   |
|  │                          │    |  │      47.7%                   │   |
|  └──────────────────────────┘    |  └──────────────────────────────┘   |
|                                  |                                      |
+──────────────────────────────────+──────────────────────────────────────+
|                                  |                                      |
|  ORDER BOOK DEPTH                |  SYMBOL COMPARISON                   |
|  ┌──────────────────────────┐    |  ┌──────────────────────────────┐   |
|  │     BIDS ▏ ASKS          │    |  │  Multi-line chart comparing  │   |
|  │  ████████▏██████         │    |  │  normalized price (% change) │   |
|  │  ██████  ▏████████       │    |  │  for AAPL vs selected peers  │   |
|  │  ████    ▏██████████     │    |  │  (MSFT, GOOGL, META)         │   |
|  │  ██      ▏████████████   │    |  │                              │   |
|  │  █       ▏██████████████ │    |  │  --- AAPL  --- MSFT          │   |
|  │          ▏               │    |  │  --- GOOGL --- META          │   |
|  └──────────────────────────┘    |  └──────────────────────────────┘   |
+──────────────────────────────────+──────────────────────────────────────+
```

**Charts & SQL:**

| Chart                     | Type                    | SQL Source View            |
|---------------------------|-------------------------|----------------------------|
| Symbol Header KPIs        | Big Number (x4)         | `daily_trade_summary`      |
| OHLCV Candlestick         | Custom Plugin (ECharts) | `daily_trade_summary`      |
| Volume Bars (below candle)| Bar Chart (overlay)     | `daily_trade_summary`      |
| Intraday Price Movement   | Line Chart              | `trades_enriched`          |
| BUY/SELL Distribution     | Pie / Donut Chart       | `daily_trade_summary`      |
| Order Book Depth          | Custom Plugin           | `order_book_snapshots`     |
| Symbol Comparison         | Multi-Line Chart        | `daily_trade_summary`      |

**Key SQL for "OHLCV Candlestick":**

```sql
SELECT
    trade_date,
    open_price AS open,
    high,
    low,
    close,
    volume,
    vwap,
    (close - open_price) / open_price * 100 AS change_pct
FROM daily_trade_summary
WHERE symbol = '{{ filter_values("symbol")[0] }}'
  AND trade_date BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
ORDER BY trade_date ASC
```

**Key SQL for "Intraday Price Movement":**

```sql
SELECT
    timestamp,
    price,
    quantity,
    trade_type,
    -- Running VWAP
    SUM(price * quantity) OVER (ORDER BY timestamp) /
    SUM(quantity) OVER (ORDER BY timestamp) AS vwap
FROM trades_enriched
WHERE symbol = '{{ filter_values("symbol")[0] }}'
  AND trade_date = '{{ from_dttm }}'
ORDER BY timestamp ASC
```

---

### Dashboard 3: Trader Analytics

**Purpose:** Analyze individual and aggregate trader behavior. Used by compliance officers
and portfolio managers to understand trading patterns and performance.

```
+─────────────────────────────────────────────────────────────────────────+
|  TRADER ANALYTICS                      [Date Range]  [Account Tier ▼]  |
+─────────────────────────────────────────────────────────────────────────+
|                                                                         |
|  TOP 20 TRADERS BY VOLUME                                              |
|  ┌─────────────────────────────────────────────────────────────────┐   |
|  │ Rank │ Trader ID   │ Volume     │ Value       │ Trades │ W/L   │   |
|  │──────│─────────────│────────────│─────────────│────────│───────│   |
|  │ 1    │ T-00234     │ 12.5M      │ $1.87B      │ 4,521  │ 62%   │   |
|  │ 2    │ T-00891     │ 10.2M      │ $1.53B      │ 3,892  │ 58%   │   |
|  │ 3    │ T-00127     │  9.8M      │ $1.47B      │ 3,654  │ 71%   │   |
|  │ ...  │ ...         │ ...        │ ...         │ ...    │ ...   │   |
|  └─────────────────────────────────────────────────────────────────┘   |
|                                                                         |
+──────────────────────────────────+──────────────────────────────────────+
|                                  |                                      |
|  TRADER ACTIVITY TIMELINE        |  WIN/LOSS RATIO DISTRIBUTION        |
|  ┌──────────────────────────┐    |  ┌──────────────────────────────┐   |
|  │ Stacked area chart       │    |  │  Histogram of W/L ratios     │   |
|  │ showing daily trade      │    |  │  across all active traders    │   |
|  │ count per top-N traders  │    |  │                              │   |
|  │ over the selected period │    |  │    ▇                         │   |
|  │                          │    |  │   ▇▇▇                        │   |
|  │                          │    |  │  ▇▇▇▇▇▇                     │   |
|  │                          │    |  │ ▇▇▇▇▇▇▇▇▇                  │   |
|  └──────────────────────────┘    |  │ 30% 40% 50% 60% 70% 80%    │   |
|                                  |  └──────────────────────────────┘   |
+──────────────────────────────────+──────────────────────────────────────+
|                                  |                                      |
|  ACCOUNT TIER DISTRIBUTION       |  TRADING PATTERN (TIME HEATMAP)     |
|  ┌──────────────────────────┐    |  ┌──────────────────────────────┐   |
|  │                          │    |  │  Hour-of-day vs day-of-week  │   |
|  │   Institutional  ▓▓▓▓   │    |  │  heatmap of trade volume     │   |
|  │   35%                    │    |  │                              │   |
|  │   Premium        ▓▓▓    │    |  │  Mon ████░░████████░░████    │   |
|  │   28%                    │    |  │  Tue ████░░████████░░████    │   |
|  │   Standard       ▓▓     │    |  │  Wed ████░░████████░░████    │   |
|  │   22%                    │    |  │  Thu ████░░████████░░████    │   |
|  │   Basic          ▓      │    |  │  Fri ████░░██████░░░░████    │   |
|  │   15%                    │    |  │      9am  11am  1pm  3pm    │   |
|  └──────────────────────────┘    |  └──────────────────────────────┘   |
+──────────────────────────────────+──────────────────────────────────────+
```

**Charts & SQL:**

| Chart                       | Type                 | SQL Source View                  |
|-----------------------------|----------------------|----------------------------------|
| Top 20 Traders Table        | Table                | `trader_performance_metrics`     |
| Trader Activity Timeline    | Stacked Area Chart   | `trades_enriched` (aggregated)   |
| Win/Loss Ratio Distribution | Histogram            | `trader_performance_metrics`     |
| Account Tier Distribution   | Pie / Donut Chart    | `trader_performance_metrics`     |
| Trading Time Heatmap        | Heatmap (Cal)        | `trades_enriched` (aggregated)   |

**Key SQL for "Top 20 Traders by Volume":**

```sql
SELECT
    trader_id,
    total_trades,
    total_volume,
    total_value,
    ROUND(win_rate * 100, 1) AS win_loss_pct,
    unique_symbols,
    most_traded_symbol,
    active_days
FROM trader_performance_metrics
WHERE last_trade >= '{{ from_dttm }}'
ORDER BY total_volume DESC
LIMIT 20
```

**Key SQL for "Trading Time Heatmap":**

```sql
SELECT
    EXTRACT(DOW FROM timestamp) AS day_of_week,
    EXTRACT(HOUR FROM timestamp) AS hour_of_day,
    COUNT(*) AS trade_count,
    SUM(quantity) AS total_volume
FROM trades_enriched
WHERE trade_date BETWEEN '{{ from_dttm }}' AND '{{ to_dttm }}'
GROUP BY 1, 2
ORDER BY 1, 2
```

---

### Dashboard 4: Data Pipeline Health

**Purpose:** Monitor the health and performance of the data pipeline. Used by data engineers
to track data freshness, processing latency, quality scores, and volume anomalies.

```
+─────────────────────────────────────────────────────────────────────────+
|  DATA PIPELINE HEALTH                          [Auto-refresh: 1 min]   |
+─────────────────────────────────────────────────────────────────────────+
|                          |                          |                   |
|  PIPELINE STATUS         |  DATA FRESHNESS          | QUALITY SCORE    |
|  ┌────────────────┐      |  ┌────────────────┐      | ┌──────────────┐ |
|  │ ● Bronze: OK   │      |  │ Bronze: 2m ago │      | │              │ |
|  │ ● Silver: OK   │      |  │ Silver: 5m ago │      | │   98.7%      │ |
|  │ ● Gold:   OK   │      |  │ Gold:   8m ago │      | │   ▲ 0.2%    │ |
|  │ ● Kafka:  OK   │      |  │ Reports: 15m   │      | │              │ |
|  └────────────────┘      |  └────────────────┘      | └──────────────┘ |
|                          |                          |                   |
+──────────────────────────+──────────────────────────+-------------------+
|                                                                         |
|  QUALITY SCORE TRENDS (30 Days)                                        |
|  ┌─────────────────────────────────────────────────────────────────┐   |
|  │  Line chart showing daily quality scores per layer              │   |
|  │  (Bronze completeness, Silver validity, Gold accuracy)          │   |
|  │                                                                  │   |
|  │  --- Bronze  --- Silver  --- Gold                                │   |
|  │  Threshold line at 95% (red dashed)                              │   |
|  └─────────────────────────────────────────────────────────────────┘   |
|                                                                         |
+──────────────────────────────────+──────────────────────────────────────+
|                                  |                                      |
|  VOLUME ANOMALIES                |  PROCESSING LATENCY                  |
|  ┌──────────────────────────┐    |  ┌──────────────────────────────┐   |
|  │ Bar chart of hourly      │    |  │ Line chart of end-to-end     │   |
|  │ record counts with       │    |  │ processing time per batch:   │   |
|  │ anomaly markers (red     │    |  │                              │   |
|  │ dots) when volume        │    |  │ Ingest->Bronze:    12s       │   |
|  │ deviates >2 std devs     │    |  │ Bronze->Silver:    45s       │   |
|  │ from moving average      │    |  │ Silver->Gold:      90s       │   |
|  │                          │    |  │ Total:            147s       │   |
|  └──────────────────────────┘    |  └──────────────────────────────┘   |
+──────────────────────────────────+──────────────────────────────────────+
|                                                                         |
|  RECENT PIPELINE RUNS                                                   |
|  ┌─────────────────────────────────────────────────────────────────┐   |
|  │ Time       │ DAG         │ Status │ Duration │ Records │ Errors │   |
|  │────────────│─────────────│────────│──────────│─────────│────────│   |
|  │ 10:30 AM   │ bronze_load │ ✓ OK   │ 12s      │ 45,231  │ 0      │   |
|  │ 10:30 AM   │ silver_tfm  │ ✓ OK   │ 45s      │ 44,987  │ 244    │   |
|  │ 10:25 AM   │ gold_agg    │ ✓ OK   │ 90s      │ 547     │ 0      │   |
|  │ 10:15 AM   │ quality_chk │ ✓ OK   │ 8s       │ N/A     │ 0      │   |
|  │ 10:00 AM   │ bronze_load │ ✗ FAIL │ 3s       │ 0       │ 1      │   |
|  └─────────────────────────────────────────────────────────────────┘   |
+─────────────────────────────────────────────────────────────────────────+
```

**Charts & SQL:**

| Chart                       | Type                 | SQL Source                      |
|-----------------------------|----------------------|---------------------------------|
| Pipeline Status             | Custom (traffic light)| PostgreSQL: `airflow.dag_run`  |
| Data Freshness              | Big Number (x4)      | PostgreSQL: `airflow.dag_run`   |
| Quality Score (Big Number)  | Big Number with Trend| PostgreSQL: `quality_metrics`   |
| Quality Score Trends        | Multi-Line Chart     | PostgreSQL: `quality_metrics`   |
| Volume Anomalies            | Bar + Scatter        | `market_daily_overview`         |
| Processing Latency          | Multi-Line Chart     | PostgreSQL: `airflow.task_inst` |
| Recent Pipeline Runs        | Table                | PostgreSQL: `airflow.dag_run`   |

**Key SQL for "Pipeline Status":**

```sql
SELECT
    dag_id,
    state,
    execution_date,
    start_date,
    end_date,
    EXTRACT(EPOCH FROM (end_date - start_date)) AS duration_seconds
FROM airflow.dag_run
WHERE execution_date >= NOW() - INTERVAL '24 hours'
ORDER BY execution_date DESC
LIMIT 50
```

**Key SQL for "Volume Anomalies":**

```sql
WITH hourly_volumes AS (
    SELECT
        DATE_TRUNC('hour', timestamp) AS hour,
        COUNT(*) AS trade_count,
        AVG(COUNT(*)) OVER (
            ORDER BY DATE_TRUNC('hour', timestamp)
            ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
        ) AS moving_avg,
        STDDEV(COUNT(*)) OVER (
            ORDER BY DATE_TRUNC('hour', timestamp)
            ROWS BETWEEN 24 PRECEDING AND 1 PRECEDING
        ) AS moving_stddev
    FROM trades_enriched
    WHERE trade_date >= CURRENT_DATE - INTERVAL '7 days'
    GROUP BY 1
)
SELECT
    hour,
    trade_count,
    moving_avg,
    CASE
        WHEN ABS(trade_count - moving_avg) > 2 * moving_stddev
        THEN 'ANOMALY'
        ELSE 'NORMAL'
    END AS status
FROM hourly_volumes
ORDER BY hour DESC
```

---

### Dashboard 5: Risk & Compliance

**Purpose:** Identify potentially suspicious trading activity, concentration risk, and
regulatory compliance issues. Used by compliance officers with restricted access.

```
+─────────────────────────────────────────────────────────────────────────+
|  RISK & COMPLIANCE                     [Date Range]  [Alert Level ▼]   |
+─────────────────────────────────────────────────────────────────────────+
|                          |                          |                   |
|  ACTIVE ALERTS           |  LARGE TRADE COUNT       |  COMPLIANCE      |
|  ┌────────────────┐      |  ┌────────────────┐      |  SCORE           |
|  │                │      |  │                │      |  ┌──────────────┐|
|  │      17        │      |  │      342       │      |  │    94.2%     │|
|  │   ▲ 5 new      │      |  │  trades > $1M  │      |  │   ▼ 1.3%    │|
|  │                │      |  │                │      |  │              │|
|  └────────────────┘      |  └────────────────┘      |  └──────────────┘|
|                          |                          |                   |
+──────────────────────────+──────────────────────────+-------------------+
|                                                                         |
|  LARGE TRADE ALERTS (> $1M or > 10K shares)                            |
|  ┌─────────────────────────────────────────────────────────────────┐   |
|  │ Time     │ Trader  │ Symbol │ Type │ Qty     │ Value     │ Flag │   |
|  │──────────│─────────│────────│──────│─────────│───────────│──────│   |
|  │ 10:32 AM │ T-00234 │ AAPL   │ SELL │ 50,000  │ $8.9M     │ ⚠ HI │   |
|  │ 10:28 AM │ T-00891 │ TSLA   │ BUY  │ 25,000  │ $6.2M     │ ⚠ HI │   |
|  │ 10:15 AM │ T-00127 │ NVDA   │ SELL │ 15,000  │ $3.8M     │ MED  │   |
|  │ 10:02 AM │ T-00456 │ MSFT   │ BUY  │ 12,000  │ $2.1M     │ LOW  │   |
|  └─────────────────────────────────────────────────────────────────┘   |
|                                                                         |
+──────────────────────────────────+──────────────────────────────────────+
|                                  |                                      |
|  UNUSUAL TRADING PATTERNS        |  CROSS-ACCOUNT ACTIVITY             |
|  ┌──────────────────────────┐    |  ┌──────────────────────────────┐   |
|  │ Scatter plot:            │    |  │ Network graph or table       │   |
|  │ X = avg daily volume     │    |  │ showing accounts that trade  │   |
|  │ Y = today's volume       │    |  │ the same symbols in close    │   |
|  │ Size = trade value        │    |  │ time proximity (potential    │   |
|  │ Color = deviation level   │    |  │ coordination or wash trades) │   |
|  │                          │    |  │                              │   |
|  │ Outliers flagged in red  │    |  │  T-001 ─── AAPL ─── T-045   │   |
|  │                          │    |  │  T-001 ─── MSFT ─── T-045   │   |
|  └──────────────────────────┘    |  └──────────────────────────────┘   |
|                                  |                                      |
+──────────────────────────────────+──────────────────────────────────────+
|                                                                         |
|  CONCENTRATION RISK BY SYMBOL/SECTOR                                   |
|  ┌─────────────────────────────────────────────────────────────────┐   |
|  │  Treemap visualization showing portfolio concentration:         │   |
|  │  - Size = total position value                                  │   |
|  │  - Color = concentration risk (green=low, red=high)             │   |
|  │                                                                  │   |
|  │  ┌──────────────┬────────┬──────────────┬──────────────────┐    │   |
|  │  │              │        │              │                  │    │   |
|  │  │  Technology  │ Health │  Financials  │    Energy        │    │   |
|  │  │  42.1%       │ 18.3%  │  15.7%       │    12.4%         │    │   |
|  │  │  (HIGH RISK) │        │              │                  │    │   |
|  │  └──────────────┴────────┴──────────────┴──────────────────┘    │   |
|  └─────────────────────────────────────────────────────────────────┘   |
+─────────────────────────────────────────────────────────────────────────+
```

**Charts & SQL:**

| Chart                        | Type                   | SQL Source View                |
|------------------------------|------------------------|--------------------------------|
| Active Alerts                | Big Number             | PostgreSQL: `compliance_alerts`|
| Large Trade Count            | Big Number             | `trades_enriched`              |
| Compliance Score             | Big Number with Trend  | PostgreSQL: `compliance_score` |
| Large Trade Alerts Table     | Table (filterable)     | `trades_enriched`              |
| Unusual Trading Patterns     | Scatter Plot           | `trader_performance_metrics`   |
| Cross-Account Activity       | Table / Network        | `trades_enriched` (self-join)  |
| Concentration Risk Treemap   | Treemap                | `portfolio_positions`          |

**Key SQL for "Large Trade Alerts":**

```sql
SELECT
    timestamp,
    trader_id,
    symbol,
    trade_type,
    quantity,
    price,
    total_value,
    CASE
        WHEN total_value > 5000000 THEN 'HIGH'
        WHEN total_value > 1000000 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS alert_level,
    is_institutional
FROM trades_enriched
WHERE trade_date = CURRENT_DATE
  AND (total_value > 1000000 OR quantity > 10000)
ORDER BY total_value DESC
```

**Key SQL for "Cross-Account Activity" (potential wash trading):**

```sql
WITH trade_pairs AS (
    SELECT
        t1.trader_id AS trader_1,
        t2.trader_id AS trader_2,
        t1.symbol,
        t1.trade_type AS type_1,
        t2.trade_type AS type_2,
        t1.timestamp AS time_1,
        t2.timestamp AS time_2,
        ABS(EXTRACT(EPOCH FROM (t1.timestamp - t2.timestamp))) AS time_diff_seconds,
        t1.quantity AS qty_1,
        t2.quantity AS qty_2
    FROM trades_enriched t1
    JOIN trades_enriched t2
        ON t1.symbol = t2.symbol
        AND t1.trader_id != t2.trader_id
        AND t1.trade_type != t2.trade_type               -- opposite sides
        AND ABS(t1.quantity - t2.quantity) < t1.quantity * 0.05  -- similar size
        AND ABS(EXTRACT(EPOCH FROM (t1.timestamp - t2.timestamp))) < 30  -- within 30s
    WHERE t1.trade_date = CURRENT_DATE
)
SELECT
    trader_1,
    trader_2,
    symbol,
    COUNT(*) AS paired_trades,
    AVG(time_diff_seconds) AS avg_time_gap_sec
FROM trade_pairs
GROUP BY 1, 2, 3
HAVING COUNT(*) >= 3
ORDER BY paired_trades DESC
```

---

## 4. Custom Visualizations

### 4.1 Candlestick Chart Plugin (ECharts)

Superset does not include a native candlestick chart. We build a custom plugin using
Superset's ECharts integration framework.

```
services/superset/custom_plugins/
|-- plugin-chart-candlestick/
    |-- package.json
    |-- src/
    |   |-- index.ts
    |   |-- plugin/
    |   |   |-- controlPanel.ts          # Chart configuration UI
    |   |   |-- transformProps.ts         # Data transformation
    |   |   |-- EchartsCandlestick.tsx    # React component
    |   |   |-- buildQuery.ts            # Custom SQL query builder
    |   |-- types.ts
    |-- tsconfig.json
```

**Key Implementation:**

```typescript
// services/superset/custom_plugins/plugin-chart-candlestick/src/plugin/transformProps.ts

export default function transformProps(chartProps: ChartProps) {
  const { queriesData, formData } = chartProps;
  const data = queriesData[0].data;

  const candlestickData = data.map((row: any) => ({
    date: row[formData.dateColumn],
    open: row[formData.openColumn],
    close: row[formData.closeColumn],
    high: row[formData.highColumn],
    low: row[formData.lowColumn],
    volume: row[formData.volumeColumn],
  }));

  return {
    echartOptions: {
      xAxis: { type: 'category', data: candlestickData.map(d => d.date) },
      yAxis: [
        { type: 'value', name: 'Price' },
        { type: 'value', name: 'Volume', position: 'right' },
      ],
      series: [
        {
          type: 'candlestick',
          data: candlestickData.map(d => [d.open, d.close, d.low, d.high]),
          itemStyle: {
            color: '#26a69a',        // bullish (green)
            color0: '#ef5350',       // bearish (red)
            borderColor: '#26a69a',
            borderColor0: '#ef5350',
          },
        },
        {
          type: 'bar',
          yAxisIndex: 1,
          data: candlestickData.map(d => d.volume),
          itemStyle: { color: '#90caf9', opacity: 0.5 },
        },
      ],
      tooltip: { trigger: 'axis' },
      dataZoom: [{ type: 'slider', start: 70, end: 100 }],
    },
  };
}
```

### 4.2 Order Book Depth Chart Plugin

```typescript
// Custom depth chart showing bid/ask levels as stacked area
// Bids stack from right to left (descending price), asks stack left to right

// services/superset/custom_plugins/plugin-chart-depth/src/plugin/transformProps.ts

export default function transformProps(chartProps: ChartProps) {
  const data = chartProps.queriesData[0].data;

  const bids = data
    .filter((r: any) => r.side === 'BID')
    .sort((a: any, b: any) => b.price - a.price);
  const asks = data
    .filter((r: any) => r.side === 'ASK')
    .sort((a: any, b: any) => a.price - b.price);

  // Cumulative volume
  let cumBid = 0;
  const bidCumulative = bids.map((b: any) => {
    cumBid += b.quantity;
    return { price: b.price, cumVolume: cumBid };
  });

  let cumAsk = 0;
  const askCumulative = asks.map((a: any) => {
    cumAsk += a.quantity;
    return { price: a.price, cumVolume: cumAsk };
  });

  return {
    echartOptions: {
      xAxis: { type: 'value', name: 'Price' },
      yAxis: { type: 'value', name: 'Cumulative Volume' },
      series: [
        {
          name: 'Bids',
          type: 'line',
          areaStyle: { color: 'rgba(38, 166, 154, 0.3)' },
          lineStyle: { color: '#26a69a' },
          data: bidCumulative.map((b: any) => [b.price, b.cumVolume]),
        },
        {
          name: 'Asks',
          type: 'line',
          areaStyle: { color: 'rgba(239, 83, 80, 0.3)' },
          lineStyle: { color: '#ef5350' },
          data: askCumulative.map((a: any) => [a.price, a.cumVolume]),
        },
      ],
    },
  };
}
```

### 4.3 Plugin Registration

```python
# services/superset/superset_config.py (continued)

# Custom plugin registration
EXTRA_CHART_PLUGINS = [
    {
        "key": "echarts_candlestick",
        "name": "ECharts Candlestick",
        "package": "@trade-platform/plugin-chart-candlestick",
    },
    {
        "key": "echarts_depth",
        "name": "Order Book Depth",
        "package": "@trade-platform/plugin-chart-depth",
    },
]
```

---

## 5. Saved Queries & SQL Lab

### 5.1 Pre-built Saved Queries for Analysts

These queries are saved in Superset's SQL Lab and shared with the "Analyst" role. They serve
as starting points for ad-hoc exploration.

**Query 1: Symbol OHLCV with Technical Indicators**

```sql
-- Saved Query: Symbol OHLCV with Moving Averages
-- Description: Daily OHLCV data with 20-day and 50-day SMA, RSI(14)
-- Tags: technical-analysis, daily, ohlcv

WITH daily AS (
    SELECT
        trade_date,
        symbol,
        open_price,
        high,
        low,
        close,
        volume,
        vwap,
        -- Simple Moving Averages
        AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS sma_20,
        AVG(close) OVER (
            PARTITION BY symbol
            ORDER BY trade_date
            ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
        ) AS sma_50,
        -- Price change for RSI
        close - LAG(close) OVER (PARTITION BY symbol ORDER BY trade_date) AS price_change
    FROM daily_trade_summary
    WHERE symbol = '{{ symbol }}'   -- Jinja template parameter
      AND trade_date >= CURRENT_DATE - INTERVAL '180 days'
),
rsi_calc AS (
    SELECT
        *,
        AVG(CASE WHEN price_change > 0 THEN price_change ELSE 0 END) OVER (
            ORDER BY trade_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_gain,
        AVG(CASE WHEN price_change < 0 THEN ABS(price_change) ELSE 0 END) OVER (
            ORDER BY trade_date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS avg_loss
    FROM daily
)
SELECT
    trade_date,
    symbol,
    open_price,
    high,
    low,
    close,
    volume,
    vwap,
    ROUND(sma_20, 2) AS sma_20,
    ROUND(sma_50, 2) AS sma_50,
    ROUND(100 - (100 / (1 + avg_gain / NULLIF(avg_loss, 0))), 2) AS rsi_14
FROM rsi_calc
ORDER BY trade_date DESC;
```

**Query 2: Unusual Volume Detection**

```sql
-- Saved Query: Unusual Volume Detection
-- Description: Identifies symbols with volume > 2x their 20-day average
-- Tags: anomaly, volume, screening

WITH vol_stats AS (
    SELECT
        symbol,
        trade_date,
        volume,
        AVG(volume) OVER (
            PARTITION BY symbol
            ORDER BY trade_date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS avg_volume_20d,
        STDDEV(volume) OVER (
            PARTITION BY symbol
            ORDER BY trade_date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS stddev_volume_20d
    FROM daily_trade_summary
    WHERE trade_date >= CURRENT_DATE - INTERVAL '30 days'
)
SELECT
    v.symbol,
    s.company_name,
    s.sector,
    v.trade_date,
    v.volume,
    ROUND(v.avg_volume_20d) AS avg_vol_20d,
    ROUND(v.volume / NULLIF(v.avg_volume_20d, 0), 2) AS volume_ratio,
    ROUND((v.volume - v.avg_volume_20d) / NULLIF(v.stddev_volume_20d, 0), 2) AS z_score
FROM vol_stats v
JOIN symbol_reference s ON v.symbol = s.ticker
WHERE v.trade_date = CURRENT_DATE
  AND v.volume > 2 * v.avg_volume_20d
ORDER BY volume_ratio DESC;
```

**Query 3: Sector Rotation Analysis**

```sql
-- Saved Query: Sector Rotation (weekly flows)
-- Description: Shows net buying/selling pressure per sector on a weekly basis
-- Tags: sector, rotation, flow-analysis

SELECT
    DATE_TRUNC('week', d.trade_date) AS week,
    s.sector,
    SUM(CASE WHEN d.buy_volume > d.sell_volume THEN d.total_value_traded ELSE 0 END) AS net_inflow,
    SUM(CASE WHEN d.sell_volume > d.buy_volume THEN d.total_value_traded ELSE 0 END) AS net_outflow,
    SUM(d.buy_volume - d.sell_volume) AS net_volume_flow,
    COUNT(DISTINCT d.symbol) AS active_symbols,
    ROUND(AVG((d.close - d.open_price) / NULLIF(d.open_price, 0) * 100), 2) AS avg_return_pct
FROM daily_trade_summary d
JOIN symbol_reference s ON d.symbol = s.ticker
WHERE d.trade_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY 1, 2
ORDER BY 1 DESC, net_volume_flow DESC;
```

**Query 4: Trader Profiling**

```sql
-- Saved Query: Trader Profile Deep Dive
-- Description: Comprehensive profile for a specific trader
-- Tags: trader, profiling, compliance

WITH trader_trades AS (
    SELECT *
    FROM trades_enriched
    WHERE trader_id = '{{ trader_id }}'
      AND trade_date BETWEEN '{{ start_date }}' AND '{{ end_date }}'
),
symbol_breakdown AS (
    SELECT
        symbol,
        COUNT(*) AS trades,
        SUM(quantity) AS total_qty,
        SUM(total_value) AS total_val,
        COUNT(CASE WHEN trade_type = 'BUY' THEN 1 END) AS buy_count,
        COUNT(CASE WHEN trade_type = 'SELL' THEN 1 END) AS sell_count,
        AVG(price) AS avg_price
    FROM trader_trades
    GROUP BY symbol
),
time_analysis AS (
    SELECT
        EXTRACT(HOUR FROM timestamp) AS hour,
        COUNT(*) AS trade_count,
        SUM(total_value) AS hour_value
    FROM trader_trades
    GROUP BY 1
)
SELECT
    '{{ trader_id }}' AS trader_id,
    (SELECT COUNT(*) FROM trader_trades) AS total_trades,
    (SELECT SUM(total_value) FROM trader_trades) AS total_value,
    (SELECT COUNT(DISTINCT symbol) FROM trader_trades) AS unique_symbols,
    (SELECT COUNT(DISTINCT trade_date) FROM trader_trades) AS active_days,
    (SELECT symbol FROM symbol_breakdown ORDER BY total_val DESC LIMIT 1) AS top_symbol,
    (SELECT hour FROM time_analysis ORDER BY trade_count DESC LIMIT 1) AS most_active_hour;
```

### 5.2 SQL Lab Configuration

```python
# services/superset/superset_config.py (continued)

# SQL Lab specific settings
SQLLAB_TIMEOUT = 300                         # 5 minute query timeout
SQL_MAX_ROW = 100000                         # Max rows returned
SQLLAB_CTAS_NO_LIMIT = True                  # Allow CREATE TABLE AS with no row limit
DEFAULT_SQLLAB_LIMIT = 1000                  # Default LIMIT clause
SQL_VALIDATORS_BY_ENGINE = {
    "duckdb": "PrestoDBSQLValidator",        # Syntax validation
}
SQLLAB_DEFAULT_DBID = 1                      # Default to DuckDB connection

# Jinja templating in SQL Lab
ENABLE_TEMPLATE_PROCESSING = True
ALLOWED_USER_CSV_SCHEMA_FUNC = lambda db, schema: True  # Allow CSV uploads

# Query cost estimation
ESTIMATE_QUERY_COST = True
QUERY_COST_FORMATTERS_BY_ENGINE = {
    "duckdb": lambda result: f"Estimated rows: {result.get('rows', 'N/A')}",
}
```

---

## 6. Alerts & Reports

### 6.1 Scheduled Email Reports

| Report Name                   | Schedule        | Recipients              | Content                          |
|-------------------------------|-----------------|-------------------------|----------------------------------|
| Daily Market Summary          | 5:00 PM ET      | executives@company.com  | Dashboard 1 PDF snapshot         |
| Weekly Trader Leaderboard     | Monday 8:00 AM  | trading-desk@company.com| Top 50 traders table             |
| Daily Data Quality Report     | 6:00 AM ET      | data-eng@company.com    | Dashboard 4 + quality scores     |
| Monthly Compliance Summary    | 1st of month    | compliance@company.com  | Dashboard 5 PDF + CSV of alerts  |

**Report Configuration:**

```python
# services/superset/superset_config.py (continued)

# Email configuration for reports
SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.company.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SMTP_STARTTLS = True
SMTP_SSL = False
SMTP_USER = os.environ.get("SMTP_USER")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD")
SMTP_MAIL_FROM = "trade-analytics@company.com"

# Screenshot generation for PDF reports
WEBDRIVER_TYPE = "chrome"
WEBDRIVER_BASEURL = "http://superset:8088/"
WEBDRIVER_OPTION_ARGS = [
    "--headless",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--window-size=1600,1200",
]

# Alert/Report schedules stored in Superset metadata DB
ALERT_REPORTS_NOTIFICATION_DRY_RUN = False
ALERT_REPORTS_QUERY_EXECUTION_MAX_TRIES = 3
```

### 6.2 Threshold-Based Alerts

| Alert Name                        | Condition                                     | Channel  |
|-----------------------------------|-----------------------------------------------|----------|
| High Volume Alert                 | Any symbol volume > 3x 20-day avg             | Slack    |
| Large Trade Alert                 | Trade value > $5M                             | Email    |
| Data Freshness Alert              | No new data in Gold layer for > 30 min        | PagerDuty|
| Quality Score Drop                | Quality score drops below 95%                 | Slack    |
| Pipeline Failure                  | Any Airflow DAG fails                         | PagerDuty|
| Market Anomaly                    | >10% of symbols move >5% in same direction    | Email    |

**Alert SQL Example (High Volume):**

```sql
-- Alert: High Volume Detection
-- Trigger: When any symbol has volume > 3x its 20-day average
-- Checked every: 5 minutes

SELECT
    symbol,
    volume,
    avg_volume_20d,
    ROUND(volume / NULLIF(avg_volume_20d, 0), 2) AS volume_ratio
FROM (
    SELECT
        symbol,
        volume,
        AVG(volume) OVER (
            PARTITION BY symbol
            ORDER BY trade_date
            ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
        ) AS avg_volume_20d
    FROM daily_trade_summary
    WHERE trade_date = CURRENT_DATE
) sub
WHERE volume > 3 * avg_volume_20d
ORDER BY volume_ratio DESC
LIMIT 10
```

**Slack Webhook Integration:**

```python
# services/superset/superset_config.py (continued)

SLACK_API_TOKEN = os.environ.get("SLACK_API_TOKEN")
SLACK_PROXY = None

# Alert channels
ALERT_SLACK_CHANNELS = {
    "high-volume": "#trading-alerts",
    "data-quality": "#data-engineering",
    "pipeline-failure": "#data-engineering-critical",
    "compliance": "#compliance-alerts",
}
```

---

## 7. Embedding

### 7.1 Embedded Dashboard Configuration

Superset supports embedding dashboards in external applications via the Embedded SDK or
iframe-based embedding with guest tokens.

```python
# services/superset/superset_config.py (continued)

# Enable embedded dashboards
FEATURE_FLAGS = {
    **FEATURE_FLAGS,
    "EMBEDDED_SUPERSET": True,
}

# Guest token configuration
GUEST_ROLE_NAME = "EmbeddedViewer"
GUEST_TOKEN_JWT_SECRET = os.environ.get("GUEST_TOKEN_SECRET")
GUEST_TOKEN_JWT_EXP_SECONDS = 3600    # 1 hour
GUEST_TOKEN_HEADER_NAME = "X-GuestToken"

# CORS for embedding
ENABLE_CORS = True
CORS_OPTIONS = {
    "origins": [
        "https://trading-platform.company.com",
        "https://dashboard.company.com",
        "http://localhost:3000",          # Development
    ],
    "supports_credentials": True,
}

# Content Security Policy for iframe embedding
TALISMAN_ENABLED = True
TALISMAN_CONFIG = {
    "content_security_policy": {
        "frame-ancestors": [
            "'self'",
            "https://trading-platform.company.com",
            "https://dashboard.company.com",
        ],
    },
}
```

### 7.2 Embedding via Superset Embedded SDK

```javascript
// External application integration example
// File: trading-platform/src/components/EmbeddedDashboard.tsx

import { embedDashboard } from "@superset-ui/embedded-sdk";

async function embedMarketOverview(containerId: string) {
  const guestToken = await fetchGuestToken();  // From your backend

  embedDashboard({
    id: "market-overview-dashboard-uuid",      // Dashboard UUID from Superset
    supersetDomain: "https://analytics.company.com",
    mountPoint: document.getElementById(containerId)!,
    fetchGuestToken: () => guestToken,
    dashboardUiConfig: {
      hideTitle: true,
      hideChartControls: false,
      hideTab: false,
      filters: {
        visible: true,
        expanded: false,
      },
    },
  });
}
```

### 7.3 Guest Token API (Backend)

```python
# trading-platform/backend/superset_embed.py

import requests
from datetime import datetime, timedelta

class SupersetEmbedService:
    def __init__(self, superset_url: str, admin_username: str, admin_password: str):
        self.superset_url = superset_url
        self.session = self._login(admin_username, admin_password)

    def get_guest_token(
        self, dashboard_id: str, user: dict, filters: list = None
    ) -> str:
        """Generate a guest token for embedding a specific dashboard."""
        payload = {
            "user": {
                "username": user["email"],
                "first_name": user["first_name"],
                "last_name": user["last_name"],
            },
            "resources": [
                {"type": "dashboard", "id": dashboard_id}
            ],
            "rls": filters or [],   # Row-level security filters
        }

        response = self.session.post(
            f"{self.superset_url}/api/v1/security/guest_token/",
            json=payload,
        )
        return response.json()["token"]
```

**Dashboard Embedding URLs:**

| Dashboard                 | UUID                                   | Embed Context                    |
|---------------------------|----------------------------------------|----------------------------------|
| Executive Market Overview | `a1b2c3d4-e5f6-7890-abcd-ef1234567890`| Main trading platform home page  |
| Symbol Deep Dive          | `b2c3d4e5-f6a7-8901-bcde-f12345678901`| Symbol detail page (filtered)    |
| Trader Analytics          | `c3d4e5f6-a7b8-9012-cdef-123456789012`| Trader profile page (filtered)   |
| Pipeline Health           | `d4e5f6a7-b8c9-0123-defa-234567890123`| Internal ops dashboard           |
| Risk & Compliance         | `e5f6a7b8-c9d0-1234-efab-345678901234`| Compliance portal (restricted)   |

---

## 8. Implementation Steps & File Structure

```
services/superset/
|-- Dockerfile                               # Custom Superset image with plugins
|-- docker-compose.yml                       # Full stack deployment
|-- .env.example                             # Environment variable template
|-- requirements-local.txt                   # Additional Python packages
|
|-- superset_config.py                       # Main Superset configuration
|-- init_duckdb.sql                          # DuckDB/Iceberg view initialization
|
|-- bootstrap/
|   |-- __init__.py
|   |-- init_superset.py                     # Programmatic Superset initialization
|   |-- create_databases.py                  # Register DuckDB + PostgreSQL connections
|   |-- create_datasets.py                   # Register Iceberg views as datasets
|   |-- import_dashboards.py                 # Import dashboard JSON exports
|   |-- create_roles.py                      # Configure custom roles and permissions
|   |-- create_saved_queries.py              # Import pre-built SQL queries
|   |-- create_alerts.py                     # Configure alerts and report schedules
|
|-- dashboards/
|   |-- 01_executive_market_overview.json     # Dashboard 1 export
|   |-- 02_symbol_deep_dive.json             # Dashboard 2 export
|   |-- 03_trader_analytics.json             # Dashboard 3 export
|   |-- 04_pipeline_health.json              # Dashboard 4 export
|   |-- 05_risk_compliance.json              # Dashboard 5 export
|   |-- charts/
|   |   |-- market_volume_bar.json           # Individual chart configs
|   |   |-- top_symbols_horizontal_bar.json
|   |   |-- sector_heatmap.json
|   |   |-- candlestick_ohlcv.json
|   |   |-- order_book_depth.json
|   |   |-- trader_time_heatmap.json
|   |   |-- concentration_treemap.json
|   |   |-- quality_score_trend.json
|   |   |-- ... (one file per chart)
|   |-- datasets/
|       |-- daily_trade_summary.yaml         # Dataset config (columns, metrics)
|       |-- trader_performance_metrics.yaml
|       |-- symbol_reference.yaml
|       |-- trades_enriched.yaml
|       |-- market_daily_overview.yaml
|       |-- sector_daily_summary.yaml
|
|-- custom_plugins/
|   |-- plugin-chart-candlestick/
|   |   |-- package.json
|   |   |-- tsconfig.json
|   |   |-- src/
|   |       |-- index.ts
|   |       |-- plugin/
|   |           |-- controlPanel.ts
|   |           |-- transformProps.ts
|   |           |-- EchartsCandlestick.tsx
|   |           |-- buildQuery.ts
|   |
|   |-- plugin-chart-depth/
|       |-- package.json
|       |-- tsconfig.json
|       |-- src/
|           |-- index.ts
|           |-- plugin/
|               |-- controlPanel.ts
|               |-- transformProps.ts
|               |-- EchartsDepth.tsx
|               |-- buildQuery.ts
|
|-- saved_queries/
|   |-- ohlcv_with_technicals.sql
|   |-- unusual_volume_detection.sql
|   |-- sector_rotation_analysis.sql
|   |-- trader_profile_deep_dive.sql
|   |-- cross_account_activity.sql
|   |-- daily_market_summary.sql
|
|-- tests/
|   |-- __init__.py
|   |-- conftest.py                          # Test fixtures (Superset test client)
|   |-- test_dashboards/
|   |   |-- test_dashboard_rendering.py      # Verify dashboards load without errors
|   |   |-- test_chart_queries.py            # Verify chart SQL executes correctly
|   |   |-- test_filters.py                  # Verify cross-filters work
|   |
|   |-- test_data_accuracy/
|   |   |-- test_kpi_values.py               # Verify KPI calculations
|   |   |-- test_aggregations.py             # Verify aggregation accuracy
|   |   |-- test_date_ranges.py              # Verify date filtering
|   |
|   |-- test_security/
|   |   |-- test_role_permissions.py          # Verify role-based access
|   |   |-- test_row_level_security.py       # Verify RLS rules
|   |   |-- test_guest_tokens.py             # Verify embedding auth
|   |
|   |-- test_alerts/
|       |-- test_alert_conditions.py         # Verify alert trigger SQL
|       |-- test_report_generation.py        # Verify report rendering
|
|-- scripts/
    |-- export_dashboards.sh                 # Export dashboards to JSON
    |-- import_dashboards.sh                 # Import dashboards from JSON
    |-- build_plugins.sh                     # Build custom viz plugins
    |-- seed_test_data.py                    # Seed test data for development
```

**Implementation Order:**

| Phase | Files / Tasks                                    | Description                           |
|-------|--------------------------------------------------|---------------------------------------|
| 1     | `docker-compose.yml`, `superset_config.py`       | Base deployment with PostgreSQL/Redis |
| 2     | `init_duckdb.sql`, `bootstrap/create_databases.py`| DuckDB + Iceberg connection setup    |
| 3     | `bootstrap/create_datasets.py`, `datasets/`      | Register datasets with column/metric  |
| 4     | `bootstrap/create_roles.py`                       | Role-based access configuration       |
| 5     | Dashboard 1: Executive Market Overview            | First dashboard (most value)          |
| 6     | Dashboard 4: Pipeline Health                      | Operational visibility                |
| 7     | `custom_plugins/plugin-chart-candlestick/`        | Build candlestick chart plugin        |
| 8     | Dashboard 2: Symbol Deep Dive                     | Requires candlestick plugin           |
| 9     | Dashboard 3: Trader Analytics                     | Trader analysis views                 |
| 10    | `custom_plugins/plugin-chart-depth/`              | Build order book depth plugin         |
| 11    | Dashboard 5: Risk & Compliance                    | Compliance dashboard                  |
| 12    | `saved_queries/`                                  | Import all pre-built queries          |
| 13    | `bootstrap/create_alerts.py`                      | Configure alerts and schedules        |
| 14    | Embedding setup                                   | Guest tokens, CORS, CSP              |
| 15    | `tests/**`                                        | Full test suite                       |

---

## 9. Testing Strategy

### 9.1 Dashboard Rendering Tests

Verify that all dashboards load without errors and all charts render data.

```python
# tests/test_dashboards/test_dashboard_rendering.py

import pytest
from superset.app import create_app
from superset.models.dashboard import Dashboard

DASHBOARD_SLUGS = [
    "executive-market-overview",
    "symbol-deep-dive",
    "trader-analytics",
    "pipeline-health",
    "risk-compliance",
]

@pytest.fixture
def superset_client():
    app = create_app()
    with app.test_client() as client:
        # Login as admin
        client.post("/login/", data={
            "username": "admin",
            "password": "admin",
        })
        yield client

@pytest.mark.parametrize("slug", DASHBOARD_SLUGS)
def test_dashboard_loads(superset_client, slug):
    """Each dashboard should load without HTTP errors."""
    response = superset_client.get(f"/superset/dashboard/{slug}/")
    assert response.status_code == 200

@pytest.mark.parametrize("slug", DASHBOARD_SLUGS)
def test_dashboard_has_charts(superset_client, slug):
    """Each dashboard should have at least one chart."""
    dashboard = Dashboard.query.filter_by(slug=slug).first()
    assert dashboard is not None
    assert len(dashboard.slices) > 0, f"Dashboard '{slug}' has no charts"
```

### 9.2 Chart Query Tests

Verify that the SQL queries behind each chart execute successfully.

```python
# tests/test_dashboards/test_chart_queries.py

import pytest
from superset.models.slice import Slice
from superset.connectors.sqla.models import SqlaTable

@pytest.fixture
def duckdb_engine():
    """Create a DuckDB connection for testing chart queries."""
    import duckdb
    conn = duckdb.connect(":memory:")
    # Load test data
    conn.execute("CREATE TABLE daily_trade_summary AS SELECT * FROM 'tests/fixtures/daily_summary.parquet'")
    conn.execute("CREATE TABLE trades_enriched AS SELECT * FROM 'tests/fixtures/trades.parquet'")
    conn.execute("CREATE TABLE symbol_reference AS SELECT * FROM 'tests/fixtures/symbols.parquet'")
    yield conn
    conn.close()

def test_top_symbols_query(duckdb_engine):
    """Top 10 most traded symbols query should return results."""
    result = duckdb_engine.execute("""
        SELECT symbol, SUM(volume) as total_volume
        FROM daily_trade_summary
        WHERE trade_date >= CURRENT_DATE - INTERVAL '30 days'
        GROUP BY symbol
        ORDER BY total_volume DESC
        LIMIT 10
    """).fetchall()
    assert len(result) > 0
    assert result[0][1] > 0  # Volume should be positive

def test_sector_heatmap_query(duckdb_engine):
    """Sector performance heatmap query should return valid sectors."""
    result = duckdb_engine.execute("""
        SELECT
            s.sector,
            AVG((d.close - d.open_price) / NULLIF(d.open_price, 0) * 100) AS avg_change
        FROM daily_trade_summary d
        JOIN symbol_reference s ON d.symbol = s.ticker
        WHERE d.trade_date = (SELECT MAX(trade_date) FROM daily_trade_summary)
        GROUP BY s.sector
        ORDER BY avg_change DESC
    """).fetchall()
    assert len(result) > 0
    for sector, change in result:
        assert sector is not None
        assert isinstance(change, (int, float))
```

### 9.3 Data Accuracy Verification

```python
# tests/test_data_accuracy/test_kpi_values.py

import pytest
from decimal import Decimal

def test_daily_volume_consistency(duckdb_engine):
    """
    Sum of individual trade quantities for a day should match
    the daily_trade_summary volume.
    """
    date = "2024-03-15"
    symbol = "AAPL"

    # From detailed trades
    trade_volume = duckdb_engine.execute(f"""
        SELECT SUM(quantity) FROM trades_enriched
        WHERE symbol = '{symbol}' AND trade_date = '{date}'
    """).fetchone()[0]

    # From daily summary
    summary_volume = duckdb_engine.execute(f"""
        SELECT volume FROM daily_trade_summary
        WHERE symbol = '{symbol}' AND trade_date = '{date}'
    """).fetchone()[0]

    assert trade_volume == summary_volume, (
        f"Volume mismatch for {symbol} on {date}: "
        f"trades={trade_volume}, summary={summary_volume}"
    )

def test_vwap_calculation(duckdb_engine):
    """VWAP should equal sum(price * quantity) / sum(quantity)."""
    result = duckdb_engine.execute("""
        SELECT
            SUM(price * quantity) / SUM(quantity) AS calculated_vwap,
            (SELECT vwap FROM daily_trade_summary
             WHERE symbol = 'AAPL' AND trade_date = '2024-03-15') AS stored_vwap
        FROM trades_enriched
        WHERE symbol = 'AAPL' AND trade_date = '2024-03-15'
    """).fetchone()

    calculated, stored = result
    assert abs(calculated - stored) < 0.01, (
        f"VWAP mismatch: calculated={calculated}, stored={stored}"
    )

def test_market_breadth(duckdb_engine):
    """
    Advancing + declining + unchanged should equal total active symbols.
    """
    result = duckdb_engine.execute("""
        SELECT
            advancing_symbols + declining_symbols + unchanged_symbols AS total,
            (SELECT COUNT(DISTINCT symbol) FROM daily_trade_summary
             WHERE trade_date = '2024-03-15') AS actual_symbols
        FROM market_daily_overview
        WHERE date = '2024-03-15'
    """).fetchone()

    assert result[0] == result[1], (
        f"Breadth mismatch: sum={result[0]}, actual={result[1]}"
    )
```

### 9.4 Security Tests

```python
# tests/test_security/test_role_permissions.py

import pytest

def test_viewer_cannot_access_sql_lab(superset_client_viewer):
    """Viewer role should not have SQL Lab access."""
    response = superset_client_viewer.get("/superset/sqllab/")
    assert response.status_code == 403

def test_viewer_can_access_market_overview(superset_client_viewer):
    """Viewer role should access the executive market overview dashboard."""
    response = superset_client_viewer.get(
        "/superset/dashboard/executive-market-overview/"
    )
    assert response.status_code == 200

def test_compliance_dashboard_restricted(superset_client_analyst):
    """Only compliance role should access risk dashboard."""
    response = superset_client_analyst.get(
        "/superset/dashboard/risk-compliance/"
    )
    assert response.status_code == 403

def test_row_level_security_trader(superset_client_trader):
    """Trader should only see their own portfolio positions."""
    response = superset_client_trader.post("/api/v1/chart/data", json={
        "datasource": {"type": "table", "id": 5},  # portfolio_positions dataset
        "queries": [{"columns": ["account_id", "symbol", "quantity"]}],
    })
    data = response.json()
    accounts = {row["account_id"] for row in data["result"][0]["data"]}
    assert len(accounts) == 1   # Should only see their own account
```

---

## Appendix: Environment Variables

```bash
# services/superset/.env.example

# ─── Superset Core ──────────────────────────────────
SUPERSET_SECRET_KEY=change-me-to-a-random-secret-key
SUPERSET_DB_USER=superset
SUPERSET_DB_PASSWORD=change-me
SUPERSET_ADMIN_USERNAME=admin
SUPERSET_ADMIN_PASSWORD=change-me
SUPERSET_ADMIN_EMAIL=admin@company.com

# ─── Redis ──────────────────────────────────────────
REDIS_HOST=superset-redis
REDIS_PORT=6379

# ─── OAuth (Keycloak) ──────────────────────────────
OAUTH_CLIENT_ID=superset
OAUTH_CLIENT_SECRET=change-me
KEYCLOAK_URL=https://auth.company.com

# ─── AWS (for DuckDB/Iceberg S3 access) ────────────
AWS_ACCESS_KEY_ID=change-me
AWS_SECRET_ACCESS_KEY=change-me
AWS_REGION=us-east-1

# ─── Email (for reports) ───────────────────────────
SMTP_HOST=smtp.company.com
SMTP_PORT=587
SMTP_USER=trade-analytics@company.com
SMTP_PASSWORD=change-me

# ─── Slack (for alerts) ────────────────────────────
SLACK_API_TOKEN=xoxb-change-me

# ─── Embedding ─────────────────────────────────────
GUEST_TOKEN_SECRET=change-me-to-a-random-secret
```
