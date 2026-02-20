"""superset_config.py
Apache Superset configuration for the DE Capstone trading analytics platform.

Configured for:
  - PostgreSQL metadata store
  - Redis cache + Celery broker
  - DuckDB/Iceberg data source
  - Role-based access control (Viewer / Analyst / Admin)
  - Embedded dashboards (guest tokens)
  - Scheduled email reports + Slack alerts
"""

import os
from datetime import timedelta

# ─── General ──────────────────────────────────────────────────────────────────

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "CHANGE_ME_IN_PRODUCTION")
APP_NAME = "Trade Data Analytics"
APP_ICON = "/static/assets/images/superset-logo-horiz.png"

ROW_LIMIT = 5000
SUPERSET_WEBSERVER_TIMEOUT = 300

# ─── Metadata Database ────────────────────────────────────────────────────────

SQLALCHEMY_DATABASE_URI = (
    f"postgresql://{os.environ.get('SUPERSET_DB_USER', 'superset')}:"
    f"{os.environ.get('SUPERSET_DB_PASSWORD', 'superset_password')}@"
    f"superset-db:5432/superset"
)

# ─── Redis / Cache ────────────────────────────────────────────────────────────

REDIS_HOST = os.environ.get("REDIS_HOST", "superset-redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 0,
}

DATA_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 600,
    "CACHE_KEY_PREFIX": "superset_data_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 1,
}

FILTER_STATE_CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 86400,
    "CACHE_KEY_PREFIX": "superset_filter_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": 2,
}

# ─── Celery (async queries + scheduled reports) ───────────────────────────────


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

# ─── Feature Flags ────────────────────────────────────────────────────────────

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

# ─── SQL Lab ──────────────────────────────────────────────────────────────────

SQL_MAX_ROW = 100000
SQLLAB_TIMEOUT = 300
SQLLAB_DEFAULT_DBID = 1
SQLLAB_ASYNC_TIME_LIMIT_SEC = 600
SQLLAB_CTAS_NO_LIMIT = True
DEFAULT_SQLLAB_LIMIT = 1000

ENABLE_TEMPLATE_PROCESSING = True

SQL_VALIDATORS_BY_ENGINE = {
    "duckdb": "PrestoDBSQLValidator",
}

# ─── Row-Level Security ───────────────────────────────────────────────────────

ENABLE_ROW_LEVEL_SECURITY = True

# ─── Authentication (basic for local dev; swap for OAuth in production) ───────

from flask_appbuilder.security.manager import AUTH_DB  # noqa: E402

AUTH_TYPE = AUTH_DB
AUTH_USER_REGISTRATION = False

# ─── Custom Roles ─────────────────────────────────────────────────────────────

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

# ─── Thumbnail generation ─────────────────────────────────────────────────────

THUMBNAILS = True
THUMBNAIL_SELENIUM_USER = "admin"

WEBDRIVER_TYPE = "chrome"
WEBDRIVER_BASEURL = "http://superset:8088/"
WEBDRIVER_OPTION_ARGS = [
    "--headless",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--window-size=1600,1200",
]

# ─── Scheduled Email Reports ─────────────────────────────────────────────────

SMTP_HOST = os.environ.get("SMTP_HOST", "smtp.example.com")
SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
SMTP_STARTTLS = True
SMTP_SSL = False
SMTP_USER = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
SMTP_MAIL_FROM = os.environ.get("SMTP_MAIL_FROM", "trade-analytics@example.com")

ALERT_REPORTS_NOTIFICATION_DRY_RUN = False
ALERT_REPORTS_QUERY_EXECUTION_MAX_TRIES = 3

# ─── Slack Integration ────────────────────────────────────────────────────────

SLACK_API_TOKEN = os.environ.get("SLACK_API_TOKEN", "")
SLACK_PROXY = None

ALERT_SLACK_CHANNELS = {
    "high-volume": "#trading-alerts",
    "data-quality": "#data-engineering",
    "pipeline-failure": "#data-engineering-critical",
    "compliance": "#compliance-alerts",
}

# ─── Embedded Dashboard (Guest Tokens) ───────────────────────────────────────

GUEST_ROLE_NAME = "EmbeddedViewer"
GUEST_TOKEN_JWT_SECRET = os.environ.get("GUEST_TOKEN_SECRET", "CHANGE_ME")
GUEST_TOKEN_JWT_EXP_SECONDS = 3600

ENABLE_CORS = True
CORS_OPTIONS = {
    "origins": [
        "http://localhost:3000",
        "http://localhost:8000",
    ],
    "supports_credentials": True,
}

TALISMAN_ENABLED = False   # Disabled locally; enable in production with proper CSP
