"""create_charts.py
Create all charts for the Symbol Analysis dashboard via Superset REST API.

Charts visualise the ``silver_trades_bid_ask`` virtual dataset and cover
price, volume, spread, order-flow and volatility perspectives.
"""

import json
import logging

log = logging.getLogger(__name__)


def _get_database_id(superset_url: str, session) -> int | None:
    resp = session.get(f"{superset_url}/api/v1/database/")
    if resp.status_code == 200:
        for db in resp.json().get("result", []):
            if "DuckDB" in db.get("database_name", ""):
                return db["id"]
    return None


def _get_dataset_id(superset_url: str, session, table_name: str) -> int | None:
    resp = session.get(
        f"{superset_url}/api/v1/dataset/",
        params={"q": json.dumps({"filters": [{"col": "table_name", "opr": "eq", "val": table_name}]})},
    )
    if resp.status_code == 200:
        results = resp.json().get("result", [])
        if results:
            return results[0]["id"]
    return None


def _build_charts(ds_id: int) -> list[dict]:
    """Return chart payload definitions for the Symbol Analysis dashboard."""

    datasource = f"{ds_id}__table"
    common_line = {
        "datasource_id": ds_id,
        "datasource_type": "table",
        "viz_type": "echarts_timeseries_line",
        "params": {},  # overridden per chart
    }

    def _line(name: str, description: str, params_extra: dict) -> dict:
        base_params = {
            "datasource": datasource,
            "viz_type": "echarts_timeseries_line",
            "granularity_sqla": "timestamp",
            "time_grain_sqla": "PT1M",
            "time_range": "No filter",
            "row_limit": 10000,
        }
        base_params.update(params_extra)
        return {
            **common_line,
            "slice_name": name,
            "description": description,
            "params": json.dumps(base_params),
        }

    charts = [
        # 1. Price vs Time
        _line(
            "Price vs Time",
            "Track price trends across all symbols over time to identify momentum and divergences",
            {
                "groupby": ["symbol"],
                "metrics": [
                    {
                        "label": "AVG(price)",
                        "expressionType": "SIMPLE",
                        "column": {"column_name": "price"},
                        "aggregate": "AVG",
                    }
                ],
            },
        ),
        # 2. OHLC Price Movement
        _line(
            "OHLC Price Movement",
            "Open/High/Low/Close per interval — the core view for identifying intraday price patterns",
            {
                "metrics": [
                    {"label": "open", "expressionType": "SQL", "sqlExpression": "ARG_MIN(price, timestamp)"},
                    {"label": "high", "expressionType": "SQL", "sqlExpression": "MAX(price)"},
                    {"label": "low", "expressionType": "SQL", "sqlExpression": "MIN(price)"},
                    {"label": "close", "expressionType": "SQL", "sqlExpression": "ARG_MAX(price, timestamp)"},
                ],
            },
        ),
        # 3. Bid vs Ask
        _line(
            "Bid vs Ask",
            "Compare aggressive buy vs sell execution prices — widening gap signals directional pressure",
            {
                "groupby": ["side"],
                "metrics": [
                    {
                        "label": "AVG(price)",
                        "expressionType": "SIMPLE",
                        "column": {"column_name": "price"},
                        "aggregate": "AVG",
                    }
                ],
            },
        ),
        # 4. Volume over Time
        _line(
            "Volume over Time",
            "Spot volume spikes that often precede major price moves or reversals",
            {
                "metrics": [
                    {
                        "label": "SUM(quantity)",
                        "expressionType": "SIMPLE",
                        "column": {"column_name": "quantity"},
                        "aggregate": "SUM",
                    }
                ],
            },
        ),
        # 5. Bid-Ask Spread
        _line(
            "Bid-Ask Spread",
            "Measures liquidity — tight spread = liquid market, wide spread = thin/volatile conditions",
            {
                "metrics": [
                    {
                        "label": "spread",
                        "expressionType": "SQL",
                        "sqlExpression": (
                            "AVG(CASE WHEN side = 'ask' THEN price END) - AVG(CASE WHEN side = 'bid' THEN price END)"
                        ),
                    },
                ],
            },
        ),
        # 6. VWAP vs Price
        _line(
            "VWAP vs Price",
            "Price above VWAP = bullish bias, below = bearish — key institutional benchmark",
            {
                "metrics": [
                    {
                        "label": "AVG(price)",
                        "expressionType": "SIMPLE",
                        "column": {"column_name": "price"},
                        "aggregate": "AVG",
                    },
                    {
                        "label": "VWAP",
                        "expressionType": "SQL",
                        "sqlExpression": "SUM(price * quantity) / NULLIF(SUM(quantity), 0)",
                    },
                ],
            },
        ),
        # 7. Buy vs Sell Pressure
        _line(
            "Buy vs Sell Pressure",
            "Compares aggressive buyer vs seller volume to reveal which side controls the tape",
            {
                "metrics": [
                    {
                        "label": "buy_volume",
                        "expressionType": "SQL",
                        "sqlExpression": "SUM(CASE WHEN side = 'buy' THEN quantity ELSE 0 END)",
                    },
                    {
                        "label": "sell_volume",
                        "expressionType": "SQL",
                        "sqlExpression": "SUM(CASE WHEN side = 'sell' THEN quantity ELSE 0 END)",
                    },
                ],
            },
        ),
        # 8. Order Flow Imbalance
        _line(
            "Order Flow Imbalance",
            "Net buy minus sell volume — sustained positive/negative readings signal trend strength",
            {
                "metrics": [
                    {
                        "label": "net_flow",
                        "expressionType": "SQL",
                        "sqlExpression": (
                            "SUM(CASE WHEN side = 'buy' THEN quantity ELSE 0 END) "
                            "- SUM(CASE WHEN side = 'sell' THEN quantity ELSE 0 END)"
                        ),
                    },
                ],
            },
        ),
        # 9. Trade Size Distribution (bar chart)
        {
            **common_line,
            "viz_type": "dist_bar",
            "slice_name": "Trade Size Distribution",
            "description": (
                "Shows mix of small/medium/large/block trades — institutional activity clusters in large buckets"
            ),
            "params": json.dumps(
                {
                    "datasource": datasource,
                    "viz_type": "dist_bar",
                    "granularity_sqla": "timestamp",
                    "time_range": "No filter",
                    "groupby": ["size_bucket"],
                    "metrics": [{"label": "COUNT(*)", "expressionType": "SQL", "sqlExpression": "COUNT(*)"}],
                    "row_limit": 10000,
                }
            ),
        },
        # 10. Large Trades (table)
        {
            **common_line,
            "viz_type": "table",
            "slice_name": "Large Trades (Block Orders)",
            "description": "Block trades (500+ shares) often signal institutional positioning — watch for clusters",
            "params": json.dumps(
                {
                    "datasource": datasource,
                    "viz_type": "table",
                    "granularity_sqla": "timestamp",
                    "time_range": "No filter",
                    "all_columns": ["timestamp", "symbol", "side", "price", "quantity"],
                    "adhoc_filters": [
                        {
                            "clause": "WHERE",
                            "comparator": "500",
                            "expressionType": "SIMPLE",
                            "operator": ">=",
                            "subject": "quantity",
                        },
                    ],
                    "row_limit": 500,
                    "order_desc": True,
                    "order_by_cols": ['["quantity", false]'],
                }
            ),
        },
        # 11. Cumulative Volume
        _line(
            "Cumulative Volume",
            "Running volume total reveals accumulation pace — steeper curve = accelerating activity",
            {
                "metrics": [
                    {
                        "label": "SUM(quantity)",
                        "expressionType": "SIMPLE",
                        "column": {"column_name": "quantity"},
                        "aggregate": "SUM",
                    }
                ],
                "rolling_type": "cumsum",
            },
        ),
        # 12. Price Volatility
        _line(
            "Price Volatility",
            "Standard deviation of price per interval — rising volatility often precedes breakouts",
            {
                "metrics": [
                    {
                        "label": "price_stddev",
                        "expressionType": "SQL",
                        "sqlExpression": "STDDEV(price)",
                    },
                ],
            },
        ),
        # 13. Live Price Ticker
        _line(
            "Live Price Ticker",
            "Near real-time price with 30s auto-refresh — sliding 15-minute window for active monitoring",
            {
                "time_grain_sqla": "PT10S",
                "time_range": "Last 15 minutes",
                "metrics": [
                    {
                        "label": "AVG(price)",
                        "expressionType": "SIMPLE",
                        "column": {"column_name": "price"},
                        "aggregate": "AVG",
                    }
                ],
            },
        ),
    ]

    return charts


def create_charts(superset_url: str, session) -> None:
    ds_id = _get_dataset_id(superset_url, session, "silver_trades_bid_ask")
    if not ds_id:
        log.error("Dataset 'silver_trades_bid_ask' not found — run create_datasets.py first.")
        return

    charts = _build_charts(ds_id)

    for chart in charts:
        resp = session.post(
            f"{superset_url}/api/v1/chart/",
            json=chart,
        )
        if resp.status_code in (200, 201):
            log.info("Created chart: %s", chart["slice_name"])
        elif resp.status_code == 422 and "already exists" in resp.text:
            log.info("Chart already exists: %s", chart["slice_name"])
        else:
            log.error(
                "Failed to create chart %s: %d %s",
                chart["slice_name"],
                resp.status_code,
                resp.text[:300],
            )
