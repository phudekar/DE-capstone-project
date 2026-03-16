"""create_symbol_dashboard.py
Create the Symbol Analysis dashboard with chart layout, native filters,
and 30-second auto-refresh.

Charts must already exist (see create_charts.py).
"""

import json
import logging

log = logging.getLogger(__name__)

DASHBOARD_TITLE = "Symbol Analysis"
DASHBOARD_SLUG = "symbol-analysis"


def _get_chart_ids(superset_url: str, session, chart_names: list[str]) -> dict[str, int]:
    """Return {chart_name: chart_id} for the requested charts."""
    resp = session.get(
        f"{superset_url}/api/v1/chart/",
        params={"q": json.dumps({"page_size": 100})},
    )
    if resp.status_code != 200:
        log.error("Failed to list charts: %d", resp.status_code)
        return {}
    lookup = {c["slice_name"]: c["id"] for c in resp.json().get("result", [])}
    return {name: lookup[name] for name in chart_names if name in lookup}


def _build_position_json(chart_map: dict[str, int]) -> dict:
    """Build a Superset position_json with 8 rows.

    Layout (12-column grid):
      Row 1: Live Price Ticker              (full width)
      Row 2: Price vs Time | OHLC           (6+6)
      Row 3: Bid vs Ask | Bid-Ask Spread    (6+6)
      Row 4: Volume over Time | Cumulative Volume (6+6)
      Row 5: VWAP vs Price | Price Volatility (6+6)
      Row 6: Buy vs Sell Pressure | Order Flow Imbalance (6+6)
      Row 7: Trade Size Distribution         (full width)
      Row 8: Large Trades (Block Orders)     (full width)
    """

    rows = [
        # (row_index, [(chart_name, width), ...])
        (0, [("Live Price Ticker", 12)]),
        (1, [("Price vs Time", 6), ("OHLC Price Movement", 6)]),
        (2, [("Bid vs Ask", 6), ("Bid-Ask Spread", 6)]),
        (3, [("Volume over Time", 6), ("Cumulative Volume", 6)]),
        (4, [("VWAP vs Price", 6), ("Price Volatility", 6)]),
        (5, [("Buy vs Sell Pressure", 6), ("Order Flow Imbalance", 6)]),
        (6, [("Trade Size Distribution", 12)]),
        (7, [("Large Trades (Block Orders)", 12)]),
    ]

    ROW_HEIGHT = 50  # grid units per row

    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": []},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": DASHBOARD_TITLE}},
    }

    for row_idx, cols in rows:
        row_id = f"ROW-{row_idx}"
        position["GRID_ID"]["children"].append(row_id)
        position[row_id] = {
            "type": "ROW",
            "id": row_id,
            "children": [],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }

        col_offset = 0
        for chart_name, width in cols:
            chart_id = chart_map.get(chart_name)
            if chart_id is None:
                log.warning("Chart not found for layout: %s", chart_name)
                col_offset += width
                continue

            col_id = f"COLUMN-{row_idx}-{col_offset}"
            chart_component_id = f"CHART-{chart_id}"

            position[row_id]["children"].append(col_id)
            position[col_id] = {
                "type": "COLUMN",
                "id": col_id,
                "children": [chart_component_id],
                "meta": {"width": width, "background": "BACKGROUND_TRANSPARENT"},
            }
            position[chart_component_id] = {
                "type": "CHART",
                "id": chart_component_id,
                "children": [],
                "meta": {
                    "chartId": chart_id,
                    "width": width,
                    "height": ROW_HEIGHT,
                    "sliceName": chart_name,
                },
            }
            col_offset += width

    return position


def _build_native_filters() -> dict:
    """Build native filter configuration for symbol, time grain, and time range."""
    return {
        "nativeFilters": [
            {
                "id": "NATIVE_FILTER-SYMBOL",
                "name": "Symbol",
                "filterType": "filter_select",
                "targets": [{"column": {"name": "symbol"}, "datasetId": None}],
                "defaultDataMask": {"filterState": {"value": None}},
                "controlValues": {
                    "enableEmptyFilter": False,
                    "inverseSelection": False,
                    "multiSelect": True,
                    "searchAllOptions": True,
                },
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            },
            {
                "id": "NATIVE_FILTER-TIMEGRAIN",
                "name": "Time Window",
                "filterType": "filter_timegrain",
                "targets": [{}],
                "defaultDataMask": {"filterState": {"value": None}},
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            },
            {
                "id": "NATIVE_FILTER-TIMERANGE",
                "name": "Time Range",
                "filterType": "filter_time",
                "targets": [{}],
                "defaultDataMask": {"filterState": {"value": "No filter"}},
                "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            },
        ],
    }


def create_symbol_dashboard(superset_url: str, session) -> None:
    # Check if dashboard already exists
    resp = session.get(
        f"{superset_url}/api/v1/dashboard/",
        params={"q": json.dumps({"filters": [{"col": "slug", "opr": "eq", "val": DASHBOARD_SLUG}]})},
    )
    if resp.status_code == 200 and resp.json().get("result"):
        log.info("Dashboard already exists: %s", DASHBOARD_TITLE)
        return

    # Gather chart IDs
    chart_names = [
        "Price vs Time",
        "OHLC Price Movement",
        "Bid vs Ask",
        "Volume over Time",
        "Bid-Ask Spread",
        "VWAP vs Price",
        "Buy vs Sell Pressure",
        "Order Flow Imbalance",
        "Trade Size Distribution",
        "Large Trades (Block Orders)",
        "Cumulative Volume",
        "Price Volatility",
        "Live Price Ticker",
    ]
    chart_map = _get_chart_ids(superset_url, session, chart_names)

    if not chart_map:
        log.error("No charts found — run create_charts.py first.")
        return

    missing = set(chart_names) - set(chart_map)
    if missing:
        log.warning("Missing charts (will be skipped in layout): %s", missing)

    position_json = _build_position_json(chart_map)
    native_filters = _build_native_filters()

    payload = {
        "dashboard_title": DASHBOARD_TITLE,
        "slug": DASHBOARD_SLUG,
        "position_json": json.dumps(position_json),
        "json_metadata": json.dumps(
            {
                "refresh_frequency": 30,
                "native_filter_configuration": native_filters["nativeFilters"],
                "color_scheme": "",
                "label_colors": {},
                "timed_refresh_immune_slices": [],
                "expanded_slices": {},
                "default_filters": "{}",
            }
        ),
        "published": True,
    }

    resp = session.post(
        f"{superset_url}/api/v1/dashboard/",
        json=payload,
    )
    if resp.status_code in (200, 201):
        dashboard_id = resp.json().get("id")
        log.info("Created dashboard: %s (id=%s)", DASHBOARD_TITLE, dashboard_id)

        # Link charts to dashboard
        if dashboard_id and chart_map:
            chart_ids = list(chart_map.values())
            for chart_id in chart_ids:
                link_resp = session.put(
                    f"{superset_url}/api/v1/chart/{chart_id}",
                    json={"dashboards": [dashboard_id]},
                )
                if link_resp.status_code == 200:
                    log.info("Linked chart %d to dashboard %d", chart_id, dashboard_id)
                else:
                    log.warning(
                        "Failed to link chart %d: %d %s",
                        chart_id,
                        link_resp.status_code,
                        link_resp.text[:200],
                    )
    elif resp.status_code == 422 and "already exists" in resp.text:
        log.info("Dashboard already exists: %s", DASHBOARD_TITLE)
    else:
        log.error(
            "Failed to create dashboard %s: %d %s",
            DASHBOARD_TITLE,
            resp.status_code,
            resp.text[:300],
        )
