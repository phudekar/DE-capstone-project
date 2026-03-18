"""PriceAlertDetector: stateful operator that fires alerts on significant price changes.

Detection algorithm:
  1. For each incoming trade, look up (or initialise) the per-symbol state which
     tracks a *baseline price*, a *baseline timestamp*, and a *last alert timestamp*.
  2. If the baseline is older than ALERT_BASELINE_WINDOW_SECONDS (default 5 min),
     reset it to the current price — this prevents stale baselines from generating
     spurious alerts after quiet periods.
  3. Compute the absolute percentage change from baseline. If the change exceeds
     the MEDIUM threshold (2%), classify severity:
       - CRITICAL: >= 10% change
       - HIGH:     >= 5% change
       - MEDIUM:   >= 2% change
  4. Enforce a per-symbol cooldown (ALERT_COOLDOWN_SECONDS, default 60s) to avoid
     flooding downstream consumers with repeated alerts during volatile periods.
  5. After firing an alert, reset the baseline to the current price so subsequent
     alerts measure from the new level.

State is held in a Python dict (not Flink managed state) because the Python
DataStream API wrapper does not expose keyed ValueState. This means state is
lost on job restart — acceptable for alerting but not for exactly-once semantics.
"""

import json
import logging
import time as _time

from flink_processor.config import (
    ALERT_BASELINE_WINDOW_SECONDS,
    ALERT_COOLDOWN_SECONDS,
    ALERT_THRESHOLD_CRITICAL,
    ALERT_THRESHOLD_HIGH,
    ALERT_THRESHOLD_MEDIUM,
)

logger = logging.getLogger(__name__)


def classify_severity(pct_change: float) -> str | None:
    """Return severity level for a given absolute percentage change."""
    if pct_change >= ALERT_THRESHOLD_CRITICAL:
        return "CRITICAL"
    if pct_change >= ALERT_THRESHOLD_HIGH:
        return "HIGH"
    if pct_change >= ALERT_THRESHOLD_MEDIUM:
        return "MEDIUM"
    return None


class PriceAlertDetector:
    """Stateful price alert detector — designed for use with Flink DataStream API.

    Maintains per-symbol state: baseline price, baseline timestamp, last alert timestamp.
    Call `process(symbol, price, timestamp_str)` for each incoming trade.
    Returns an alert dict if triggered, else None.
    """

    def __init__(self) -> None:
        # State per symbol: {symbol: {"baseline_price": float, "baseline_ts": float, "last_alert_ts": float}}
        self._state: dict[str, dict] = {}

    def process(self, symbol: str, price: float, timestamp_str: str) -> dict | None:
        """Process a trade event. Returns alert dict or None."""
        now = _time.time()

        if symbol not in self._state:
            self._state[symbol] = {
                "baseline_price": price,
                "baseline_ts": now,
                "last_alert_ts": 0.0,
            }
            return None

        state = self._state[symbol]

        # Reset baseline if window expired
        if now - state["baseline_ts"] > ALERT_BASELINE_WINDOW_SECONDS:
            state["baseline_price"] = price
            state["baseline_ts"] = now
            return None

        baseline = state["baseline_price"]
        if baseline == 0:
            state["baseline_price"] = price
            return None

        pct_change = abs(price - baseline) / baseline
        severity = classify_severity(pct_change)

        if severity is None:
            return None

        # Check cooldown
        if now - state["last_alert_ts"] < ALERT_COOLDOWN_SECONDS:
            return None

        # Fire alert
        state["last_alert_ts"] = now
        direction = "UP" if price > baseline else "DOWN"

        alert = {
            "symbol": symbol,
            "severity": severity,
            "direction": direction,
            "price": price,
            "baseline_price": baseline,
            "pct_change": round(pct_change * 100, 4),
            "timestamp": timestamp_str,
            "alert_type": "price_movement",
        }

        # Update baseline after alert
        state["baseline_price"] = price
        state["baseline_ts"] = now

        logger.info("ALERT %s: %s %.2f%% on %s", severity, direction, pct_change * 100, symbol)
        return alert


def process_trade_for_alert(detector: PriceAlertDetector, trade_json: str) -> str | None:
    """Process a raw JSON trade string. Returns JSON alert string or None.

    Designed as a flat_map function for Flink DataStream.
    """
    try:
        trade = json.loads(trade_json)
        result = detector.process(
            symbol=trade["symbol"],
            price=trade["price"],
            timestamp_str=trade["timestamp"],
        )
        if result is not None:
            return json.dumps(result)
    except (json.JSONDecodeError, KeyError) as exc:
        logger.warning("Failed to process trade for alert: %s", exc)
    return None
