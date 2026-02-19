"""OrderBookAnalyzer: computes spread, depth, and imbalance from orderbook snapshots.

Input: raw orderbook snapshot JSON with bids/asks arrays of {price, quantity}.
Output: analytics JSON with best_bid, best_ask, spread, spread_bps, mid_price,
        bid_depth_5, ask_depth_5, and imbalance metrics.
"""

import json
import logging

logger = logging.getLogger(__name__)

DEPTH_LEVELS = 5


def analyze_orderbook(snapshot: dict) -> dict:
    """Compute orderbook analytics from a snapshot dict.

    Args:
        snapshot: Dict with 'symbol', 'timestamp', 'bids', 'asks', 'sequence_number'.

    Returns:
        Analytics dict with spread, depth, and imbalance metrics.
    """
    bids = snapshot.get("bids", [])
    asks = snapshot.get("asks", [])

    # Sort bids descending by price, asks ascending
    sorted_bids = sorted(bids, key=lambda x: x["price"], reverse=True)
    sorted_asks = sorted(asks, key=lambda x: x["price"])

    best_bid = sorted_bids[0]["price"] if sorted_bids else 0.0
    best_ask = sorted_asks[0]["price"] if sorted_asks else 0.0

    spread = best_ask - best_bid if best_bid > 0 and best_ask > 0 else 0.0
    mid_price = (best_bid + best_ask) / 2 if best_bid > 0 and best_ask > 0 else 0.0
    spread_bps = (spread / mid_price * 10000) if mid_price > 0 else 0.0

    # Depth at top N levels
    bid_depth = sum(b["quantity"] for b in sorted_bids[:DEPTH_LEVELS])
    ask_depth = sum(a["quantity"] for a in sorted_asks[:DEPTH_LEVELS])
    total_depth = bid_depth + ask_depth
    imbalance = (bid_depth - ask_depth) / total_depth if total_depth > 0 else 0.0

    return {
        "symbol": snapshot.get("symbol", ""),
        "timestamp": snapshot.get("timestamp", ""),
        "sequence_number": snapshot.get("sequence_number", 0),
        "best_bid": round(best_bid, 4),
        "best_ask": round(best_ask, 4),
        "spread": round(spread, 4),
        "spread_bps": round(spread_bps, 2),
        "mid_price": round(mid_price, 4),
        "bid_depth_5": bid_depth,
        "ask_depth_5": ask_depth,
        "imbalance": round(imbalance, 4),
        "bid_levels": len(sorted_bids),
        "ask_levels": len(sorted_asks),
    }


def process_orderbook_snapshot(snapshot_json: str) -> str | None:
    """Process a raw JSON orderbook snapshot. Returns JSON analytics string or None.

    Designed as a map function for Flink DataStream.
    """
    try:
        snapshot = json.loads(snapshot_json)
        result = analyze_orderbook(snapshot)
        return json.dumps(result)
    except (json.JSONDecodeError, KeyError) as exc:
        logger.warning("Failed to process orderbook snapshot: %s", exc)
        return None
