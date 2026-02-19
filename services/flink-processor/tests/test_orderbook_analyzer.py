"""Tests for OrderBookAnalyzer: spread, depth, imbalance calculations."""

from flink_processor.operators.orderbook_analyzer import analyze_orderbook


class TestAnalyzeOrderbook:
    def _snapshot(self, bids=None, asks=None, symbol="AAPL"):
        return {
            "event_type": "OrderBookSnapshot",
            "timestamp": "2024-01-01T00:00:00Z",
            "symbol": symbol,
            "bids": bids or [],
            "asks": asks or [],
            "sequence_number": 42,
        }

    def test_basic_spread(self):
        result = analyze_orderbook(self._snapshot(
            bids=[{"price": 100.0, "quantity": 10}],
            asks=[{"price": 101.0, "quantity": 10}],
        ))
        assert result["best_bid"] == 100.0
        assert result["best_ask"] == 101.0
        assert result["spread"] == 1.0
        assert result["mid_price"] == 100.5

    def test_spread_bps(self):
        result = analyze_orderbook(self._snapshot(
            bids=[{"price": 100.0, "quantity": 10}],
            asks=[{"price": 100.5, "quantity": 10}],
        ))
        # spread = 0.5, mid = 100.25, bps = 0.5/100.25 * 10000 ≈ 49.88
        assert 49.0 < result["spread_bps"] < 50.0

    def test_depth_at_5_levels(self):
        bids = [{"price": 100.0 - i, "quantity": 10 + i} for i in range(7)]
        asks = [{"price": 101.0 + i, "quantity": 5 + i} for i in range(7)]

        result = analyze_orderbook(self._snapshot(bids=bids, asks=asks))
        # Top 5 bid quantities: 10, 11, 12, 13, 14 = 60
        assert result["bid_depth_5"] == 60
        # Top 5 ask quantities: 5, 6, 7, 8, 9 = 35
        assert result["ask_depth_5"] == 35

    def test_imbalance_calculation(self):
        result = analyze_orderbook(self._snapshot(
            bids=[{"price": 100.0, "quantity": 80}],
            asks=[{"price": 101.0, "quantity": 20}],
        ))
        # imbalance = (80 - 20) / (80 + 20) = 0.6
        assert result["imbalance"] == 0.6

    def test_imbalance_negative_when_asks_dominate(self):
        result = analyze_orderbook(self._snapshot(
            bids=[{"price": 100.0, "quantity": 20}],
            asks=[{"price": 101.0, "quantity": 80}],
        ))
        assert result["imbalance"] == -0.6

    def test_empty_orderbook(self):
        result = analyze_orderbook(self._snapshot(bids=[], asks=[]))
        assert result["best_bid"] == 0.0
        assert result["best_ask"] == 0.0
        assert result["spread"] == 0.0
        assert result["mid_price"] == 0.0
        assert result["imbalance"] == 0.0

    def test_bids_sorted_descending(self):
        """Best bid should be the highest price, even if input is unsorted."""
        bids = [
            {"price": 98.0, "quantity": 10},
            {"price": 100.0, "quantity": 5},
            {"price": 99.0, "quantity": 15},
        ]
        result = analyze_orderbook(self._snapshot(bids=bids, asks=[{"price": 101.0, "quantity": 10}]))
        assert result["best_bid"] == 100.0

    def test_asks_sorted_ascending(self):
        """Best ask should be the lowest price, even if input is unsorted."""
        asks = [
            {"price": 103.0, "quantity": 10},
            {"price": 101.0, "quantity": 5},
            {"price": 102.0, "quantity": 15},
        ]
        result = analyze_orderbook(self._snapshot(bids=[{"price": 100.0, "quantity": 10}], asks=asks))
        assert result["best_ask"] == 101.0

    def test_output_contains_expected_fields(self):
        result = analyze_orderbook(self._snapshot(
            bids=[{"price": 100.0, "quantity": 10}],
            asks=[{"price": 101.0, "quantity": 10}],
        ))
        expected_keys = {
            "symbol", "timestamp", "sequence_number",
            "best_bid", "best_ask", "spread", "spread_bps", "mid_price",
            "bid_depth_5", "ask_depth_5", "imbalance",
            "bid_levels", "ask_levels",
        }
        assert set(result.keys()) == expected_keys

    def test_symbol_preserved(self):
        result = analyze_orderbook(self._snapshot(
            symbol="NVDA",
            bids=[{"price": 450.0, "quantity": 10}],
            asks=[{"price": 451.0, "quantity": 10}],
        ))
        assert result["symbol"] == "NVDA"
