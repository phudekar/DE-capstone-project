"""Tests for PriceAlertDetector: threshold, cooldown, severity classification."""

from flink_processor.operators.price_alert_detector import (
    PriceAlertDetector,
    classify_severity,
)


class TestClassifySeverity:
    def test_below_threshold_returns_none(self):
        assert classify_severity(0.01) is None

    def test_medium_threshold(self):
        assert classify_severity(0.02) == "MEDIUM"
        assert classify_severity(0.04) == "MEDIUM"

    def test_high_threshold(self):
        assert classify_severity(0.05) == "HIGH"
        assert classify_severity(0.09) == "HIGH"

    def test_critical_threshold(self):
        assert classify_severity(0.10) == "CRITICAL"
        assert classify_severity(0.50) == "CRITICAL"


class TestPriceAlertDetector:
    def test_first_event_sets_baseline_no_alert(self):
        detector = PriceAlertDetector()
        result = detector.process("AAPL", 100.0, "2024-01-01T00:00:00Z")
        assert result is None

    def test_small_change_no_alert(self):
        detector = PriceAlertDetector()
        detector.process("AAPL", 100.0, "2024-01-01T00:00:00Z")
        result = detector.process("AAPL", 101.0, "2024-01-01T00:00:01Z")
        assert result is None  # 1% < 2% threshold

    def test_medium_alert_on_price_drop(self):
        detector = PriceAlertDetector()
        detector.process("AAPL", 100.0, "2024-01-01T00:00:00Z")
        result = detector.process("AAPL", 97.5, "2024-01-01T00:00:01Z")
        assert result is not None
        assert result["severity"] == "MEDIUM"
        assert result["direction"] == "DOWN"
        assert result["symbol"] == "AAPL"

    def test_high_alert_on_price_rise(self):
        detector = PriceAlertDetector()
        detector.process("AAPL", 100.0, "2024-01-01T00:00:00Z")
        result = detector.process("AAPL", 106.0, "2024-01-01T00:00:01Z")
        assert result is not None
        assert result["severity"] == "HIGH"
        assert result["direction"] == "UP"

    def test_critical_alert(self):
        detector = PriceAlertDetector()
        detector.process("AAPL", 100.0, "2024-01-01T00:00:00Z")
        result = detector.process("AAPL", 112.0, "2024-01-01T00:00:01Z")
        assert result is not None
        assert result["severity"] == "CRITICAL"

    def test_cooldown_prevents_repeated_alerts(self):
        detector = PriceAlertDetector()
        detector.process("AAPL", 100.0, "2024-01-01T00:00:00Z")

        # First alert fires
        result1 = detector.process("AAPL", 97.0, "2024-01-01T00:00:01Z")
        assert result1 is not None

        # Second alert within cooldown is suppressed
        result2 = detector.process("AAPL", 93.0, "2024-01-01T00:00:02Z")
        assert result2 is None

    def test_alert_fires_after_cooldown(self):
        detector = PriceAlertDetector()

        # Baseline
        detector.process("AAPL", 100.0, "2024-01-01T00:00:00Z")

        # First alert fires (3% drop)
        result1 = detector.process("AAPL", 97.0, "2024-01-01T00:00:01Z")
        assert result1 is not None

        # Immediately suppressed by cooldown
        result2 = detector.process("AAPL", 93.0, "2024-01-01T00:00:02Z")
        assert result2 is None

        # Simulate cooldown expiry by directly modifying state
        detector._state["AAPL"]["last_alert_ts"] -= 120
        result3 = detector.process("AAPL", 90.0, "2024-01-01T00:02:02Z")
        assert result3 is not None

    def test_independent_symbols(self):
        detector = PriceAlertDetector()
        detector.process("AAPL", 100.0, "2024-01-01T00:00:00Z")
        detector.process("GOOGL", 200.0, "2024-01-01T00:00:00Z")

        # Alert on AAPL shouldn't affect GOOGL
        result_aapl = detector.process("AAPL", 97.0, "2024-01-01T00:00:01Z")
        result_googl = detector.process("GOOGL", 194.0, "2024-01-01T00:00:01Z")
        assert result_aapl is not None
        assert result_googl is not None

    def test_alert_contains_expected_fields(self):
        detector = PriceAlertDetector()
        detector.process("MSFT", 400.0, "2024-01-01T00:00:00Z")
        result = detector.process("MSFT", 380.0, "2024-01-01T00:00:01Z")
        assert result is not None
        assert set(result.keys()) == {
            "symbol", "severity", "direction", "price",
            "baseline_price", "pct_change", "timestamp", "alert_type",
        }
        assert result["alert_type"] == "price_movement"
        assert result["price"] == 380.0
        assert result["baseline_price"] == 400.0
