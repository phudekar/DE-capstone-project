"""Tests for config module: topic names, reference data loading."""

from pathlib import Path

from flink_processor.config import (
    TOPIC_DLQ,
    TOPIC_ENRICHED_TRADES,
    TOPIC_ORDERBOOK_METRICS,
    TOPIC_PRICE_ALERTS,
    TOPIC_RAW_ORDERBOOK,
    TOPIC_RAW_TRADES,
    TOPIC_TRADE_AGGREGATES,
    load_reference_symbols,
)


class TestTopicNames:
    def test_input_topics(self):
        assert TOPIC_RAW_TRADES == "raw.trades"
        assert TOPIC_RAW_ORDERBOOK == "raw.orderbook-snapshots"

    def test_output_topics(self):
        assert TOPIC_TRADE_AGGREGATES == "analytics.trade-aggregates"
        assert TOPIC_ORDERBOOK_METRICS == "analytics.orderbook-metrics"
        assert TOPIC_PRICE_ALERTS == "alerts.price-movement"
        assert TOPIC_ENRICHED_TRADES == "enriched.trades"
        assert TOPIC_DLQ == "dlq.processing-errors"


class TestReferenceDataLoading:
    def test_load_reference_symbols(self):
        symbols = load_reference_symbols()
        assert len(symbols) == 24

    def test_symbols_keyed_by_ticker(self):
        symbols = load_reference_symbols()
        assert "AAPL" in symbols
        assert "GOOGL" in symbols
        assert "JPM" in symbols

    def test_symbol_has_required_fields(self):
        symbols = load_reference_symbols()
        aapl = symbols["AAPL"]
        assert aapl["company_name"] == "Apple Inc."
        assert aapl["sector"] == "Technology"
        assert aapl["market_cap_category"] == "large"

    def test_sectors_present(self):
        symbols = load_reference_symbols()
        sectors = {s["sector"] for s in symbols.values()}
        assert "Technology" in sectors
        assert "Finance" in sectors
        assert "Healthcare" in sectors
        assert "Consumer" in sectors
        assert "Energy" in sectors

    def test_mid_cap_symbols(self):
        symbols = load_reference_symbols()
        mid_caps = [k for k, v in symbols.items() if v["market_cap_category"] == "mid"]
        assert set(mid_caps) == {"SMCI", "MSTR", "PLTR", "RIVN"}
