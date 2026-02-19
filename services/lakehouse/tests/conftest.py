"""Shared test fixtures for lakehouse tests."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime, timezone
from unittest.mock import MagicMock

import pyarrow as pa
import pytest


@pytest.fixture(autouse=True)
def _patch_config(monkeypatch, tmp_path):
    """Override config to use a temp reference data file."""
    symbols = [
        {"symbol": "AAPL", "company_name": "Apple Inc.", "sector": "Technology", "market_cap_category": "large"},
        {"symbol": "GOOGL", "company_name": "Alphabet Inc.", "sector": "Technology", "market_cap_category": "large"},
        {"symbol": "JPM", "company_name": "JPMorgan Chase & Co.", "sector": "Finance", "market_cap_category": "large"},
    ]
    ref_path = tmp_path / "symbols.json"
    ref_path.write_text(json.dumps(symbols))
    monkeypatch.setattr("lakehouse.config.REFERENCE_DATA_PATH", str(ref_path))


@pytest.fixture
def sample_trade_message():
    """A raw trade message as it comes from Kafka (JSON-decoded)."""
    return {
        "event_type": "TradeExecuted",
        "timestamp": "2024-01-15T10:30:00.123Z",
        "trade_id": "T-001",
        "symbol": "AAPL",
        "price": 185.50,
        "quantity": 100,
        "buyer_order_id": "BO-001",
        "seller_order_id": "SO-001",
        "buyer_agent_id": "BA-001",
        "seller_agent_id": "SA-001",
        "aggressor_side": "Buy",
    }


@pytest.fixture
def sample_orderbook_message():
    """A raw orderbook snapshot as it comes from Kafka (JSON-decoded)."""
    return {
        "event_type": "OrderBookSnapshot",
        "timestamp": "2024-01-15T10:30:00.456Z",
        "symbol": "AAPL",
        "bids": [
            {"price": 185.40, "quantity": 200},
            {"price": 185.30, "quantity": 150},
        ],
        "asks": [
            {"price": 185.60, "quantity": 180},
            {"price": 185.70, "quantity": 120},
        ],
        "sequence_number": 42,
    }
