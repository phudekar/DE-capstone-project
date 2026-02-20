"""Shared fixtures: valid bronze, silver, and gold DataFrames."""

import datetime

import pandas as pd
import pytest


@pytest.fixture
def valid_bronze_df():
    """A valid bronze raw_trades DataFrame."""
    now = pd.Timestamp.now(tz="UTC")
    return pd.DataFrame(
        {
            "trade_id": ["T001", "T002", "T003"],
            "symbol": ["AAPL", "MSFT", "GOOGL"],
            "price": [150.25, 310.50, 140.00],
            "quantity": [100, 200, 50],
            "buyer_order_id": ["B001", "B002", "B003"],
            "seller_order_id": ["S001", "S002", "S003"],
            "buyer_agent_id": ["BA01", "BA02", "BA03"],
            "seller_agent_id": ["SA01", "SA02", "SA03"],
            "aggressor_side": ["Buy", "Sell", "Buy"],
            "event_type": ["TradeExecuted", "TradeExecuted", "TradeExecuted"],
            "timestamp": [now, now, now],
            "_kafka_topic": ["trades", "trades", "trades"],
            "_kafka_partition": [0, 0, 1],
            "_kafka_offset": [1, 2, 3],
            "_ingested_at": [now, now, now],
        }
    )


@pytest.fixture
def valid_silver_df():
    """A valid silver trades DataFrame."""
    now = pd.Timestamp.now(tz="UTC")
    return pd.DataFrame(
        {
            "trade_id": ["T001", "T002", "T003"],
            "symbol": ["AAPL", "MSFT", "GOOGL"],
            "price": [150.25, 310.50, 140.00],
            "quantity": [100, 200, 50],
            "buyer_order_id": ["B001", "B002", "B003"],
            "seller_order_id": ["S001", "S002", "S003"],
            "buyer_agent_id": ["BA01", "BA02", "BA03"],
            "seller_agent_id": ["SA01", "SA02", "SA03"],
            "aggressor_side": ["Buy", "Sell", "Buy"],
            "timestamp": [now, now, now],
            "company_name": ["Apple Inc.", "Microsoft Corp.", "Alphabet Inc."],
            "sector": ["Technology", "Technology", "Technology"],
            "_processed_at": [now, now, now],
        }
    )


@pytest.fixture
def valid_gold_df():
    """A valid gold daily_trading_summary DataFrame."""
    now = pd.Timestamp.now(tz="UTC")
    today = datetime.date.today()
    return pd.DataFrame(
        {
            "symbol": ["AAPL", "MSFT"],
            "trading_date": [today, today],
            "open_price": [150.00, 310.00],
            "close_price": [155.00, 315.00],
            "high_price": [158.00, 320.00],
            "low_price": [148.00, 308.00],
            "vwap": [153.00, 314.00],
            "total_volume": [10000, 20000],
            "trade_count": [500, 800],
            "total_value": [1530000.0, 6280000.0],
            "company_name": ["Apple Inc.", "Microsoft Corp."],
            "sector": ["Technology", "Technology"],
            "_aggregated_at": [now, now],
        }
    )
