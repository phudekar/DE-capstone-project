"""Asset definitions for Bronze, Silver, Gold, Dimension, and Governance layers."""

from orchestrator.assets.governance.masked_tables import (
    masked_gold_summary_public,
    masked_silver_trades_analyst,
    masked_silver_trades_business,
)
from orchestrator.assets.governance.pii_retention import pii_retention_enforcement
from orchestrator.assets.bronze import (
    bronze_raw_marketdata,
    bronze_raw_orderbook,
    bronze_raw_trades,
)
from orchestrator.assets.dimensions import (
    dim_account,
    dim_exchange,
    dim_symbol,
    dim_time,
    dim_trader,
)
from orchestrator.assets.gold import (
    gold_daily_trading_summary,
    gold_market_overview,
    gold_portfolio_positions,
    gold_trader_performance,
)
from orchestrator.assets.silver import (
    silver_market_data,
    silver_orderbook_snapshots,
    silver_trades,
    silver_trader_activity,
)

__all__ = [
    "bronze_raw_trades",
    # Governance
    "masked_silver_trades_analyst",
    "masked_silver_trades_business",
    "masked_gold_summary_public",
    "pii_retention_enforcement",
    "bronze_raw_orderbook",
    "bronze_raw_marketdata",
    "silver_trades",
    "silver_orderbook_snapshots",
    "silver_market_data",
    "silver_trader_activity",
    "gold_daily_trading_summary",
    "gold_trader_performance",
    "gold_market_overview",
    "gold_portfolio_positions",
    "dim_symbol",
    "dim_trader",
    "dim_exchange",
    "dim_time",
    "dim_account",
]
