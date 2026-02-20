"""Stage 6 — Data quality validation rules.

Expresses the Great Expectations suite logic directly in DuckDB SQL so
the tests can run without needing the GX package installed in this venv.

Validates:
  - Bronze: schema compliance, null checks, positive values, unique offsets
  - Silver: dedup, field integrity, price/qty unchanged from bronze
  - Gold: OHLCV invariants (the custom ExpectOHLCVValid expectation)
  - Cross-layer: no data loss, temporal ordering (processed_at >= ingested_at)
"""

import pytest


# ── Bronze quality ─────────────────────────────────────────────────────────────

def test_bronze_no_null_trade_ids(pipeline_db, populated_pipeline):
    count = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM bronze_trades WHERE trade_id IS NULL"
    ).fetchone()[0]
    assert count == 0


def test_bronze_event_type_only_trade_executed(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute("""
        SELECT COUNT(*) FROM bronze_trades
        WHERE event_type NOT IN ('TradeExecuted')
    """).fetchone()[0]
    assert bad == 0


def test_bronze_price_in_reasonable_range(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM bronze_trades WHERE price < 0.01 OR price > 1_000_000"
    ).fetchone()[0]
    assert bad == 0


def test_bronze_quantity_positive(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM bronze_trades WHERE quantity <= 0"
    ).fetchone()[0]
    assert bad == 0


def test_bronze_kafka_offset_unique_per_partition(pipeline_db, populated_pipeline):
    dups = pipeline_db.conn.execute("""
        SELECT _kafka_partition, _kafka_offset, COUNT(*)
        FROM bronze_trades
        GROUP BY _kafka_partition, _kafka_offset HAVING COUNT(*) > 1
    """).fetchall()
    assert len(dups) == 0


def test_bronze_no_null_timestamps(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM bronze_trades WHERE timestamp IS NULL"
    ).fetchone()[0]
    assert bad == 0


# ── Silver quality ─────────────────────────────────────────────────────────────

def test_silver_no_duplicate_trade_ids(pipeline_db, populated_pipeline):
    dups = pipeline_db.conn.execute("""
        SELECT trade_id, COUNT(*) FROM silver_trades
        GROUP BY trade_id HAVING COUNT(*) > 1
    """).fetchall()
    assert len(dups) == 0


def test_silver_required_fields_not_null(pipeline_db, populated_pipeline):
    for col in ("trade_id", "symbol", "price", "quantity",
                "buyer_order_id", "seller_order_id", "timestamp"):
        bad = pipeline_db.conn.execute(
            f"SELECT COUNT(*) FROM silver_trades WHERE {col} IS NULL"
        ).fetchone()[0]
        assert bad == 0, f"NULL in silver_trades.{col}"


def test_silver_price_consistent_with_bronze(pipeline_db, populated_pipeline):
    mismatches = pipeline_db.conn.execute("""
        SELECT COUNT(*) FROM silver_trades s
        JOIN bronze_trades b USING (trade_id)
        WHERE ABS(s.price - b.price) > 0.001
    """).fetchone()[0]
    assert mismatches == 0


def test_silver_quantity_consistent_with_bronze(pipeline_db, populated_pipeline):
    mismatches = pipeline_db.conn.execute("""
        SELECT COUNT(*) FROM silver_trades s
        JOIN bronze_trades b USING (trade_id)
        WHERE s.quantity != b.quantity
    """).fetchone()[0]
    assert mismatches == 0


def test_silver_timestamp_consistent_with_bronze(pipeline_db, populated_pipeline):
    mismatches = pipeline_db.conn.execute("""
        SELECT COUNT(*) FROM silver_trades s
        JOIN bronze_trades b USING (trade_id)
        WHERE s.timestamp != b.timestamp
    """).fetchone()[0]
    assert mismatches == 0


# ── Custom OHLCV expectation (ExpectOHLCVValid) expressed in SQL ───────────────

def test_ohlcv_high_gte_open(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM gold_daily_summary WHERE high_price < open_price"
    ).fetchone()[0]
    assert bad == 0, "OHLCV violation: high_price < open_price"


def test_ohlcv_high_gte_close(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM gold_daily_summary WHERE high_price < close_price"
    ).fetchone()[0]
    assert bad == 0, "OHLCV violation: high_price < close_price"


def test_ohlcv_low_lte_open(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM gold_daily_summary WHERE low_price > open_price"
    ).fetchone()[0]
    assert bad == 0, "OHLCV violation: low_price > open_price"


def test_ohlcv_low_lte_close(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM gold_daily_summary WHERE low_price > close_price"
    ).fetchone()[0]
    assert bad == 0, "OHLCV violation: low_price > close_price"


def test_ohlcv_volume_positive(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM gold_daily_summary WHERE total_volume <= 0"
    ).fetchone()[0]
    assert bad == 0, "OHLCV violation: total_volume <= 0"


# ── Freshness / temporal ordering ─────────────────────────────────────────────

def test_silver_processed_at_after_bronze_ingested(pipeline_db, populated_pipeline):
    violations = pipeline_db.conn.execute("""
        SELECT COUNT(*) FROM silver_trades s
        JOIN bronze_trades b USING (trade_id)
        WHERE s._processed_at < b._ingested_at
    """).fetchone()[0]
    assert violations == 0


def test_bronze_ingested_at_not_null(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM bronze_trades WHERE _ingested_at IS NULL"
    ).fetchone()[0]
    assert bad == 0


def test_gold_aggregated_at_not_null(pipeline_db, populated_pipeline):
    bad = pipeline_db.conn.execute(
        "SELECT COUNT(*) FROM gold_daily_summary WHERE _aggregated_at IS NULL"
    ).fetchone()[0]
    assert bad == 0


# ── Cross-layer row count checks ──────────────────────────────────────────────

def test_no_data_loss_bronze_to_silver(pipeline_db, populated_pipeline):
    missing = pipeline_db.conn.execute("""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT trade_id FROM bronze_trades
            EXCEPT
            SELECT trade_id FROM silver_trades
        )
    """).fetchone()[0]
    assert missing == 0


def test_silver_not_more_than_bronze(pipeline_db, populated_pipeline):
    assert pipeline_db.count("silver_trades") <= pipeline_db.count("bronze_trades")


def test_gold_not_more_rows_than_silver(pipeline_db, populated_pipeline):
    assert pipeline_db.count("gold_daily_summary") <= pipeline_db.count("silver_trades")
