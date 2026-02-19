"""Job A entry point: SQL Analytics + Enrichment Pipeline.

Submits tumbling-window trade aggregation (1m, 5m, 15m) and
trade enrichment (lookup join with reference symbols) as a
single Flink SQL statement set.

Usage:
    python -m flink_processor.submit_sql_pipeline
"""

import logging
import sys

from pyflink.table import EnvironmentSettings, TableEnvironment

from flink_processor.config import (
    CHECKPOINT_INTERVAL_MS,
    PARALLELISM,
    load_reference_symbols,
)
from flink_processor.sql.ddl import (
    enriched_trades_sink_ddl,
    raw_trades_source_ddl,
    reference_symbols_ddl,
    trade_aggregates_sink_ddl,
)
from flink_processor.sql.trade_aggregation import all_aggregation_inserts, enrichment_insert

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)


def main() -> None:
    logger.info("Starting Job A: SQL Analytics + Enrichment Pipeline")

    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # Configure execution
    t_env.get_config().set("parallelism.default", str(PARALLELISM))
    t_env.get_config().set(
        "execution.checkpointing.interval", f"{CHECKPOINT_INTERVAL_MS}ms"
    )
    t_env.get_config().set(
        "execution.checkpointing.mode", "AT_LEAST_ONCE"
    )
    t_env.get_config().set("state.backend", "hashmap")
    t_env.get_config().set("table.exec.source.idle-timeout", "30000ms")

    # Create source and sink tables
    logger.info("Creating source table: raw_trades")
    t_env.execute_sql(raw_trades_source_ddl())

    logger.info("Creating sink table: trade_aggregates")
    t_env.execute_sql(trade_aggregates_sink_ddl())

    logger.info("Creating sink table: enriched_trades")
    t_env.execute_sql(enriched_trades_sink_ddl())

    # Create reference view from symbols.json
    symbols = load_reference_symbols()
    symbols_list = list(symbols.values())
    logger.info("Creating reference view with %d symbols", len(symbols_list))
    t_env.execute_sql(reference_symbols_ddl(symbols_list))

    # Build statement set for all outputs
    stmt_set = t_env.create_statement_set()

    # Add windowed aggregation inserts (1m, 5m, 15m)
    for sql in all_aggregation_inserts():
        logger.info("Adding aggregation: %s", sql.strip()[:80])
        stmt_set.add_insert_sql(sql)

    # Add enrichment insert
    logger.info("Adding trade enrichment insert")
    stmt_set.add_insert_sql(enrichment_insert())

    # Execute all as a single Flink job
    logger.info("Submitting Job A statement set")
    stmt_set.execute()
    logger.info("Job A submitted successfully")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("Job A failed")
        sys.exit(1)
