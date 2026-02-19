"""Bronze writer: Kafka consumer → Iceberg Bronze tables (micro-batch append)."""

from __future__ import annotations

import json
import logging
import signal
import time
from datetime import datetime, timezone

import pyarrow as pa
from confluent_kafka import Consumer, KafkaError

from lakehouse import config
from lakehouse.catalog import get_catalog

logger = logging.getLogger(__name__)


class BronzeWriter:
    """Micro-batch Kafka consumer that writes to Bronze Iceberg tables."""

    def __init__(self):
        self._running = False
        self._catalog = get_catalog()
        self._trades_table = self._catalog.load_table(f"{config.NS_BRONZE}.raw_trades")
        self._orderbook_table = self._catalog.load_table(f"{config.NS_BRONZE}.raw_orderbook")
        self._consumer = Consumer(
            {
                "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
                "group.id": config.KAFKA_GROUP_ID,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

    def _parse_trade(self, value: dict, topic: str, partition: int, offset: int) -> dict:
        """Parse a raw trade message into Bronze schema columns."""
        now = datetime.now(timezone.utc)
        ts = datetime.fromisoformat(value["timestamp"].replace("Z", "+00:00"))
        return {
            "trade_id": value["trade_id"],
            "symbol": value["symbol"],
            "price": float(value["price"]),
            "quantity": int(value["quantity"]),
            "buyer_order_id": value["buyer_order_id"],
            "seller_order_id": value["seller_order_id"],
            "buyer_agent_id": value["buyer_agent_id"],
            "seller_agent_id": value["seller_agent_id"],
            "aggressor_side": str(value["aggressor_side"]),
            "event_type": value["event_type"],
            "timestamp": ts,
            "_kafka_topic": topic,
            "_kafka_partition": partition,
            "_kafka_offset": offset,
            "_ingested_at": now,
        }

    def _parse_orderbook(self, value: dict, topic: str, partition: int, offset: int) -> dict:
        """Parse a raw orderbook message into Bronze schema columns."""
        now = datetime.now(timezone.utc)
        ts = datetime.fromisoformat(value["timestamp"].replace("Z", "+00:00"))
        return {
            "symbol": value["symbol"],
            "bids_json": json.dumps(value.get("bids", [])),
            "asks_json": json.dumps(value.get("asks", [])),
            "sequence_number": int(value["sequence_number"]),
            "event_type": value["event_type"],
            "timestamp": ts,
            "_kafka_topic": topic,
            "_kafka_partition": partition,
            "_kafka_offset": offset,
            "_ingested_at": now,
        }

    def _flush_batch(self, trade_rows: list[dict], orderbook_rows: list[dict]) -> None:
        """Write accumulated rows to their respective Bronze tables."""
        if trade_rows:
            arrow_table = pa.table({k: [r[k] for r in trade_rows] for k in trade_rows[0]})
            self._trades_table.append(arrow_table)
            logger.info("Flushed %d trade rows to bronze.raw_trades.", len(trade_rows))

        if orderbook_rows:
            arrow_table = pa.table(
                {k: [r[k] for r in orderbook_rows] for k in orderbook_rows[0]}
            )
            self._orderbook_table.append(arrow_table)
            logger.info("Flushed %d orderbook rows to bronze.raw_orderbook.", len(orderbook_rows))

    def run(self) -> None:
        """Main consumer loop with micro-batching."""
        self._running = True
        self._consumer.subscribe([config.TOPIC_RAW_TRADES, config.TOPIC_RAW_ORDERBOOK])
        logger.info(
            "Bronze writer started. Subscribed to: %s, %s",
            config.TOPIC_RAW_TRADES,
            config.TOPIC_RAW_ORDERBOOK,
        )

        trade_rows: list[dict] = []
        orderbook_rows: list[dict] = []
        batch_start = time.monotonic()

        while self._running:
            msg = self._consumer.poll(timeout=1.0)

            if msg is not None:
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Kafka error: %s", msg.error())
                    continue

                try:
                    value = json.loads(msg.value().decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    logger.warning(
                        "Skipping unparseable message at %s/%d/%d",
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                    )
                    continue

                topic = msg.topic()
                if topic == config.TOPIC_RAW_TRADES:
                    trade_rows.append(
                        self._parse_trade(value, topic, msg.partition(), msg.offset())
                    )
                elif topic == config.TOPIC_RAW_ORDERBOOK:
                    orderbook_rows.append(
                        self._parse_orderbook(value, topic, msg.partition(), msg.offset())
                    )

            total = len(trade_rows) + len(orderbook_rows)
            elapsed = time.monotonic() - batch_start

            if total >= config.BATCH_MAX_MESSAGES or (total > 0 and elapsed >= config.BATCH_TIMEOUT_SECONDS):
                self._flush_batch(trade_rows, orderbook_rows)
                self._consumer.commit()
                trade_rows.clear()
                orderbook_rows.clear()
                batch_start = time.monotonic()

        # Flush remaining on shutdown
        if trade_rows or orderbook_rows:
            self._flush_batch(trade_rows, orderbook_rows)
            self._consumer.commit()

        self._consumer.close()
        logger.info("Bronze writer stopped.")

    def stop(self) -> None:
        """Signal the writer to stop gracefully."""
        self._running = False


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    writer = BronzeWriter()

    def _handle_signal(signum, frame):
        logger.info("Received signal %d, shutting down...", signum)
        writer.stop()

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    writer.run()


if __name__ == "__main__":
    main()
