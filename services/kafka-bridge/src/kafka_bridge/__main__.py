"""Entry point for the Kafka Bridge service."""

import asyncio
import logging
import signal
import sys

from kafka_bridge.bridge import Bridge
from kafka_bridge.config.settings import Settings


def main() -> None:
    settings = Settings()

    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
        stream=sys.stdout,
    )
    logger = logging.getLogger("kafka_bridge")
    logger.info("Starting Kafka Bridge service")

    bridge = Bridge(settings)

    loop = asyncio.new_event_loop()

    def _shutdown(sig: signal.Signals) -> None:
        logger.info("Received %s, shutting down...", sig.name)
        bridge.request_shutdown()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown, sig)

    try:
        loop.run_until_complete(bridge.run())
    finally:
        loop.close()
        logger.info("Kafka Bridge stopped")


if __name__ == "__main__":
    main()
