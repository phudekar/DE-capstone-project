"""Async WebSocket consumer with reconnection and exponential backoff."""

import asyncio
import logging
from collections.abc import AsyncIterator

import websockets

from kafka_bridge.config.settings import Settings
from kafka_bridge.metrics.prometheus import WS_RECONNECTS

logger = logging.getLogger(__name__)


class WebSocketClient:
    """Connects to DE-Stock WebSocket and yields raw text messages."""

    def __init__(self, settings: Settings) -> None:
        self._uri = settings.ws_uri
        self._reconnect_delay = settings.ws_reconnect_delay
        self._max_delay = settings.ws_reconnect_max_delay
        self._shutdown = False

    def request_shutdown(self) -> None:
        self._shutdown = True

    async def messages(self) -> AsyncIterator[str]:
        """Yield WebSocket messages, reconnecting on failure with exponential backoff."""
        delay = self._reconnect_delay

        while not self._shutdown:
            try:
                async with websockets.connect(self._uri, ping_interval=20, ping_timeout=10) as ws:
                    logger.info("Connected to DE-Stock WebSocket at %s", self._uri)
                    delay = self._reconnect_delay  # reset on successful connect

                    async for message in ws:
                        yield message
                        if self._shutdown:
                            return

            except websockets.ConnectionClosed as exc:
                logger.warning("WebSocket connection closed: %s", exc)
            except (OSError, websockets.WebSocketException) as exc:
                logger.error("WebSocket error: %s", exc)

            if self._shutdown:
                return

            WS_RECONNECTS.inc()
            logger.info("Reconnecting in %.1fs...", delay)
            await asyncio.sleep(delay)
            delay = min(delay * 2, self._max_delay)
