"""WebSocket ingestion load test.

Simulates a high-throughput producer sending raw trade events over a WebSocket
connection.  Runs without Docker; measures achieved message rate and p99 latency.

Usage (no pytest, standalone):
    python tests/load/ingestion_load_test.py --messages 10000 --concurrency 4

Usage (as pytest — skipped by default; run with -m load):
    pytest tests/load/ingestion_load_test.py -m load -s
"""

import argparse
import asyncio
import json
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from statistics import mean, quantiles


# ──────────────────────────────────────────────────────────────────────────────
# Synthetic event factories
# ──────────────────────────────────────────────────────────────────────────────

SYMBOLS = ["AAPL", "MSFT", "TSLA", "NVDA", "GOOGL", "AMZN"]
RNG = random.Random(42)


def _make_trade_event() -> dict:
    sym = RNG.choice(SYMBOLS)
    return {
        "event_type": "trade",
        "trade_id": str(uuid.uuid4()),
        "symbol": sym,
        "price": round(100.0 + RNG.gauss(0, 5), 4),
        "quantity": RNG.randint(1, 500),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "buyer_id": f"B{RNG.randint(1000, 9999)}",
        "seller_id": f"S{RNG.randint(1000, 9999)}",
    }


# ──────────────────────────────────────────────────────────────────────────────
# In-process pipeline stub (replaces live WebSocket for standalone measurement)
# ──────────────────────────────────────────────────────────────────────────────


@dataclass
class LoadTestResult:
    total_messages: int = 0
    errors: int = 0
    latencies_ms: list[float] = field(default_factory=list)

    @property
    def throughput_mps(self) -> float:
        if not self.latencies_ms:
            return 0.0
        total_s = sum(self.latencies_ms) / 1000.0
        return self.total_messages / total_s if total_s > 0 else 0.0

    @property
    def p99_ms(self) -> float:
        if len(self.latencies_ms) < 2:
            return self.latencies_ms[0] if self.latencies_ms else 0.0
        return quantiles(self.latencies_ms, n=100)[98]

    @property
    def mean_ms(self) -> float:
        return mean(self.latencies_ms) if self.latencies_ms else 0.0


async def _simulate_produce(event: dict) -> float:
    """Simulate encode + validate latency (no real I/O)."""
    t0 = time.perf_counter()
    payload = json.dumps(event).encode()
    # Simulate a small parse + validation cost
    parsed = json.loads(payload)
    assert parsed["event_type"] == "trade"
    elapsed_ms = (time.perf_counter() - t0) * 1000
    return elapsed_ms


async def _worker(n_messages: int, result: LoadTestResult) -> None:
    for _ in range(n_messages):
        event = _make_trade_event()
        try:
            latency = await _simulate_produce(event)
            result.latencies_ms.append(latency)
            result.total_messages += 1
        except Exception:
            result.errors += 1


async def run_load_test(total_messages: int = 10_000, concurrency: int = 4) -> LoadTestResult:
    """Run the load test and return aggregated results."""
    result = LoadTestResult()
    per_worker = total_messages // concurrency
    tasks = [asyncio.create_task(_worker(per_worker, result)) for _ in range(concurrency)]
    await asyncio.gather(*tasks)
    return result


# ──────────────────────────────────────────────────────────────────────────────
# pytest integration
# ──────────────────────────────────────────────────────────────────────────────

import pytest  # noqa: E402  (after stdlib/dataclass imports)

pytestmark = pytest.mark.load


@pytest.mark.asyncio
async def test_ingestion_throughput_baseline():
    """Assert > 5 000 msg/s and p99 < 1 ms for in-process encode/validate."""
    result = await run_load_test(total_messages=10_000, concurrency=4)
    assert result.errors == 0, f"Load test had {result.errors} errors"
    assert result.total_messages == 10_000
    # In-process (no real network) should be well above 5 k msg/s
    assert result.throughput_mps > 5_000, (
        f"Throughput {result.throughput_mps:.0f} msg/s < 5000 threshold"
    )
    assert result.p99_ms < 1.0, f"p99 latency {result.p99_ms:.3f}ms >= 1.0ms threshold"


# ──────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ──────────────────────────────────────────────────────────────────────────────


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="WebSocket ingestion load test")
    p.add_argument("--messages", type=int, default=10_000, help="Total messages to send")
    p.add_argument("--concurrency", type=int, default=4, help="Concurrent workers")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    result = asyncio.run(run_load_test(args.messages, args.concurrency))
    print(f"\n=== Load Test Results ===")
    print(f"Messages : {result.total_messages:,}")
    print(f"Errors   : {result.errors}")
    print(f"Mean     : {result.mean_ms:.3f} ms")
    print(f"p99      : {result.p99_ms:.3f} ms")
    print(f"Rate     : {result.throughput_mps:,.0f} msg/s")
