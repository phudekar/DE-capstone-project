"""Lightweight Docker container metrics exporter for Prometheus.

Polls the Docker API (/containers/stats) and exposes per-container
CPU and memory usage as Prometheus gauges. Designed to work on Colima
where cAdvisor cannot resolve per-container cgroups.
"""

import logging
import time

import docker
from prometheus_client import Gauge, start_http_server

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger(__name__)

POLL_INTERVAL = 15  # seconds

cpu_usage = Gauge(
    "docker_container_cpu_percent",
    "CPU usage percentage per container",
    ["name", "id"],
)
memory_usage = Gauge(
    "docker_container_memory_bytes",
    "Memory usage in bytes per container",
    ["name", "id"],
)
memory_limit = Gauge(
    "docker_container_memory_limit_bytes",
    "Memory limit in bytes per container",
    ["name", "id"],
)
network_rx = Gauge(
    "docker_container_network_rx_bytes",
    "Network bytes received per container",
    ["name", "id"],
)
network_tx = Gauge(
    "docker_container_network_tx_bytes",
    "Network bytes transmitted per container",
    ["name", "id"],
)


def _calc_cpu_percent(stats: dict) -> float:
    """Calculate CPU usage % from Docker stats JSON."""
    cpu = stats.get("cpu_stats", {})
    precpu = stats.get("precpu_stats", {})
    cpu_delta = cpu.get("cpu_usage", {}).get("total_usage", 0) - precpu.get("cpu_usage", {}).get("total_usage", 0)
    system_delta = cpu.get("system_cpu_usage", 0) - precpu.get("system_cpu_usage", 0)
    n_cpus = cpu.get("online_cpus", 1)
    if system_delta > 0 and cpu_delta >= 0:
        return (cpu_delta / system_delta) * n_cpus * 100.0
    return 0.0


def collect(client: docker.DockerClient) -> None:
    """Collect stats for all running containers."""
    for container in client.containers.list():
        name = container.name
        cid = container.short_id
        try:
            stats = container.stats(stream=False)
            cpu_pct = _calc_cpu_percent(stats)
            mem = stats.get("memory_stats", {})
            mem_used = mem.get("usage", 0)
            mem_lim = mem.get("limit", 0)

            cpu_usage.labels(name=name, id=cid).set(cpu_pct)
            memory_usage.labels(name=name, id=cid).set(mem_used)
            memory_limit.labels(name=name, id=cid).set(mem_lim)

            networks = stats.get("networks", {})
            rx = sum(n.get("rx_bytes", 0) for n in networks.values())
            tx = sum(n.get("tx_bytes", 0) for n in networks.values())
            network_rx.labels(name=name, id=cid).set(rx)
            network_tx.labels(name=name, id=cid).set(tx)
        except Exception as exc:
            logger.warning("Failed to collect stats for %s: %s", name, exc)


def main() -> None:
    start_http_server(9417)
    logger.info("Docker exporter listening on :9417")
    client = docker.DockerClient(base_url="unix:///var/run/docker.sock")
    while True:
        collect(client)
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
