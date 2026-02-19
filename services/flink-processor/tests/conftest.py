"""Shared test fixtures for flink-processor tests."""

import sys
from pathlib import Path

import pytest

# Ensure src is on the path for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
