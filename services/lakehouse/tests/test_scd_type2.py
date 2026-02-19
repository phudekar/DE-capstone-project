"""Tests for SCD Type 2 hash-based change detection logic."""

import hashlib

from lakehouse.scd.scd_type2 import _hash_row


class TestSCD2Hashing:
    """Test the hash function used for SCD2 change detection."""

    def test_same_input_same_hash(self):
        h1 = _hash_row("AAPL", "Apple Inc.", "Technology", "large")
        h2 = _hash_row("AAPL", "Apple Inc.", "Technology", "large")
        assert h1 == h2

    def test_different_company_name_different_hash(self):
        h1 = _hash_row("AAPL", "Apple Inc.", "Technology", "large")
        h2 = _hash_row("AAPL", "Apple Corporation", "Technology", "large")
        assert h1 != h2

    def test_different_sector_different_hash(self):
        h1 = _hash_row("AAPL", "Apple Inc.", "Technology", "large")
        h2 = _hash_row("AAPL", "Apple Inc.", "Consumer Electronics", "large")
        assert h1 != h2

    def test_different_market_cap_different_hash(self):
        h1 = _hash_row("AAPL", "Apple Inc.", "Technology", "large")
        h2 = _hash_row("AAPL", "Apple Inc.", "Technology", "mid")
        assert h1 != h2

    def test_hash_length_is_16(self):
        h = _hash_row("AAPL", "Apple Inc.", "Technology", "large")
        assert len(h) == 16

    def test_hash_is_deterministic_hex(self):
        h = _hash_row("TEST", "Test Co", "Sector", "small")
        assert all(c in "0123456789abcdef" for c in h)
