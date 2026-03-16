"""Tests for cursor pagination encoding/decoding."""

from __future__ import annotations

from app.schema.pagination import decode_cursor, encode_cursor


def test_encode_decode_round_trip():
    for offset in [0, 1, 20, 100, 999]:
        cursor = encode_cursor(offset)
        assert decode_cursor(cursor) == offset


def test_cursor_is_base64_string():
    cursor = encode_cursor(42)
    assert isinstance(cursor, str)
    # Must be decodable
    import base64
    import json

    data = json.loads(base64.b64decode(cursor.encode()).decode())
    assert data["offset"] == 42


def test_different_offsets_produce_different_cursors():
    c1 = encode_cursor(0)
    c2 = encode_cursor(1)
    assert c1 != c2
