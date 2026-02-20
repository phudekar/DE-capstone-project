"""Tests for GraphQL schema compilation and type structure."""

from __future__ import annotations

import pytest
from app.schema import schema


def test_schema_compiles():
    sdl = str(schema)
    assert "type Trade" in sdl
    assert "type Query" in sdl
    assert "type Mutation" in sdl
    assert "type Subscription" in sdl


def test_trade_type_fields():
    trade_type = schema.get_type_by_name("Trade")
    assert trade_type is not None
    # Strawberry stores field .name in Python snake_case
    field_names = {f.name for f in trade_type.fields}
    expected = {"trade_id", "symbol", "price", "quantity", "aggressor_side", "timestamp"}
    assert expected.issubset(field_names)


def test_query_root_fields():
    query_type = schema.get_type_by_name("Query")
    assert query_type is not None
    field_names = {f.name for f in query_type.fields}
    expected = {"trades", "trade_by_id", "daily_summary", "market_overview", "symbols", "order_book"}
    assert expected.issubset(field_names)


def test_mutation_root_fields():
    mutation_type = schema.get_type_by_name("Mutation")
    assert mutation_type is not None
    field_names = {f.name for f in mutation_type.fields}
    expected = {"create_watchlist", "add_to_watchlist", "delete_watchlist"}
    assert expected.issubset(field_names)


def test_subscription_root_fields():
    sub_type = schema.get_type_by_name("Subscription")
    assert sub_type is not None
    field_names = {f.name for f in sub_type.fields}
    assert "on_new_trade" in field_names
    assert "on_trade_alert" in field_names


def test_trade_connection_relay_spec():
    conn_type = schema.get_type_by_name("TradeConnection")
    assert conn_type is not None
    field_names = {f.name for f in conn_type.fields}
    assert "edges" in field_names
    assert "page_info" in field_names
    assert "total_count" in field_names


def test_daily_summary_computed_fields():
    ds_type = schema.get_type_by_name("DailySummary")
    assert ds_type is not None
    field_names = {f.name for f in ds_type.fields}
    assert "price_range" in field_names
    assert "price_change" in field_names
    assert "price_change_pct" in field_names
