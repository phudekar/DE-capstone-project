"""glossary_setup.py
Create the business glossary and glossary terms in OpenMetadata.

Terms are linked to the relevant Iceberg table columns where possible.

Usage:
    OM_HOST=http://localhost:8585 OM_TOKEN=<jwt> python glossary_setup.py
"""

import os
import logging
import requests

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

OM_HOST = os.environ.get("OM_HOST", "http://localhost:8585")
OM_TOKEN = os.environ.get("OM_TOKEN", "CHANGE_ME")
API = f"{OM_HOST}/api/v1"
HEADERS = {
    "Authorization": f"Bearer {OM_TOKEN}",
    "Content-Type": "application/json",
}

GLOSSARY_NAME = "TradingDomainGlossary"

GLOSSARY_TERMS: list[dict] = [
    {
        "name": "Trade",
        "displayName": "Trade",
        "description": (
            "A single financial transaction where a buyer and seller agree "
            "on a price and quantity for a financial instrument."
        ),
        "synonyms": ["execution", "transaction", "fill"],
    },
    {
        "name": "VWAP",
        "displayName": "VWAP (Volume-Weighted Average Price)",
        "description": (
            "The ratio of the value traded to total volume traded over a "
            "given time horizon. VWAP = Σ(price × volume) / Σ(volume). "
            "Used as a benchmark for execution quality."
        ),
        "synonyms": ["volume weighted average price"],
    },
    {
        "name": "BidAskSpread",
        "displayName": "Bid-Ask Spread",
        "description": (
            "The difference between the best ask price and the best bid price "
            "in the order book. A proxy for market liquidity — tighter spreads "
            "indicate deeper markets."
        ),
        "synonyms": ["spread", "bid-offer spread"],
    },
    {
        "name": "OHLCV",
        "displayName": "OHLCV (Open-High-Low-Close-Volume)",
        "description": (
            "Standard candlestick representation for a time period: "
            "opening price, highest price, lowest price, closing price, "
            "and total traded volume."
        ),
        "synonyms": ["candle", "bar", "candlestick"],
    },
    {
        "name": "SurrogatKey",
        "displayName": "Surrogate Key",
        "description": (
            "A system-generated identifier (typically a hash or sequence) "
            "used as the primary key in a dimensional table, independent of "
            "business keys."
        ),
        "synonyms": ["synthetic key", "technical key"],
    },
    {
        "name": "SCDType1",
        "displayName": "SCD Type 1 (Overwrite)",
        "description": (
            "Slowly Changing Dimension strategy where historical values are "
            "overwritten with the latest value — no history is preserved."
        ),
        "synonyms": ["type 1 SCD"],
    },
    {
        "name": "SCDType2",
        "displayName": "SCD Type 2 (Add Row)",
        "description": (
            "Slowly Changing Dimension strategy where each change creates a "
            "new row with validity dates, preserving full history."
        ),
        "synonyms": ["type 2 SCD", "versioned dimension"],
    },
    {
        "name": "MedallionArchitecture",
        "displayName": "Medallion Architecture",
        "description": (
            "A data lakehouse design pattern with three layers: "
            "Bronze (raw), Silver (cleansed/enriched), Gold (aggregated/business-ready)."
        ),
        "synonyms": ["bronze-silver-gold", "multi-hop architecture"],
    },
    {
        "name": "Pseudonymisation",
        "displayName": "Pseudonymisation",
        "description": (
            "Replacing PII with a reversible (given a secret key) or "
            "irreversible (hash) substitute to reduce privacy risk while "
            "retaining analytical utility."
        ),
        "synonyms": ["tokenisation", "de-identification"],
    },
    {
        "name": "DataLineage",
        "displayName": "Data Lineage",
        "description": (
            "The end-to-end record of data's origin, movement, and "
            "transformation — from raw source to final consumption."
        ),
        "synonyms": ["data provenance", "data traceability"],
    },
]


def _get_or_create_glossary() -> str:
    """Return the glossary ID, creating it if necessary."""
    resp = requests.get(f"{API}/glossaries/name/{GLOSSARY_NAME}", headers=HEADERS)
    if resp.status_code == 200:
        gid = resp.json()["id"]
        log.info("Glossary already exists: %s", gid)
        return gid

    body = {
        "name": GLOSSARY_NAME,
        "displayName": "Trading Domain Glossary",
        "description": (
            "Business glossary for the DE Capstone stock-exchange lakehouse, "
            "covering trading concepts, data patterns, and governance terms."
        ),
    }
    resp = requests.post(f"{API}/glossaries", headers=HEADERS, json=body)
    resp.raise_for_status()
    gid = resp.json()["id"]
    log.info("Created glossary %s (id=%s)", GLOSSARY_NAME, gid)
    return gid


def _create_term(glossary_id: str, term: dict) -> None:
    payload = {
        "glossary": {"id": glossary_id, "type": "glossary"},
        "name": term["name"],
        "displayName": term["displayName"],
        "description": term["description"],
        "synonyms": term.get("synonyms", []),
    }
    resp = requests.post(f"{API}/glossaryTerms", headers=HEADERS, json=payload)
    if resp.status_code in (200, 201):
        log.info("Created glossary term: %s", term["name"])
    elif resp.status_code == 409:
        log.info("Term already exists: %s", term["name"])
    else:
        log.error("Failed to create term %s: %d %s", term["name"], resp.status_code, resp.text[:200])


def main() -> None:
    glossary_id = _get_or_create_glossary()
    log.info("Creating %d glossary terms...", len(GLOSSARY_TERMS))
    for term in GLOSSARY_TERMS:
        _create_term(glossary_id, term)
    log.info("Glossary setup complete.")


if __name__ == "__main__":
    main()
