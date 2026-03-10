"""
intelligence/wallet_labeling.py
BlockWatchKE — Wallet Labeling Engine

Loads known wallet labels from CSV and the database,
and provides lookup utilities for the risk engine.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from loguru import logger
from sqlalchemy import text
from database.db_connection import engine
from config.settings import BASE_DIR

KENYA_WALLETS_FILE = os.path.join(BASE_DIR, "data", "kenya_wallets.csv")

KNOWN_EXCHANGES = {
    "TKzxdSv2FZKQrEqkKVgp5DcwEXBEKMg2Ax": "Binance",
    "TNXoiAJ3dct8Fjg4M9fkLFh9S2v9TXc7bR": "Huobi",
    "TN3W4H6rK2ce4vX9YnFQHwKENnHjoxb4oH": "OKX",
    "TQA9xCzBbwxkEoTfXNkDbDGQrJg6mAGGcf": "Bybit",
    "TVj7RNVHy6thbM7BWdSe9G6gXwKhjhdNZS": "KuCoin",
}

KNOWN_KENYA_ENTITIES = {
    "TXA123KenyaBroker001": {"label": "Kenya OTC Broker Alpha", "type": "broker",    "confidence": 0.95},
    "TXB999KenyaTrader01":  {"label": "Kenya Individual Trader","type": "individual","confidence": 0.80},
    "TXC456NairobiExch01":  {"label": "Nairobi Crypto Exchange","type": "exchange",  "confidence": 0.99},
    "TXD789KenyaBroker02":  {"label": "Kenya OTC Broker Beta",  "type": "broker",    "confidence": 0.90},
}


def load_kenya_wallets_from_csv() -> pd.DataFrame:
    """Load seed Kenya wallets from CSV file."""
    if not os.path.exists(KENYA_WALLETS_FILE):
        logger.warning(f"Kenya wallets file not found: {KENYA_WALLETS_FILE}")
        return pd.DataFrame()
    df = pd.read_csv(KENYA_WALLETS_FILE)
    logger.info(f"Loaded {len(df)} Kenya wallets from CSV")
    return df


def seed_wallet_labels() -> int:
    """
    Seed the wallet_labels table from CSV and known entities.
    Returns number of records inserted.
    """
    df = load_kenya_wallets_from_csv()
    if df.empty:
        return 0

    records = []
    for _, row in df.iterrows():
        records.append({
            "address":     row["address"],
            "label":       row["label"],
            "entity_type": row["entity_type"],
            "country":     row["country"],
            "confidence":  float(row["confidence"]),
            "is_kenya":    row["country"] == "KE",
            "kenya_score": float(row["confidence"]) if row["country"] == "KE" else 0.0,
        })

    for address, info in KNOWN_EXCHANGES.items():
        records.append({
            "address":     address,
            "label":       info,
            "entity_type": "exchange",
            "country":     "unknown",
            "confidence":  0.99,
            "is_kenya":    False,
            "kenya_score": 0.0,
        })

    sql = text("""
        INSERT INTO wallet_labels
            (address, label, entity_type, country, confidence, is_kenya, kenya_score)
        VALUES
            (:address, :label, :entity_type, :country, :confidence, :is_kenya, :kenya_score)
        ON CONFLICT (address) DO UPDATE SET
            label       = EXCLUDED.label,
            entity_type = EXCLUDED.entity_type,
            confidence  = EXCLUDED.confidence,
            is_kenya    = EXCLUDED.is_kenya,
            kenya_score = EXCLUDED.kenya_score,
            updated_at  = NOW()
    """)

    try:
        with engine.begin() as conn:
            result = conn.execute(sql, records)
        logger.info(f"✅ Seeded {result.rowcount} wallet labels into database")
        return result.rowcount
    except Exception as exc:
        logger.error(f"Seed failed: {exc}")
        return 0


def get_label(address: str) -> dict:
    """Look up a wallet label by address."""
    sql = text("SELECT * FROM wallet_labels WHERE address = :address")
    with engine.connect() as conn:
        row = conn.execute(sql, {"address": address}).fetchone()
    if row:
        return dict(row._mapping)
    if address in KNOWN_EXCHANGES:
        return {"label": KNOWN_EXCHANGES[address], "entity_type": "exchange",
                "country": "unknown", "is_kenya": False}
    return {"label": "unknown", "entity_type": "unknown",
            "country": "unknown", "is_kenya": False}


if __name__ == "__main__":
    seed_wallet_labels()
