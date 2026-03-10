"""
intelligence/kenya_wallet_detection.py
BlockWatchKE — Phase 2: Kenyan Wallet Detection

Scores every wallet in the transactions table for Kenya likelihood
using four detection signals:
  1. Direct seed match (known Kenyan wallets)
  2. Interaction frequency with known Kenyan entities
  3. Transaction timing patterns (EAT timezone: UTC+3)
  4. Repeated cash-out flows to known Kenyan brokers
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from loguru import logger
from sqlalchemy import text
from database.db_connection import engine
from intelligence.wallet_labeling import KNOWN_KENYA_ENTITIES, seed_wallet_labels

# ── Signal weights (must sum to 1.0) ────────────────────────────
WEIGHT_SEED_MATCH   = 0.40
WEIGHT_INTERACTION  = 0.25
WEIGHT_TIMING       = 0.20
WEIGHT_CASHOUT      = 0.15

KENYA_SCORE_THRESHOLD = 0.50   # Wallets above this are flagged as Kenyan


def load_transactions() -> pd.DataFrame:
    """Load all transactions from PostgreSQL into a DataFrame."""
    logger.info("Loading transactions from database...")
    sql = """
        SELECT tx_hash, sender, receiver, amount_usdt,
               timestamp, block_time
        FROM transactions
        ORDER BY timestamp DESC
    """
    df = pd.read_sql(sql, engine)
    logger.info(f"Loaded {len(df):,} transactions")
    return df


def signal_seed_match(all_wallets: set) -> dict:
    """Signal 1 — Direct match against known Kenyan seed wallets."""
    scores = {}
    for wallet in all_wallets:
        if wallet in KNOWN_KENYA_ENTITIES:
            scores[wallet] = KNOWN_KENYA_ENTITIES[wallet]["confidence"]
        else:
            scores[wallet] = 0.0
    logger.info(f"Seed match: {sum(1 for s in scores.values() if s > 0)} direct matches")
    return scores


def signal_interaction_frequency(df: pd.DataFrame, all_wallets: set) -> dict:
    """
    Signal 2 — How often does a wallet transact with known Kenyan entities?
    Score = (Kenya interactions) / (total interactions), capped at 1.0
    """
    kenya_addresses = set(KNOWN_KENYA_ENTITIES.keys())
    scores = {}

    for wallet in all_wallets:
        wallet_txs = df[(df["sender"] == wallet) | (df["receiver"] == wallet)]
        total = len(wallet_txs)
        if total == 0:
            scores[wallet] = 0.0
            continue

        kenya_interactions = wallet_txs[
            (wallet_txs["sender"].isin(kenya_addresses)) |
            (wallet_txs["receiver"].isin(kenya_addresses))
        ]
        scores[wallet] = min(len(kenya_interactions) / total, 1.0)

    high = sum(1 for s in scores.values() if s > 0.3)
    logger.info(f"Interaction frequency: {high} wallets with >30% Kenya interactions")
    return scores


def signal_timing_pattern(df: pd.DataFrame, all_wallets: set) -> dict:
    """
    Signal 3 — East Africa Time (UTC+3) activity pattern.
    Kenyan wallets tend to be most active during EAT business hours (8am–10pm).
    Score = proportion of transactions during EAT business hours.
    """
    scores = {}
    df = df.copy()
    df["block_time_parsed"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df["hour_eat"] = df["block_time_parsed"].dt.tz_convert("Africa/Nairobi").dt.hour

    for wallet in all_wallets:
        wallet_txs = df[(df["sender"] == wallet) | (df["receiver"] == wallet)]
        if len(wallet_txs) < 3:
            scores[wallet] = 0.0
            continue
        business_hours = wallet_txs[
            (wallet_txs["hour_eat"] >= 8) & (wallet_txs["hour_eat"] <= 22)
        ]
        scores[wallet] = len(business_hours) / len(wallet_txs)

    logger.info("Timing signal computed for all wallets")
    return scores


def signal_cashout_flow(df: pd.DataFrame, all_wallets: set) -> dict:
    """
    Signal 4 — Repeated cash-out flows ending at known Kenyan brokers.
    Score = 1.0 if wallet sends directly to a Kenyan broker, else 0.
    """
    kenya_brokers = {
        addr for addr, info in KNOWN_KENYA_ENTITIES.items()
        if info["type"] == "broker"
    }
    scores = {}

    for wallet in all_wallets:
        outflows = df[
            (df["sender"] == wallet) &
            (df["receiver"].isin(kenya_brokers))
        ]
        scores[wallet] = 1.0 if len(outflows) > 0 else 0.0

    cashout_count = sum(1 for s in scores.values() if s > 0)
    logger.info(f"Cashout flow: {cashout_count} wallets sending to Kenyan brokers")
    return scores


def compute_kenya_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Combine all four signals into a final Kenya confidence score
    using weighted average.
    """
    all_wallets = set(df["sender"].tolist() + df["receiver"].tolist())
    logger.info(f"Scoring {len(all_wallets):,} unique wallets...")

    s1 = signal_seed_match(all_wallets)
    s2 = signal_interaction_frequency(df, all_wallets)
    s3 = signal_timing_pattern(df, all_wallets)
    s4 = signal_cashout_flow(df, all_wallets)

    results = []
    for wallet in all_wallets:
        score = (
            WEIGHT_SEED_MATCH  * s1.get(wallet, 0) +
            WEIGHT_INTERACTION * s2.get(wallet, 0) +
            WEIGHT_TIMING      * s3.get(wallet, 0) +
            WEIGHT_CASHOUT     * s4.get(wallet, 0)
        )
        is_kenya = score >= KENYA_SCORE_THRESHOLD

        seed_info = KNOWN_KENYA_ENTITIES.get(wallet, {})
        results.append({
            "address":          wallet,
            "kenya_score":      round(score, 4),
            "is_kenya":         is_kenya,
            "label":            seed_info.get("label", "unknown"),
            "entity_type":      seed_info.get("type", "unknown"),
            "country":          "KE" if is_kenya else "unknown",
            "confidence":       round(score, 4),
            "signal_seed":      round(s1.get(wallet, 0), 4),
            "signal_interact":  round(s2.get(wallet, 0), 4),
            "signal_timing":    round(s3.get(wallet, 0), 4),
            "signal_cashout":   round(s4.get(wallet, 0), 4),
        })

    result_df = pd.DataFrame(results).sort_values("kenya_score", ascending=False)
    kenya_count = result_df[result_df["is_kenya"]].shape[0]
    logger.info(f"✅ Detected {kenya_count:,} Kenyan wallets out of {len(all_wallets):,} total")
    return result_df


def save_results(df: pd.DataFrame):
    """Save scored wallets to PostgreSQL and CSV."""
    kenya_wallets = df[df["is_kenya"]].copy()

    records = kenya_wallets[[
        "address", "label", "entity_type", "country", "confidence", "is_kenya", "kenya_score"
    ]].to_dict("records")

    if records:
        sql = text("""
            INSERT INTO wallet_labels
                (address, label, entity_type, country, confidence, is_kenya, kenya_score)
            VALUES
                (:address, :label, :entity_type, :country, :confidence, :is_kenya, :kenya_score)
            ON CONFLICT (address) DO UPDATE SET
                kenya_score = EXCLUDED.kenya_score,
                is_kenya    = EXCLUDED.is_kenya,
                confidence  = EXCLUDED.confidence,
                updated_at  = NOW()
        """)
        with engine.begin() as conn:
            conn.execute(sql, records)
        logger.info(f"✅ Saved {len(records)} Kenyan wallets to database")

    out_path = "data/processed/kenya_scored_wallets.csv"
    df.to_csv(out_path, index=False)
    logger.info(f"✅ Full scored wallet list saved to {out_path}")


def run_detection():
    logger.info("=" * 55)
    logger.info("  BlockWatchKE — Phase 2: Kenya Wallet Detection")
    logger.info("=" * 55)

    seed_wallet_labels()
    df = load_transactions()

    if df.empty:
        logger.warning("No transactions found. Run Phase 1 ingestion first.")
        return

    scored = compute_kenya_scores(df)
    save_results(scored)

    logger.info("\n── Top 10 Kenyan Wallets ──────────────────────")
    top = scored[scored["is_kenya"]].head(10)[
        ["address", "label", "kenya_score", "entity_type"]
    ]
    print(top.to_string(index=False))
    logger.info("─" * 55)


if __name__ == "__main__":
    run_detection()
