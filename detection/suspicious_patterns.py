"""
detection/suspicious_patterns.py
BlockWatchKE — Phase 3: Suspicious Pattern Detection Rules

Rule 1: Large Transfer       — amount > 50,000 USDT
Rule 2: Structuring          — many small transfers, same sender, short window
Rule 3: Multi-hop Transfer   — A→B→C→D within minutes
Rule 4: Network Proximity    — connected to flagged wallets
Rule 5: High Velocity        — too many transfers in short time
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from loguru import logger
from sqlalchemy import text
from database.db_connection import engine

LARGE_TRANSFER_THRESHOLD = 50_000
STRUCTURING_WINDOW_SEC   = 3_600   # 1 hour
STRUCTURING_MIN_COUNT    = 5
STRUCTURING_MAX_AMOUNT   = 10_000
MULTIHOP_WINDOW_SEC      = 600     # 10 minutes
VELOCITY_WINDOW_SEC      = 3_600
VELOCITY_MAX_COUNT       = 20


def load_transactions() -> pd.DataFrame:
    logger.info("Loading transactions for pattern analysis...")
    df = pd.read_sql("SELECT tx_hash, sender, receiver, amount_usdt, timestamp FROM transactions ORDER BY timestamp ASC", engine)
    logger.info(f"Loaded {len(df):,} transactions")
    return df


def detect_large_transfers(df: pd.DataFrame) -> pd.DataFrame:
    """Rule 1 — Flag any transfer above 50,000 USDT."""
    flagged = df[df["amount_usdt"] > LARGE_TRANSFER_THRESHOLD].copy()
    flagged["rule"]        = "large_transfer"
    flagged["description"] = flagged["amount_usdt"].apply(
        lambda x: f"Large transfer of {x:,.2f} USDT exceeds threshold"
    )
    flagged["score_contribution"] = 40.0
    logger.info(f"Rule 1 — Large transfers: {len(flagged)} flagged")
    return flagged


def detect_structuring(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rule 2 — Structuring: many small transfers from same sender
    within a 1-hour window, each below 10,000 USDT.
    Classic pattern used to avoid reporting thresholds.
    """
    flagged_hashes = []
    small = df[df["amount_usdt"] < STRUCTURING_MAX_AMOUNT].copy()
    small = small.sort_values("timestamp")

    for sender, group in small.groupby("sender"):
        group = group.sort_values("timestamp")
        timestamps = group["timestamp"].tolist()

        for i in range(len(timestamps)):
            window = [
                t for t in timestamps[i:]
                if t - timestamps[i] <= STRUCTURING_WINDOW_SEC * 1000
            ]
            if len(window) >= STRUCTURING_MIN_COUNT:
                window_txs = group[
                    (group["timestamp"] >= timestamps[i]) &
                    (group["timestamp"] <= timestamps[i] + STRUCTURING_WINDOW_SEC * 1000)
                ]
                flagged_hashes.extend(window_txs["tx_hash"].tolist())
                break

    if not flagged_hashes:
        logger.info("Rule 2 — Structuring: 0 flagged")
        return pd.DataFrame()

    flagged = df[df["tx_hash"].isin(set(flagged_hashes))].copy()
    flagged["rule"]               = "structuring"
    flagged["description"]        = "Part of structuring pattern — multiple small transfers within 1 hour"
    flagged["score_contribution"] = 35.0
    logger.info(f"Rule 2 — Structuring: {len(flagged)} transactions flagged")
    return flagged


def detect_multihop(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rule 3 — Multi-hop: A→B→C→D chain within 10 minutes.
    Funds pass through multiple wallets rapidly to obscure origin.
    """
    flagged_hashes = []
    df_sorted = df.sort_values("timestamp")
    receiver_map = df_sorted.groupby("receiver")

    for _, row in df_sorted.iterrows():
        chain = [row["tx_hash"]]
        current_receiver = row["receiver"]
        current_time     = row["timestamp"]

        for _ in range(3):
            if current_receiver not in receiver_map.groups:
                break
            next_txs = receiver_map.get_group(current_receiver)
            next_txs = next_txs[
                (next_txs["sender"] == current_receiver) &
                (next_txs["timestamp"] > current_time) &
                (next_txs["timestamp"] <= current_time + MULTIHOP_WINDOW_SEC * 1000)
            ]
            if next_txs.empty:
                break
            next_tx = next_txs.iloc[0]
            chain.append(next_tx["tx_hash"])
            current_receiver = next_tx["receiver"]
            current_time     = next_tx["timestamp"]

        if len(chain) >= 3:
            flagged_hashes.extend(chain)

    if not flagged_hashes:
        logger.info("Rule 3 — Multi-hop: 0 flagged")
        return pd.DataFrame()

    flagged = df[df["tx_hash"].isin(set(flagged_hashes))].copy()
    flagged["rule"]               = "multi_hop"
    flagged["description"]        = "Part of multi-hop chain — funds moved through 3+ wallets within 10 minutes"
    flagged["score_contribution"] = 30.0
    logger.info(f"Rule 3 — Multi-hop: {len(flagged)} transactions flagged")
    return flagged


def detect_high_velocity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rule 4 — Velocity: wallet sends more than 20 transactions
    within a 1-hour window.
    """
    flagged_hashes = []
    df_sorted = df.sort_values("timestamp")

    for sender, group in df_sorted.groupby("sender"):
        group = group.sort_values("timestamp")
        timestamps = group["timestamp"].tolist()

        for i in range(len(timestamps)):
            window = [
                t for t in timestamps[i:]
                if t - timestamps[i] <= VELOCITY_WINDOW_SEC * 1000
            ]
            if len(window) >= VELOCITY_MAX_COUNT:
                window_txs = group[
                    (group["timestamp"] >= timestamps[i]) &
                    (group["timestamp"] <= timestamps[i] + VELOCITY_WINDOW_SEC * 1000)
                ]
                flagged_hashes.extend(window_txs["tx_hash"].tolist())
                break

    if not flagged_hashes:
        logger.info("Rule 4 — High velocity: 0 flagged")
        return pd.DataFrame()

    flagged = df[df["tx_hash"].isin(set(flagged_hashes))].copy()
    flagged["rule"]               = "high_velocity"
    flagged["description"]        = f"High velocity — more than {VELOCITY_MAX_COUNT} transfers within 1 hour"
    flagged["score_contribution"] = 25.0
    logger.info(f"Rule 4 — High velocity: {len(flagged)} transactions flagged")
    return flagged


def run_all_rules(df: pd.DataFrame) -> pd.DataFrame:
    """Run all detection rules and return combined flagged transactions."""
    results = []
    for detect_fn in [
        detect_large_transfers,
        detect_structuring,
        detect_multihop,
        detect_high_velocity,
    ]:
        result = detect_fn(df)
        if not result.empty:
            results.append(result)

    if not results:
        logger.info("No suspicious patterns detected")
        return pd.DataFrame()

    combined = pd.concat(results, ignore_index=True)
    logger.info(f"Total flagged transactions across all rules: {len(combined):,}")
    return combined
