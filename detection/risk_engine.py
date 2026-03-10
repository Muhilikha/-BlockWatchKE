"""
detection/risk_engine.py
BlockWatchKE — Phase 3: Risk Scoring Engine
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from loguru import logger
from sqlalchemy import text
from database.db_connection import engine
from detection.suspicious_patterns import load_transactions, run_all_rules

ALERT_THRESHOLD = 60.0


def compute_risk_scores(df: pd.DataFrame, flagged: pd.DataFrame) -> pd.DataFrame:
    if flagged.empty:
        logger.info("No flagged transactions to score.")
        return pd.DataFrame()

    score_map = (
        flagged.groupby("tx_hash")["score_contribution"]
        .sum()
        .reset_index()
        .rename(columns={"score_contribution": "total_score"})
    )
    score_map["total_score"] = score_map["total_score"].clip(upper=100.0)

    rule_pivot = (
        flagged.pivot_table(
            index="tx_hash",
            columns="rule",
            values="score_contribution",
            aggfunc="sum",
        )
        .fillna(0)
        .reset_index()
    )

    for col in ["large_transfer", "structuring", "multi_hop", "high_velocity"]:
        if col not in rule_pivot.columns:
            rule_pivot[col] = 0.0

    scores = score_map.merge(rule_pivot, on="tx_hash", how="left")
    scores["is_alert"] = scores["total_score"] >= ALERT_THRESHOLD

    alert_count = scores["is_alert"].sum()
    logger.info(f"✅ Scored {len(scores):,} transactions — {alert_count} alerts generated")
    return scores


def save_risk_scores(scores: pd.DataFrame):
    if scores.empty:
        return

    records = scores[[
        "tx_hash", "total_score", "large_transfer",
        "structuring", "multi_hop", "high_velocity"
    ]].to_dict("records")

    sql = text("""
        INSERT INTO risk_scores
            (tx_hash, total_score, large_transfer, structuring, multi_hop)
        VALUES
            (:tx_hash, :total_score, :large_transfer, :structuring, :multi_hop)
        ON CONFLICT (tx_hash) DO UPDATE SET
            total_score    = EXCLUDED.total_score,
            large_transfer = EXCLUDED.large_transfer,
            multi_hop      = EXCLUDED.multi_hop,
            structuring    = EXCLUDED.structuring,
            scored_at      = NOW()
    """)

    with engine.begin() as conn:
        conn.execute(sql, records)
    logger.info(f"✅ Saved {len(records)} risk scores to database")


def save_alerts(scores: pd.DataFrame, df: pd.DataFrame):
    alerts_df = scores[scores["is_alert"]].merge(
        df[["tx_hash", "sender", "receiver", "amount_usdt"]],
        on="tx_hash", how="left"
    )
    if alerts_df.empty:
        logger.info("No alerts to save.")
        return

    records = []
    for _, row in alerts_df.iterrows():
        records.append({
            "tx_hash":    row["tx_hash"],
            "wallet":     row["receiver"],
            "alert_type": "composite_risk",
            "risk_score": row["total_score"],
            "status":     "open",
        })

    sql = text("""
        INSERT INTO alerts (tx_hash, wallet, alert_type, risk_score, status)
        VALUES (:tx_hash, :wallet, :alert_type, :risk_score, :status)
        ON CONFLICT DO NOTHING
    """)
    with engine.begin() as conn:
        conn.execute(sql, records)
    logger.info(f"✅ {len(records)} alerts saved to database")


def print_summary(scores: pd.DataFrame, df: pd.DataFrame):
    if scores.empty:
        return
    top = (
        scores[scores["is_alert"]]
        .merge(df[["tx_hash", "sender", "receiver", "amount_usdt"]], on="tx_hash")
        .sort_values("total_score", ascending=False)
        .head(10)
    )[["tx_hash", "sender", "receiver", "amount_usdt", "total_score"]]

    logger.info("\n── Top 10 High-Risk Transactions ──────────────")
    print(top.to_string(index=False))
    logger.info("─" * 55)


def run_risk_engine():
    logger.info("=" * 55)
    logger.info("  BlockWatchKE — Phase 3: Risk Scoring Engine")
    logger.info("=" * 55)

    df = load_transactions()
    if df.empty:
        logger.warning("No transactions found. Run Phase 1 first.")
        return

    flagged = run_all_rules(df)
    scores  = compute_risk_scores(df, flagged)

    if not scores.empty:
        save_risk_scores(scores)
        save_alerts(scores, df)
        print_summary(scores, df)

        out_path = "data/processed/risk_scores.csv"
        scores.to_csv(out_path, index=False)
        logger.info(f"✅ Risk scores exported to {out_path}")

    logger.info("Phase 3 complete.")


if __name__ == "__main__":
    run_risk_engine()
