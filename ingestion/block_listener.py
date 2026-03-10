"""
ingestion/block_listener.py
BlockWatchKE — Real-Time Block Listener

Continuously polls the Tron blockchain for new USDT transfers,
scores them instantly, and fires alerts in real time.

Usage:
    python ingestion/block_listener.py
    python ingestion/block_listener.py --interval 5
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import json
import argparse
from datetime import datetime, timezone

import requests
from loguru import logger
from sqlalchemy import text

from config.settings import (
    TRONGRID_API_KEY, TRONGRID_BASE_URL, USDT_CONTRACT,
    REQUEST_DELAY_SEC, PROCESSED_DIR
)
from database.db_connection import engine, bulk_insert_transactions
from detection.suspicious_patterns import (
    detect_large_transfers, detect_structuring, detect_high_velocity
)
import pandas as pd

os.makedirs("logs", exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

logger.remove()
logger.add(sys.stderr, level="INFO", colorize=True,
           format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
logger.add("logs/listener.log", level="DEBUG", rotation="100 MB", retention="7 days")

LISTENER_STATE_FILE = os.path.join(PROCESSED_DIR, "listener_state.json")
ALERT_THRESHOLD     = 60.0


# ── State management ───────────────────────────────────────────
def load_state() -> dict:
    if os.path.exists(LISTENER_STATE_FILE):
        with open(LISTENER_STATE_FILE) as f:
            return json.load(f)
    return {"last_fingerprint": None, "total_processed": 0, "last_seen_ts": 0}

def save_state(state: dict):
    with open(LISTENER_STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)


# ── API client ─────────────────────────────────────────────────
def fetch_latest_transfers(fingerprint: str | None = None) -> dict:
    url     = f"{TRONGRID_BASE_URL}/v1/contracts/{USDT_CONTRACT}/events"
    headers = {"Accept": "application/json"}
    if TRONGRID_API_KEY:
        headers["TRON-PRO-API-KEY"] = TRONGRID_API_KEY

    params = {
        "event_name": "Transfer",
        "limit":      200,
        "order_by":   "block_timestamp,desc",
    }
    if fingerprint:
        params["fingerprint"] = fingerprint

    resp = requests.get(url, headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


# ── Parser ─────────────────────────────────────────────────────
def parse_event(event: dict) -> dict | None:
    try:
        result = event.get("result", {})
        return {
            "tx_hash":        event["transaction_id"],
            "block_number":   int(event["block_number"]),
            "timestamp":      int(event["block_timestamp"]),
            "sender":         result.get("from", ""),
            "receiver":       result.get("to", ""),
            "amount":         int(result.get("value", 0)),
            "amount_usdt":    int(result.get("value", 0)) / 1_000_000,
            "token":          "USDT",
            "token_contract": USDT_CONTRACT,
        }
    except (KeyError, ValueError):
        return None


# ── Real-time risk scoring ─────────────────────────────────────
def score_live_batch(records: list[dict]) -> list[dict]:
    """
    Score a small batch of live transactions instantly.
    Uses simplified rules for speed — full engine runs offline.
    """
    alerts = []
    for r in records:
        score       = 0.0
        reasons     = []
        amount_usdt = r.get("amount_usdt", 0)

        # Rule 1 — Large transfer
        if amount_usdt > 50_000:
            score += 40.0
            reasons.append(f"large_transfer: ${amount_usdt:,.0f} USDT")

        # Rule 2 — Very large transfer
        if amount_usdt > 500_000:
            score += 25.0
            reasons.append(f"very_large: ${amount_usdt:,.0f} USDT")

        # Rule 3 — Round number (often indicates OTC/structured transfer)
        if amount_usdt > 1000 and amount_usdt % 1000 == 0:
            score += 10.0
            reasons.append("round_number_amount")

        if score >= ALERT_THRESHOLD:
            alerts.append({
                **r,
                "risk_score": min(score, 100),
                "reasons":    reasons,
            })

    return alerts


# ── Alert dispatcher ───────────────────────────────────────────
def fire_alert(alert: dict):
    """Log and store a real-time alert."""
    amount = alert.get("amount_usdt", 0)
    score  = alert.get("risk_score", 0)
    sender = alert.get("sender", "")[:20]
    recvr  = alert.get("receiver", "")[:20]

    logger.warning(
        f"🚨 ALERT | Score:{score:.0f} | "
        f"${amount:,.0f} USDT | "
        f"{sender}... → {recvr}... | "
        f"{', '.join(alert.get('reasons', []))}"
    )

    sql = text("""
        INSERT INTO alerts (tx_hash, wallet, alert_type, risk_score, status)
        VALUES (:tx_hash, :wallet, :alert_type, :risk_score, :status)
        ON CONFLICT DO NOTHING
    """)
    try:
        with engine.begin() as conn:
            conn.execute(sql, {
                "tx_hash":    alert["tx_hash"],
                "wallet":     alert["receiver"],
                "alert_type": "realtime_large_transfer",
                "risk_score": alert["risk_score"],
                "status":     "open",
            })
    except Exception as exc:
        logger.error(f"Failed to save alert: {exc}")


# ── Stats printer ──────────────────────────────────────────────
def print_stats(state: dict, new_tx: int, new_alerts: int):
    total = state["total_processed"]
    ts    = datetime.now(timezone.utc).strftime("%H:%M:%S")
    logger.info(
        f"[{ts}] "
        f"New: {new_tx:>4} tx | "
        f"Alerts: {new_alerts:>3} | "
        f"Total processed: {total:,}"
    )


# ── Main listener loop ─────────────────────────────────────────
def run_listener(poll_interval: int = 10):
    logger.info("=" * 55)
    logger.info("  BlockWatchKE — Real-Time Block Listener")
    logger.info(f"  Polling every {poll_interval}s")
    logger.info(f"  Contract: {USDT_CONTRACT}")
    logger.info("  Press Ctrl+C to stop")
    logger.info("=" * 55)

    state       = load_state()
    fingerprint = state.get("last_fingerprint")
    last_ts     = state.get("last_seen_ts", 0)

    consecutive_errors = 0

    try:
        while True:
            try:
                response = fetch_latest_transfers(fingerprint=None)
                consecutive_errors = 0
            except requests.HTTPError as exc:
                consecutive_errors += 1
                logger.warning(f"HTTP {exc.response.status_code} — waiting {poll_interval * 2}s")
                time.sleep(poll_interval * 2)
                continue
            except requests.Timeout:
                logger.warning("Request timed out — retrying...")
                time.sleep(poll_interval)
                continue
            except Exception as exc:
                consecutive_errors += 1
                logger.error(f"Unexpected error: {exc}")
                if consecutive_errors >= 10:
                    logger.critical("Too many errors — stopping listener.")
                    break
                time.sleep(poll_interval)
                continue

            events = response.get("data", [])
            if not events:
                time.sleep(poll_interval)
                continue

            # Filter to only new events since last poll
            new_records = []
            newest_ts   = last_ts

            for event in events:
                ts = int(event.get("block_timestamp", 0))
                if ts <= last_ts:
                    continue
                record = parse_event(event)
                if record:
                    new_records.append(record)
                    if ts > newest_ts:
                        newest_ts = ts

            if new_records:
                # Store to database
                db_records = [
                    {k: v for k, v in r.items() if k != "amount_usdt"}
                    for r in new_records
                ]
                inserted = bulk_insert_transactions(db_records)

                # Score live and fire alerts
                live_alerts = score_live_batch(new_records)
                for alert in live_alerts:
                    fire_alert(alert)

                state["total_processed"] += len(new_records)
                state["last_seen_ts"]     = newest_ts
                save_state(state)

                print_stats(state, len(new_records), len(live_alerts))
            else:
                logger.debug(f"No new transactions — sleeping {poll_interval}s")

            time.sleep(poll_interval)

    except KeyboardInterrupt:
        logger.info("\n⏹  Listener stopped by user.")
        logger.info(f"   Total processed this session: {state['total_processed']:,}")
        save_state(state)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="BlockWatchKE Real-Time Listener")
    parser.add_argument("--interval", type=int, default=10,
                        help="Polling interval in seconds (default: 10)")
    args = parser.parse_args()
    run_listener(poll_interval=args.interval)
