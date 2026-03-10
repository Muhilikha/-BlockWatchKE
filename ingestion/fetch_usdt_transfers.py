import sys, os, json, time, argparse
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timezone
import requests
import pandas as pd
from tqdm import tqdm
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from config.settings import (
    TRONGRID_API_KEY, TRONGRID_BASE_URL, USDT_CONTRACT,
    BATCH_SIZE, REQUEST_DELAY_SEC, MAX_RETRIES, TARGET_TX_COUNT,
    RAW_DIR, PROCESSED_DIR
)
from database.db_connection import bulk_insert_transactions, test_connection

os.makedirs("logs", exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

logger.remove()
logger.add(sys.stderr, level="INFO", colorize=True,
           format="<green>{time:HH:mm:ss}</green> | <level>{level:<8}</level> | {message}")
logger.add("logs/ingestion.log", level="DEBUG", rotation="50 MB")

CHECKPOINT_FILE = os.path.join(PROCESSED_DIR, "checkpoint.json")

class TronGridClient:
    def __init__(self, api_key=""):
        self.base_url = TRONGRID_BASE_URL
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        if api_key:
            self.session.headers["TRON-PRO-API-KEY"] = api_key
        else:
            logger.warning("No API key set — requests will be rate-limited")

    @retry(
        stop=stop_after_attempt(MAX_RETRIES),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
        reraise=True
    )
    def get_trc20_transfers(self, contract=USDT_CONTRACT, limit=BATCH_SIZE, fingerprint=None):
        url = f"{self.base_url}/v1/contracts/{contract}/events"
        params = {"event_name": "Transfer", "limit": min(limit, 200), "order_by": "block_timestamp,desc"}
        if fingerprint:
            params["fingerprint"] = fingerprint
        resp = self.session.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()

def parse_event(event, contract=USDT_CONTRACT):
    try:
        result = event.get("result", {})
        return {
            "tx_hash":        event["transaction_id"],
            "block_number":   int(event["block_number"]),
            "timestamp":      int(event["block_timestamp"]),
            "sender":         result.get("from", ""),
            "receiver":       result.get("to", ""),
            "amount":         int(result.get("value", 0)),
            "token":          "USDT",
            "token_contract": contract,
        }
    except (KeyError, ValueError, TypeError):
        return None

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    return {"fingerprint": None, "total_collected": 0}

def save_checkpoint(fingerprint, total):
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"fingerprint": fingerprint, "total_collected": total,
                   "last_run": datetime.now(timezone.utc).isoformat()}, f, indent=2)

def flush_csv(records, batch_index):
    path = os.path.join(RAW_DIR, f"usdt_batch_{batch_index:05d}.csv")
    pd.DataFrame(records).to_csv(path, index=False)

def run_ingestion(target=TARGET_TX_COUNT, resume=True):
    logger.info("=" * 55)
    logger.info("  BlockWatchKE — Phase 1: USDT Ingestion")
    logger.info(f"  Target : {target:,} transactions")
    logger.info(f"  Contract: {USDT_CONTRACT}")
    logger.info("=" * 55)

    if not test_connection():
        logger.error("Cannot reach database. Check your .env file.")
        sys.exit(1)

    checkpoint     = load_checkpoint() if resume else {"fingerprint": None, "total_collected": 0}
    fingerprint    = checkpoint["fingerprint"]
    total          = checkpoint["total_collected"]
    batch_index    = total // BATCH_SIZE

    client = TronGridClient(api_key=TRONGRID_API_KEY)
    pbar   = tqdm(total=target, initial=total, unit="tx", desc="Collecting USDT transfers")
    buffer = []

    try:
        while total < target:
            try:
                response = client.get_trc20_transfers(fingerprint=fingerprint)
            except requests.HTTPError as exc:
                logger.warning(f"HTTP {exc.response.status_code} — backing off")
                time.sleep(5)
                continue
            except Exception as exc:
                logger.error(f"Fatal error: {exc}")
                break

            events = response.get("data", [])
            if not events:
                logger.info("No more events — ingestion complete.")
                break

            for event in events:
                record = parse_event(event)
                if record:
                    buffer.append(record)

            if len(buffer) >= BATCH_SIZE:
                batch = buffer[:BATCH_SIZE]
                buffer = buffer[BATCH_SIZE:]
                flush_csv(batch, batch_index)
                inserted = bulk_insert_transactions(batch)
                total += len(batch)
                batch_index += 1
                pbar.update(len(batch))
                save_checkpoint(fingerprint, total)
                logger.debug(f"Batch {batch_index}: inserted {inserted}/{len(batch)} | total {total:,}")

            fingerprint = response.get("meta", {}).get("fingerprint")
            if not fingerprint:
                logger.info("No fingerprint returned — end of available data.")
                break

            time.sleep(REQUEST_DELAY_SEC)

    finally:
        if buffer:
            flush_csv(buffer, batch_index)
            bulk_insert_transactions(buffer)
            total += len(buffer)
            pbar.update(len(buffer))
            save_checkpoint(fingerprint, total)
        pbar.close()

    logger.info("─" * 55)
    logger.info(f"✅ Done — {total:,} transactions collected")
    logger.info(f"   CSV files : data/raw/")
    logger.info(f"   Checkpoint: {CHECKPOINT_FILE}")
    logger.info("─" * 55)
    return total

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", type=int, default=TARGET_TX_COUNT)
    parser.add_argument("--resume", action="store_true", default=True)
    parser.add_argument("--no-resume", dest="resume", action="store_false")
    args = parser.parse_args()
    run_ingestion(target=args.target, resume=args.resume)
