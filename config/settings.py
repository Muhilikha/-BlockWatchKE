import os
from dotenv import load_dotenv
load_dotenv()

DB_HOST     = os.getenv("DB_HOST", "localhost")
DB_PORT     = int(os.getenv("DB_PORT", "5432"))
DB_NAME     = os.getenv("DB_NAME", "blockwatchke")
DB_USER     = os.getenv("DB_USER", "bwke_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "blockwatch2024")

DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

TRONGRID_API_KEY  = os.getenv("TRONGRID_API_KEY", "")
TRONGRID_BASE_URL = "https://api.trongrid.io"
USDT_CONTRACT     = "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"

BATCH_SIZE        = 200
REQUEST_DELAY_SEC = 0.5
MAX_RETRIES       = 5
TARGET_TX_COUNT   = 100000

BASE_DIR       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR        = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR  = os.path.join(BASE_DIR, "data", "processed")
