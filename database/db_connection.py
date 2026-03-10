import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from loguru import logger
from config.settings import DATABASE_URL

engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)

@contextmanager
def get_session():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception as exc:
        session.rollback()
        logger.error(f"DB error: {exc}")
        raise
    finally:
        session.close()

def test_connection():
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("✅ Database connection successful")
        return True
    except Exception as exc:
        logger.error(f"❌ Database connection failed: {exc}")
        return False

def bulk_insert_transactions(records):
    if not records:
        return 0
    sql = text("""
        INSERT INTO transactions
            (tx_hash, block_number, timestamp, sender, receiver, amount, token, token_contract)
        VALUES
            (:tx_hash, :block_number, :timestamp, :sender, :receiver, :amount, :token, :token_contract)
        ON CONFLICT (tx_hash) DO NOTHING
    """)
    with engine.begin() as conn:
        result = conn.execute(sql, records)
    return result.rowcount
