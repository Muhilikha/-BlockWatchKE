CREATE TABLE IF NOT EXISTS transactions (
    tx_hash         TEXT            PRIMARY KEY,
    block_number    BIGINT          NOT NULL,
    timestamp       BIGINT          NOT NULL,
    sender          TEXT            NOT NULL,
    receiver        TEXT            NOT NULL,
    amount          NUMERIC(28,6)   NOT NULL,
    amount_usdt     NUMERIC(20,2)   GENERATED ALWAYS AS (amount / 1000000.0) STORED,
    token           TEXT            NOT NULL DEFAULT 'USDT',
    token_contract  TEXT            NOT NULL,
    ingested_at     TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_tx_sender    ON transactions (sender);
CREATE INDEX IF NOT EXISTS idx_tx_receiver  ON transactions (receiver);
CREATE INDEX IF NOT EXISTS idx_tx_timestamp ON transactions (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_tx_amount    ON transactions (amount_usdt DESC);

CREATE TABLE IF NOT EXISTS wallet_labels (
    address         TEXT        PRIMARY KEY,
    label           TEXT,
    entity_type     TEXT,
    country         TEXT,
    confidence      FLOAT       DEFAULT 0.0,
    is_kenya        BOOLEAN     DEFAULT FALSE,
    kenya_score     FLOAT       DEFAULT 0.0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS risk_scores (
    id              BIGSERIAL   PRIMARY KEY,
    tx_hash         TEXT        NOT NULL REFERENCES transactions(tx_hash),
    total_score     FLOAT       NOT NULL DEFAULT 0.0,
    large_transfer  FLOAT       DEFAULT 0.0,
    multi_hop       FLOAT       DEFAULT 0.0,
    structuring     FLOAT       DEFAULT 0.0,
    is_alert        BOOLEAN     GENERATED ALWAYS AS (total_score >= 60) STORED,
    scored_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(tx_hash)
);

CREATE TABLE IF NOT EXISTS alerts (
    id          BIGSERIAL   PRIMARY KEY,
    tx_hash     TEXT        REFERENCES transactions(tx_hash),
    wallet      TEXT,
    alert_type  TEXT        NOT NULL,
    risk_score  FLOAT,
    status      TEXT        NOT NULL DEFAULT 'open',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS ingestion_checkpoints (
    id               SERIAL  PRIMARY KEY,
    token_contract   TEXT    NOT NULL UNIQUE,
    last_fingerprint TEXT,
    total_collected  BIGINT  DEFAULT 0,
    last_run_at      TIMESTAMPTZ,
    status           TEXT    DEFAULT 'idle'
);

INSERT INTO ingestion_checkpoints (token_contract, status)
VALUES ('TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t', 'idle')
ON CONFLICT (token_contract) DO NOTHING;
