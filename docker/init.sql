CREATE SCHEMA IF NOT EXISTS fraud;

CREATE TABLE IF NOT EXISTS fraud.flagged_transactions (
    id                  SERIAL PRIMARY KEY,
    transaction_id      VARCHAR(36),
    account_id          VARCHAR(20),
    amount              NUMERIC(12,2),
    merchant_category   VARCHAR(50),
    channel             VARCHAR(20),
    fraud_reason        TEXT,
    risk_score          INTEGER,
    flagged_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fraud.transaction_velocity (
    account_id          VARCHAR(20),
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    transaction_count   INTEGER,
    total_amount        NUMERIC(12,2),
    PRIMARY KEY (account_id, window_start)
);
