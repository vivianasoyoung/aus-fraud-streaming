CREATE SCHEMA IF NOT EXISTS fraud;

CREATE TABLE IF NOT EXISTS fraud.flagged_transactions (
    id                  BIGSERIAL PRIMARY KEY,
    transaction_id      VARCHAR(36)  NOT NULL UNIQUE,
    account_id          VARCHAR(20)  NOT NULL,
    amount              NUMERIC(12, 2) NOT NULL CHECK (amount >= 0),
    merchant_category   VARCHAR(50),
    channel             VARCHAR(20),
    fraud_reasons       TEXT[]       NOT NULL,
    risk_score          INTEGER      NOT NULL CHECK (risk_score BETWEEN 0 AND 100),
    event_time          TIMESTAMPTZ  NOT NULL,
    processed_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_flagged_account_time
    ON fraud.flagged_transactions (account_id, event_time DESC);

CREATE INDEX IF NOT EXISTS idx_flagged_processed_at
    ON fraud.flagged_transactions (processed_at DESC);
