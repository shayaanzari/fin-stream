SET search_path TO silver;

CREATE TABLE IF NOT EXISTS transactions_dlq (
    dlq_id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    -- Bronze lineage (enough context to triage and re-classify)
    bronze_id        INT              NOT NULL,
    vendor           VARCHAR(255)     NOT NULL,
    amount           DOUBLE PRECISION NOT NULL,
    transaction_type VARCHAR(20)      NOT NULL,
    raw_text         TEXT             NOT NULL,

    -- Failure context
    -- Examples:
    --   "low_confidence:0.0000 (category=Groceries, threshold=0.5)"
    --   "low_confidence:0.3500 (category=Other, threshold=0.5)"
    failure_reason   TEXT             NOT NULL,

    created_at       TIMESTAMPTZ      NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Prevent duplicate DLQ entries for the same bronze row
CREATE UNIQUE INDEX IF NOT EXISTS uq_silver_dlq_bronze_id ON transactions_dlq (bronze_id);
CREATE INDEX IF NOT EXISTS idx_silver_dlq_created_at ON transactions_dlq (created_at DESC);
