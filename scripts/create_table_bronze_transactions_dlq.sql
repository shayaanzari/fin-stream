CREATE TABLE bronze.transactions_dlq (
    dlq_id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    raw_text         TEXT NOT NULL,
    error_reason     TEXT NOT NULL,
    error_detail     TEXT,
    kafka_partition  INT,
    kafka_offset     BIGINT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);
