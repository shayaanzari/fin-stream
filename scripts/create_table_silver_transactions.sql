SET search_path TO silver;

CREATE TABLE IF NOT EXISTS transactions (
    -- ── Identity & Lineage ────────────────────────────────────────────────────
    id                    INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    bronze_id             INT NOT NULL,                                      -- Lineage ref to bronze.transactions(id), no FK constraint

    -- ── Bronze columns (full copy) ────────────────────────────────────────────
    alert_datetime        TIMESTAMPTZ   NOT NULL,
    bank                  VARCHAR(50)   NOT NULL,
    vendor                VARCHAR(255)  NOT NULL,
    amount                DOUBLE PRECISION NOT NULL CHECK (amount >= 0),
    transaction_type      VARCHAR(20)   NOT NULL DEFAULT 'PURCHASE'
                              CHECK (transaction_type IN ('PURCHASE', 'CREDIT')),
    card_last_4           CHAR(4)       NOT NULL,
    is_card_not_present   BOOLEAN       NOT NULL DEFAULT FALSE,
    raw_text              TEXT          NOT NULL,
    bronze_created_at     TIMESTAMPTZ   NOT NULL,                            -- bronze.transactions.created_at, renamed to avoid ambiguity

    -- ── Silver enrichment columns ─────────────────────────────────────────────
    vendor_classification VARCHAR(100)  NOT NULL,                            -- LLM predicted category (e.g. 'Groceries', 'Dining')
    confidence_score      DOUBLE PRECISION CHECK (confidence_score BETWEEN 0 AND 1),  -- LLM confidence: 0.000–1.000
    llm		          VARCHAR(100)  NOT NULL,                            -- Model provenance for auditing/comparison
    classified_at         TIMESTAMPTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Prevent double-enrichment of the same bronze row
CREATE UNIQUE INDEX IF NOT EXISTS uq_silver_bronze_id
    ON transactions (bronze_id);

CREATE INDEX IF NOT EXISTS idx_silver_alert_datetime
    ON transactions (alert_datetime DESC);
CREATE INDEX IF NOT EXISTS idx_silver_vendor_classification
    ON transactions (vendor_classification);
CREATE INDEX IF NOT EXISTS idx_silver_bank
    ON transactions (bank);
