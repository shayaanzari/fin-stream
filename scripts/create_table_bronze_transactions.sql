SET search_path TO bronze; -- You can alternatively do CREATE TABLE bronze.transactions, but I like this more

CREATE TABLE IF NOT EXISTS transactions (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    alert_datetime TIMESTAMPTZ NOT NULL,
    bank VARCHAR(50) NOT NULL,                                            -- Entity issuing the card/alert (e.g. 'Citi')
    vendor VARCHAR(255) NOT NULL,                                         -- Name of Merchant/Vendor where money was spent
    amount NUMERIC(10, 2) NOT NULL CHECK (amount >= 0),                    -- Monetary amount
    transaction_type VARCHAR(20) NOT NULL DEFAULT 'PURCHASE' CHECK (transaction_type IN ('PURCHASE', 'CREDIT')),
    card_last_4 CHAR(4) NOT NULL,                                         -- String to preserve leading zeros "e.g. ending in 0123"
    is_card_not_present BOOLEAN NOT NULL DEFAULT FALSE,                   -- Only true when detecting "Card not present"
    raw_text TEXT NOT NULL,                                               -- Unparsed data from Redpanda Bronze Layer
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_transactions_dedup UNIQUE (bank, card_last_4, alert_datetime, amount) -- Postgres requires this, otherwise you get the error: there is no unique or exclusion constraint matching the ON CONFLICT specification
);
      
CREATE INDEX IF NOT EXISTS idx_transactions_alert_datetime ON transactions (alert_datetime DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_bank ON transactions (bank);
CREATE INDEX IF NOT EXISTS idx_transactions_vendor ON transactions (vendor);
