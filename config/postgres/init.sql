-- Source schema, and the Postgres settings Debezium's logical decoding needs.

CREATE TABLE IF NOT EXISTS merchants (
    merchant_id   BIGINT PRIMARY KEY,
    merchant_name TEXT NOT NULL,
    country_code  TEXT NOT NULL,
    category      TEXT NOT NULL,
    created_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS payments (
    payment_id     BIGINT PRIMARY KEY,
    merchant_id    BIGINT NOT NULL REFERENCES merchants(merchant_id),
    shopper_id     BIGINT NOT NULL,
    -- NUMERIC, not DOUBLE PRECISION. Debezium encodes this as a base64
    -- unscaled integer, which the decoder handles; a float column would have
    -- lost cents before the pipeline ever saw it.
    amount         NUMERIC(12, 2) NOT NULL,
    currency       TEXT NOT NULL,
    payment_method TEXT NOT NULL,
    payment_status TEXT NOT NULL,
    country_code   TEXT NOT NULL,
    created_at     TIMESTAMP NOT NULL,
    updated_at     TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS refunds (
    refund_id     BIGINT PRIMARY KEY,
    payment_id    BIGINT NOT NULL REFERENCES payments(payment_id),
    refund_amount NUMERIC(12, 2) NOT NULL,
    refund_reason TEXT NOT NULL,
    created_at    TIMESTAMP NOT NULL
);

-- REPLICA IDENTITY FULL makes Postgres write the *whole* old row to the WAL on
-- an update or delete. Without it Debezium's `before` image contains only the
-- primary key, so a delete arrives with no columns to work with and any
-- downstream logic that reads the deleted row's attributes breaks.
ALTER TABLE payments  REPLICA IDENTITY FULL;
ALTER TABLE merchants REPLICA IDENTITY FULL;
ALTER TABLE refunds   REPLICA IDENTITY FULL;

INSERT INTO merchants (merchant_id, merchant_name, country_code, category) VALUES
    (1, 'Northwind Fashion',   'NL', 'fashion'),
    (2, 'Blue Harbor Travel',  'US', 'travel'),
    (3, 'Green Basket Foods',  'DE', 'grocery'),
    (4, 'Cedar Health Market', 'CA', 'health'),
    (5, 'Metro Home Studio',   'GB', 'home')
ON CONFLICT (merchant_id) DO NOTHING;

-- Deterministic synthetic history: no RNG, so a rebuild reproduces the same
-- dataset and test expectations stay stable.
INSERT INTO payments (
    payment_id, merchant_id, shopper_id, amount, currency,
    payment_method, payment_status, country_code, created_at, updated_at
)
SELECT
    1000 + gs,
    ((gs - 1) % 5) + 1,
    500 + ((gs - 1) % 400) + 1,
    ROUND((25 + ((gs * 17) % 475) + (((gs * 13) % 100)::NUMERIC / 100)), 2)::NUMERIC(12, 2),
    (ARRAY['EUR','USD','GBP','CAD','AUD','CHF'])[((gs - 1) % 6) + 1],
    (ARRAY['card','paypal','apple_pay','bank_transfer','google_pay'])[((gs - 1) % 5) + 1],
    (ARRAY['authorized','failed','authorized','pending','refunded','authorized','chargeback','cancelled'])[((gs - 1) % 8) + 1],
    (ARRAY['NL','US','DE','BE','FR','GB','CA','ES','AU','CH'])[((gs - 1) % 10) + 1],
    NOW() - (((gs - 1) % 2160) || ' hours')::INTERVAL,
    NOW() - (((gs - 1) % 2160) || ' hours')::INTERVAL
FROM generate_series(1, 5000) AS gs
ON CONFLICT (payment_id) DO NOTHING;
