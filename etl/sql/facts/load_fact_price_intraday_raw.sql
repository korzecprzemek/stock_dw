-- etl/sql/facts/load_fact_price_intraday_raw.sql

-- 1. Załaduj z STAGING do FACT z mapowaniem symbol -> symbol_id
--    i z obsługą UPSERT (idempotentne ładowanie mikro-batchy).

INSERT INTO fact_price_intraday_raw (
    symbol_id,
    ts_utc,
    open,
    high,
    low,
    close,
    volume
)
SELECT
    ds.symbol_id,
    s.ts_utc,
    s.open,
    s.high,
    s.low,
    s.close,
    s.volume
FROM stg_price_intraday s
JOIN dim_symbol ds
  ON ds.symbol = s.symbol
ON CONFLICT (symbol_id, ts_utc) DO UPDATE
SET
    open   = EXCLUDED.open,
    high   = EXCLUDED.high,
    low    = EXCLUDED.low,
    close  = EXCLUDED.close,
    volume = EXCLUDED.volume;

-- 2. Wyczyść staging po udanym załadowaniu,
--    żeby kolejne mikro-batche nie dublowały danych.

TRUNCATE TABLE stg_price_intraday;
