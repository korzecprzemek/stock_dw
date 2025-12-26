CREATE OR REPLACE VIEW mart.vw_intraday_last_30m AS
SELECT
    s.symbol,
    f.ts_utc,
    f.open,
    f.high,
    f.low,
    f.close,
    f.volume
FROM fact_price_intraday_raw f
JOIN dim_symbol s USING (symbol_id)
WHERE f.ts_utc >= now() - interval '30 minutes'
ORDER BY s.symbol, f.ts_utc;
