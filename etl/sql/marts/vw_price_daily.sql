CREATE SCHEMA IF NOT EXISTS mart;

CREATE OR REPLACE VIEW mart.vw_price_daily AS
SELECT
    s.symbol_id,
    s.symbol,
    s.name,
    d.date_sk,
    d.date_value,
    fp.open,
    fp.high,
    fp.low,
    fp.close,
    fp.adj_close,
    fp.volume
FROM fact_price fp
JOIN dim_symbol s USING (symbol_id)
JOIN dim_date d USING (date_sk);