CREATE OR REPLACE VIEW mart.vw_dividend_daily AS
SELECT
    s.symbol_id,
    s.symbol,
    s.name,
    d.date_sk,
    d.date_value AS ex_date,
    fd.dividend
FROM fact_dividend fd
JOIN dim_symbol s USING (symbol_id)
JOIN dim_date   d USING (date_sk);
