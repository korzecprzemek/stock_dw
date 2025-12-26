INSERT INTO fact_dividend (
    symbol_id,
    date_sk,
    ex_date,
    dividend,
    load_ts
)
SELECT
    ds.symbol_id,
    dd.date_sk,
    s.ex_date,
    s.dividend,
    now()
FROM stg.stg_dividend s
JOIN dim_symbol ds ON ds.symbol = s.symbol
JOIN dim_date   dd ON dd.date_value = s.ex_date
WHERE s.batch_id = :batch_id
ON CONFLICT (symbol_id, date_sk)
DO UPDATE SET
    dividend = EXCLUDED.dividend,
    load_ts  = EXCLUDED.load_ts;
