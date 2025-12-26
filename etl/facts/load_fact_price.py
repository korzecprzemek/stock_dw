from sqlalchemy import text

SQL = """
INSERT INTO fact_price (
    symbol_id, date_sk, date, open, high, low, close, adj_close, volume, load_ts
)
SELECT
    ds.symbol_id,
    dd.date_sk,
    s.date_value,
    s.open,
    s.high,
    s.low,
    s.close,
    s.adj_close,
    s.volume,
    now()
FROM stg.stg_price s
JOIN dim_symbol ds ON ds.symbol = s.symbol
JOIN dim_date dd ON dd.date_value = s.date_value
WHERE s.batch_id = :batch_id
ON CONFLICT (symbol_id, date_sk)
DO UPDATE SET
    open      = EXCLUDED.open,
    high      = EXCLUDED.high,
    low       = EXCLUDED.low,
    close     = EXCLUDED.close,
    adj_close = EXCLUDED.adj_close,
    volume    = EXCLUDED.volume,
    load_ts   = EXCLUDED.load_ts;
"""

def load_fact_price(engine, batch_id):
    with engine.begin() as conn:
        conn.execute(text(SQL), {"batch_id": batch_id})
