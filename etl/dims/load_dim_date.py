from sqlalchemy import text

SQL = """
INSERT INTO dim_date (date_sk, date_value, year, month, day)
SELECT DISTINCT
    (EXTRACT(YEAR FROM s.date_value)::int * 10000) +
    (EXTRACT(MONTH FROM s.date_value)::int * 100) +
    (EXTRACT(DAY FROM s.date_value)::int),
    s.date_value,
    EXTRACT(YEAR FROM s.date_value)::int,
    EXTRACT(MONTH FROM s.date_value)::int,
    EXTRACT(DAY FROM s.date_value)::int
FROM stg.stg_price s
LEFT JOIN dim_date d ON d.date_value = s.date_value
WHERE d.date_value IS NULL;
"""

def load_dim_date(engine):
    with engine.begin() as conn:
        conn.execute(text(SQL))
