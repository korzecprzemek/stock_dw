from sqlalchemy import text

SQL = """
INSERT INTO dim_symbol (symbol)
SELECT DISTINCT s.symbol
FROM stg.stg_price s
LEFT JOIN dim_symbol d ON d.symbol = s.symbol
WHERE d.symbol_id IS NULL;
"""

def load_dim_symbol(engine):
    with engine.begin() as conn:
        conn.execute(text(SQL))
