import time
from pathlib import Path

from fetch_intraday import fetch_intraday
from load_staging import get_pg_connection, load_intraday_staging


SQL_LOAD_FACT_INTRADAY = (
    Path(__file__)
    .resolve()
    .parents[2]  # wychodzi z intraday/ do etl/
    / "sql"
    / "facts"
    / "load_fact_price_intraday_raw.sql"
)

def load_active_symbols(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("""
                    SELECT symbol
                    FROM dim_symbol;
                    """)
        return [r[0] for r in cur.fetchall()]
def run_load_fact_intraday(conn):
    with conn.cursor() as cur:
        cur.execute(SQL_LOAD_FACT_INTRADAY.read_text())
    conn.commit()


def main():
    conn = get_pg_connection()
    symbols = load_active_symbols(conn)  # albo wczytane z configu

    try:
        while True:
            rows = fetch_intraday(symbols)

            if rows:
                load_intraday_staging(conn, rows)
                run_load_fact_intraday(conn)

            time.sleep(60)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
