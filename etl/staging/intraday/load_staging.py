# etl/staging/intraday/load_staging.py

from __future__ import annotations

import datetime as dt
from typing import Iterable, Sequence, Tuple

import os
import psycopg2
from psycopg2.extras import execute_values


# 1. Typ jednego wiersza z fetch_intraday
Row = Tuple[str, dt.datetime, float, float, float, float, int]


# 2. Funkcja pomocnicza do uzyskania połączenia z Postgres
def get_pg_connection():
    """
    Zwraca połączenie do bazy PostgreSQL.
    Parametry pobierane są ze zmiennych środowiskowych:

        PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD

    Zmień to, jeśli w projekcie masz już swój sposób
    konfigurowania połączenia.
    """
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.getenv("PGDATABASE", "stock_dw"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "postgres"),
    )


# 3. Główna funkcja ładująca dane do stagingu
def load_intraday_staging(
    conn,
    rows: Sequence[Row],
) -> int:
    """
    Ładuje zebrane ticki intraday do tabeli stg_price_intraday.

    :param conn: otwarte połączenie psycopg2 do bazy.
    :param rows: sekwencja krotek:
                 (symbol, ts_utc, open, high, low, close, volume)
    :return: liczba wierszy załadowanych do stagingu.
    """
    if not rows:
        return 0

    sql = """
        INSERT INTO stg_price_intraday
            (symbol, ts_utc, open, high, low, close, volume)
        VALUES %s
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, rows)

    conn.commit()
    return len(rows)


# 4. Prosty CLI do ręcznego testu
if __name__ == "__main__":
    from fetch_intraday import fetch_intraday

    symbols = ["AAPL", "MSFT"]

    conn = get_pg_connection()
    try:
        ticks = fetch_intraday(symbols)
        inserted = load_intraday_staging(conn, ticks)
        print(f"Załadowano {inserted} wierszy do stg_price_intraday.")
    finally:
        conn.close()
