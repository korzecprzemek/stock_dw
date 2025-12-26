# etl/staging/intraday/fetch_intraday.py

from __future__ import annotations

import pandas as pd
import datetime as dt

from typing import Iterable, List, Tuple

from yfinance.exceptions import YFPricesMissingError
import yfinance as yf


Row = Tuple[str, dt.datetime, float, float, float, float, int]
# (symbol, ts_utc, open, high, low, close, volume)


def fetch_intraday(symbols: Iterable[str]) -> List[Row]:
    """
    Pobiera najnowsze dane intraday (1m) dla podanych tickerów z yfinance.

    Zwraca listę krotek:
        (symbol, ts_utc, open, high, low, close, volume)

    Uwaga:
    - Funkcja jest czysta: nie ma żadnych operacji na bazie.
    - Zakładamy, że będzie wywoływana co ~60 sekund w pętli streamingowej.
    - Dla uproszczenia pobieramy małe okno czasowe (ostatnie 2 minuty)
      i bierzemy ostatnią świeczkę dla każdego symbolu.
      Ewentualne duplikaty wytnie ON CONFLICT w loaderze do fact.
    """
    symbols = list(symbols)
    if not symbols:
        return []

    end = dt.datetime.utcnow()
    start = end - dt.timedelta(minutes=2)

    try:
        data = yf.download(
            tickers=" ".join(symbols),
            period="1d",
            interval="1m",
            group_by="ticker",
            auto_adjust=False,
            progress=False,
            threads=True,
        )
    except YFPricesMissingError:
        return []

    rows: List[Row] = []

    # yfinance zwraca różną strukturę dla 1 tickera i wielu tickerów
    for symbol in symbols:
        if len(symbols) == 1:
            df = data
        else:
            # gdy tickery są grupowane po tickerze
            if symbol not in data:
                continue
            df = data[symbol]

        if df is None or df.empty:
            continue

        # bierzemy ostatni dostępny wiersz
        last_row = df.iloc[-1]
        ts_utc = df.index[-1].to_pydatetime()

        rows.append(
            (
                symbol,
                ts_utc,
                float(last_row["Open"]),
                float(last_row["High"]),
                float(last_row["Low"]),
                float(last_row["Close"]),
                int(last_row["Volume"]) if not pd.isna(last_row["Volume"]) else 0,
            )
        )

    return rows
