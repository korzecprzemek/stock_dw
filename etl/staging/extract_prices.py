import yfinance as yf
import pandas as pd


def extract_prices(tickers, start, end):
    """
    Extract prices from yfinance and normalize to a flat DataFrame:

    columns: symbol, date_value, open, high, low, close, adj_close, volume
    """
    # Wymuszamy auto_adjust=False, żeby mieć kolumnę 'Adj Close'
    df = yf.download(
        tickers,
        start=start,
        end=end,
        auto_adjust=False,   # ← kluczowe
        progress=True,
    )

    # Przenosimy indeks Date do kolumny
    df = df.reset_index().rename(columns={"Date": "date_value"})

    # ===== CASE 1: MultiIndex kolumn (wiele tickerów) =====
    if isinstance(df.columns, pd.MultiIndex):
        # MultiIndex ma poziomy (field, symbol)
        df = df.set_index("date_value")
        df.columns = df.columns.set_names(["field", "symbol"])

        # Stack po poziomie 'symbol' → zwykłe kolumny
        df = df.stack(level="symbol").reset_index()   # date_value, symbol, Open, High, ...

        # Ujednolicamy nazwy
        df = df.rename(columns=str.lower)             # open, high, low, close, adj close, volume
        if "adj close" in df.columns:
            df = df.rename(columns={"adj close": "adj_close"})
        else:
            # fallback: jeśli nie ma adj close, użyj close
            df["adj_close"] = df["close"]

        return df[["symbol", "date_value", "open", "high", "low", "close", "adj_close", "volume"]]

    # ===== CASE 2:
