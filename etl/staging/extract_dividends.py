import yfinance as yf
import pandas as pd


def extract_dividends(tickers, start, end):
    """
    Zwraca dataframe:
      symbol, ex_date, dividend
    w zakresie dat [start, end].
    """

    if isinstance(tickers, str):
        tickers = [tickers]

    frames = []

    for symbol in tickers:
        t = yf.Ticker(symbol)
        divs = t.dividends  # Series: index = DatetimeIndex, values = dividend amount

        if divs is None or divs.empty:
            continue

        df = divs.reset_index()
        df.columns = ["ex_date", "dividend"]
        df["symbol"] = symbol

        df["ex_date"] = pd.to_datetime(df["ex_date"]).dt.date

        if start:
            df = df[df["ex_date"] >= pd.to_datetime(start).date()]
        if end:
            df = df[df["ex_date"] <= pd.to_datetime(end).date()]

        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["symbol", "ex_date", "dividend"])

    return pd.concat(frames, ignore_index=True)[["symbol", "ex_date", "dividend"]]
