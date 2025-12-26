import time
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

# ---------- KONFIG ----------
REFRESH_SECONDS = 5

engine = create_engine(
    "postgresql://postgres:postgres@localhost:5432/stock_dw"
)

st.set_page_config(layout="wide")
st.title("📈 LIVE – Intraday Prices (micro-batch)")

symbols_df = pd.read_sql(
    "SELECT symbol FROM dim_symbol ORDER BY symbol",
    engine
)

# ---------- UI ----------
symbol = st.selectbox(
    "Symbol",
    symbols_df["symbol"].tolist(),
    index=0
)

placeholder = st.empty()

# ---------- LOOP ----------
while True:
    df = pd.read_sql(
        """
        SELECT
            ts_utc,
            open,
            high,
            low,
            close,
            volume
        FROM mart.vw_intraday_last_30m
        WHERE symbol = %(symbol)s
        ORDER BY ts_utc
        """,
        engine,
        params={"symbol": symbol},
    )

    with placeholder.container():
        st.subheader(f"{symbol} – last 30 minutes")

        if df.empty:
            st.warning("Brak danych (poza godzinami sesji?)")
        else:
            df["delta"] = df["close"].diff()
            st.line_chart(
                df.set_index("ts_utc")["delta"],
                height=300
            )

            st.dataframe(
                df.tail(10),
                use_container_width=True
            )

    time.sleep(REFRESH_SECONDS)
