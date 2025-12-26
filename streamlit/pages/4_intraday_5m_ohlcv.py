import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
import plotly.graph_objects as go
from datetime import date

# ---------- KONFIG ----------

st.set_page_config(
    page_title="Intraday 5m OHLCV",
    layout="wide",
)

st.title("📈 Intraday 5m OHLCV")

engine = create_engine(
    "postgresql://postgres:postgres@localhost:5432/stock_dw"
)

# ---------- DANE POMOCNICZE ----------

@st.cache_data(ttl=60)
def load_symbols():
    query = "select symbol from dim_symbol order by symbol"
    with engine.connect() as conn:
        return pd.read_sql(query, conn)["symbol"].tolist()


@st.cache_data(ttl=30)
def load_intraday_5m(symbol: str, trade_date: date) -> pd.DataFrame:
    query = text("""
        select
            ts_5m,
            open_5m,
            high_5m,
            low_5m,
            close_5m,
            volume_5m
        from mart.mv_intraday_ohlcv_5m
        where symbol = :symbol
          and trade_date = :trade_date
        order by ts_5m
    """)
    with engine.connect() as conn:
        return pd.read_sql(
            query,
            conn,
            params={"symbol": symbol, "trade_date": trade_date}
        )

# ---------- SIDEBAR ----------

with st.sidebar:
    st.header("Ustawienia")

    symbols = load_symbols()
    symbol = st.selectbox(
        "Symbol",
        symbols,
        index=0 if symbols else None
    )

    trade_date = st.date_input(
        "Dzień sesji",
        value=date.today()
    )

# ---------- BODY ----------

df = load_intraday_5m(symbol, trade_date)

if df.empty:
    st.warning(
        f"Brak danych intraday 5m dla {symbol} w dniu {trade_date}."
    )
    st.stop()

df["ts_5m"] = pd.to_datetime(df["ts_5m"])
df = df.sort_values("ts_5m")

# ---------- OHLC ----------

st.subheader(f"{symbol} – świece 5-min ({trade_date})")

fig = go.Figure(
    data=[
        go.Candlestick(
            x=df["ts_5m"],
            open=df["open_5m"],
            high=df["high_5m"],
            low=df["low_5m"],
            close=df["close_5m"],
        )
    ]
)

fig.update_layout(
    height=500,
    xaxis_title="Czas (UTC)",
    yaxis_title="Cena",
    xaxis_rangeslider_visible=False,
)

st.plotly_chart(fig, use_container_width=True)

# ---------- WOLUMEN ----------

st.subheader("Wolumen 5-min")

fig_vol = go.Figure(
    data=[
        go.Bar(
            x=df["ts_5m"],
            y=df["volume_5m"],
        )
    ]
)

fig_vol.update_layout(
    height=250,
    xaxis_title="Czas (UTC)",
    yaxis_title="Wolumen",
)

st.plotly_chart(fig_vol, use_container_width=True)

# ---------- TABELA ----------

with st.expander("Tabela danych"):
    st.dataframe(df, use_container_width=True, hide_index=True)
