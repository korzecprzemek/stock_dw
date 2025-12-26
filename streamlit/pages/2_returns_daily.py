import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from datetime import date, timedelta
import plotly.graph_objects as go

# ---------- KONFIG ----------

st.set_page_config(
    page_title="Daily Returns & SMA",
    layout="wide",
)

st.title("📊 Daily returns & SMA (EOD)")

engine = create_engine(
    "postgresql://postgres:postgres@localhost:5432/stock_dw"
)

# ---------- FUNKCJE POMOCNICZE ----------

@st.cache_data(ttl=60)
def load_symbols():
    query = "select distinct symbol from mart.mv_returns_daily order by symbol"
    with engine.connect() as conn:
        return pd.read_sql(query, conn)["symbol"].tolist()


@st.cache_data(ttl=60)
def get_date_range_for_symbol(symbol: str):
    query = text("""
        select 
            min(trade_date) as min_date,
            max(trade_date) as max_date
        from mart.mv_returns_daily
        where symbol = :symbol
    """)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"symbol": symbol})

    min_date = df["min_date"].iloc[0]
    max_date = df["max_date"].iloc[0]
    return min_date, max_date


@st.cache_data(ttl=60)
def load_returns_daily(symbol: str, date_from: date, date_to: date) -> pd.DataFrame:
    query = text("""
        select
            trade_date,
            close,
            adj_close,
            daily_return,
            log_return,
            sma_20,
            sma_50,
            vol_20d_annualized
        from mart.mv_returns_daily
        where symbol = :symbol
          and trade_date between :date_from and :date_to
        order by trade_date
    """)
    with engine.connect() as conn:
        df = pd.read_sql(
            query,
            conn,
            params={
                "symbol": symbol,
                "date_from": date_from,
                "date_to": date_to,
            },
        )
    return df


# ---------- SIDEBAR ----------

with st.sidebar:
    st.header("Ustawienia")

    symbols = load_symbols()
    if symbols:
        symbol = st.selectbox("Symbol", symbols, index=0)
        min_d, max_d = get_date_range_for_symbol(symbol)

        if min_d is not None and max_d is not None:
            default_from = max_d - timedelta(days=180)  # ostatnie 6 miesięcy
            if default_from < min_d:
                default_from = min_d

            date_from = st.date_input(
                "Data od",
                value=default_from,
                min_value=min_d,
                max_value=max_d,
            )
            date_to = st.date_input(
                "Data do",
                value=max_d,
                min_value=min_d,
                max_value=max_d,
            )
        else:
            # fallback jak mart jest pusty
            today = date.today()
            date_from = st.date_input("Data od", value=today - timedelta(days=180))
            date_to = st.date_input("Data do", value=today)
    else:
        st.warning("Brak danych w mart.mv_returns_daily.")
        st.stop()

# ---------- ŁADOWANIE DANYCH ----------

df = load_returns_daily(symbol, date_from, date_to)

if df.empty:
    st.warning(
        f"Brak danych EOD dla {symbol} w zakresie {date_from} – {date_to}."
    )
    st.stop()

df["trade_date"] = pd.to_datetime(df["trade_date"])

# Helper kolumny procentowe do prezentacji
df["daily_return_pct"] = df["daily_return"] * 100
df["vol_20d_pct"] = df["vol_20d_annualized"] * 100

# ---------- WYKRES 1: CENA + SMA ----------

st.subheader(f"{symbol} – cena zamknięcia i SMA ({date_from} – {date_to})")

fig_price = go.Figure()

fig_price.add_trace(
    go.Scatter(
        x=df["trade_date"],
        y=df["close"],
        mode="lines",
        name="Close",
    )
)

if "sma_20" in df.columns:
    fig_price.add_trace(
        go.Scatter(
            x=df["trade_date"],
            y=df["sma_20"],
            mode="lines",
            name="SMA 20",
            line=dict(dash="dash"),
        )
    )

if "sma_50" in df.columns:
    fig_price.add_trace(
        go.Scatter(
            x=df["trade_date"],
            y=df["sma_50"],
            mode="lines",
            name="SMA 50",
            line=dict(dash="dot"),
        )
    )

fig_price.update_layout(
    height=450,
    xaxis_title="Data",
    yaxis_title="Cena",
)

st.plotly_chart(fig_price, use_container_width=True)

# ---------- WYKRES 2: DAILY RETURNS ----------

st.subheader("Dzienne stopy zwrotu")

fig_ret = go.Figure()

fig_ret.add_trace(
    go.Bar(
        x=df["trade_date"],
        y=df["daily_return_pct"],
        name="Daily return [%]",
    )
)

fig_ret.update_layout(
    height=300,
    xaxis_title="Data",
    yaxis_title="Stopa zwrotu [%]",
)

st.plotly_chart(fig_ret, use_container_width=True)

# ---------- WYKRES 3: VOLATILITY 20D ----------

st.subheader("Zannualizowana zmienność 20-dniowa")

fig_vol = go.Figure()

fig_vol.add_trace(
    go.Scatter(
        x=df["trade_date"],
        y=df["vol_20d_pct"],
        mode="lines",
        name="Vol 20D [%]",
    )
)

fig_vol.update_layout(
    height=300,
    xaxis_title="Data",
    yaxis_title="Volatility [%]",
)

st.plotly_chart(fig_vol, use_container_width=True)

# ---------- TABELA ----------

with st.expander("Tabela danych (EOD features)"):
    st.dataframe(
        df[
            [
                "trade_date",
                "close",
                "adj_close",
                "daily_return",
                "log_return",
                "sma_20",
                "sma_50",
                "vol_20d_annualized",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )
