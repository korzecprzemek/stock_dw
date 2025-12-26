import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import time


# ----------------------------------------------------
# CONFIG
# ----------------------------------------------------

DB_URL = "postgresql+psycopg2://pkorzec@localhost:5432/stock_dw"
engine = create_engine(DB_URL)

st.set_page_config(
    page_title="Stock Price Dashboard",
    layout="wide",
)

# ----------------------------------------------------
# DATA LOADING
# ----------------------------------------------------


@st.cache_data(ttl=60)
def load_data() -> pd.DataFrame:
    """
    Loads joined price + symbol + date data from the warehouse.
    Cached for 60 seconds.
    """
    query = """
        SELECT 
            date_value AS date,
            symbol,
            open,
            high,
            low,
            close,
            adj_close,
            volume
        FROM mart.vw_price_daily
        ORDER BY date_value ASC
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    df["date"] = pd.to_datetime(df["date"])
    return df
@st.cache_data(ttl=60)
def load_dividends() -> pd.DataFrame:
    query = """
        SELECT
            ex_date,
            symbol,
            dividend
        FROM mart.vw_dividend_daily
        ORDER BY ex_date ASC
    """
    with engine.connect() as conn:
        df = pd.read_sql(query,conn)
    
    df["ex_date"] = pd.to_datetime(df["ex_date"])
    return df
@st.cache_data(ttl=60)
def load_intraday(symbol: str, limit: int = 200) -> pd.DataFrame:
    """
    Czyta ostatnie ticki intraday ze stagingu.
    """
    query = """
        SELECT
            ts_utc AS datetime,
            open,
            high,
            low,
            close,
            volume
        FROM mart.vw_intraday_last_30m
        WHERE symbol = %(symbol)s
        ORDER BY ts_utc
    """
    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"symbol": symbol, "limit": limit})

    if df.empty:
        return df

    df["datetime"] = pd.to_datetime(df["datetime"])
    return df.sort_values("datetime")

# ----------------------------------------------------
# HELPERY
# ----------------------------------------------------


def filter_data(df: pd.DataFrame,
                selected_symbols: list[str],
                start_date: pd.Timestamp,
                end_date: pd.Timestamp) -> pd.DataFrame:
    mask = (
        df["symbol"].isin(selected_symbols)
        & (df["date"] >= start_date)
        & (df["date"] <= end_date)
    )
    return df.loc[mask].copy()


def compute_latest_metrics(df_symbol: pd.DataFrame) -> dict:
    """
    Zwraca kilka prostych metryk dla pojedynczego tickera:
    ostatnia cena, zmiana dzienna, wolumen.
    """
    df_symbol = df_symbol.sort_values("date")
    if len(df_symbol) < 2:
        return {}

    latest = df_symbol.iloc[-1]
    prev = df_symbol.iloc[-2]

    change = latest["close"] - prev["close"]
    pct = (change / prev["close"]) * 100 if prev["close"] != 0 else None

    return {
        "last_close": latest["close"],
        "prev_close": prev["close"],
        "change_abs": change,
        "change_pct": pct,
        "last_volume": latest["volume"],
        "last_date": latest["date"],
    }


# ----------------------------------------------------
# STREAMLIT UI
# ----------------------------------------------------

st.title("📈 Stock Price Dashboard")
st.markdown("**Hurtownia danych:** PostgreSQL + Dagster + Streamlit")

df = load_data()
df_div = load_dividends()
if df.empty:
    st.warning("Brak danych w hurtowni – odpal ETL w Dagsterze.")
    st.stop()

# ---------------- SIDEBAR: FILTRY --------------------

with st.sidebar:
    st.header("⚙️ Filtry")

    all_tickers = sorted(df["symbol"].unique())

    selected_tickers = st.multiselect(
        "Wybierz tickery:",
        options=all_tickers,
        default=all_tickers[:3] if len(all_tickers) > 3 else all_tickers,
    )

    if not selected_tickers:
        st.warning("Wybierz przynajmniej jeden ticker.")
        st.stop()

    global_min_date = df["date"].min()
    global_max_date = df["date"].max()

    start_date, end_date = st.slider(
        "Zakres dat:",
        min_value=global_min_date.to_pydatetime(),
        max_value=global_max_date.to_pydatetime(),
        value=(global_min_date.to_pydatetime(), global_max_date.to_pydatetime()),
    )

filtered_df = filter_data(df, selected_tickers, pd.to_datetime(start_date), pd.to_datetime(end_date))

if filtered_df.empty:
    st.warning("Brak danych dla wybranego zakresu / tickerów.")
    st.stop()

# ---------------- GŁÓWNA CZĘŚĆ: TABS -----------------

tab_overview, tab_single, tab_volume, tab_live = st.tabs(
    ["📊 Porównanie cen", "🔍 Szczegóły tickera", "📦 Wolumen","📡 Intraday (live)"]
)

# === TAB 1: PORÓWNANIE ===
with tab_overview:
    st.subheader("Porównanie cen zamknięcia (Close)")

    fig_prices = px.line(
        filtered_df,
        x="date",
        y="close",
        color="symbol",
        labels={"date": "Data", "close": "Cena zamknięcia", "symbol": "Ticker"},
    )
    st.plotly_chart(fig_prices, use_container_width=True)

    st.markdown("### Surowe dane (filtrowane)")
    st.dataframe(
        filtered_df.sort_values(["symbol", "date"], ascending=[True, False]),
        use_container_width=True,
        height=400,
    )

# === TAB 2: SZCZEGÓŁY POJEDYNCZEGO TICKERA ===
with tab_single:
    st.subheader("Szczegóły pojedynczego tickera")

    selected_single = st.selectbox(
        "Ticker:",
        options=selected_tickers,
        key="single_ticker_select",
    )

    df_single = filtered_df[filtered_df["symbol"] == selected_single].copy()

    df_div_single = df_div[df_div["symbol"] == selected_single].copy()
    df_div_single = df_div_single[
        (df_div_single["ex_date"] >= pd.to_datetime(start_date))
        & (df_div_single["ex_date"] <= pd.to_datetime(end_date))
    ]

    # Metryki
    metrics = compute_latest_metrics(df_single)
    if metrics:
        col1, col2, col3, col4 = st.columns(4)
        col1.metric(
            "Ostatnia cena",
            f"{metrics['last_close']:.2f}",
            f"{metrics['change_abs']:+.2f}",
        )
        col2.metric(
            "Zmiana [%]",
            f"{metrics['change_pct']:+.2f}%" if metrics["change_pct"] is not None else "–",
        )
        col3.metric("Ostatni wolumen", f"{metrics['last_volume']:,}".replace(",", " "))
        col4.metric("Data", metrics["last_date"].strftime("%Y-%m-%d"))

    st.markdown(f"### Historia cen dla {selected_single}")
    fig_single = px.line(
        df_single,
        x="date",
        y="close",
        labels={"date": "Data", "close": "Cena zamknięcia"},
    )
    st.plotly_chart(fig_single, use_container_width=True)

    st.markdown("### Ostatnie notowania")
    st.dataframe(
        df_single.sort_values("date", ascending=False).head(20),
        use_container_width=True,
        height=400,
    )

    if not df_div_single.empty:
        st.markdown("### Dywidendy")
        
        st.dataframe(
            df_div_single.sort_values("ex_date",ascending=False),
            use_container_width=True,
            height=300,
        )
    else:
        st.info("Brak dywidend dla wybranego okresu")

    st.markdown("### Cena + Dywidendy")

    fig_div = px.line(
        df_single,
        x="date",
        y="close",
        labels={"date": "Data", "close": "Cena zamknięcia"},
    )

# Dodajemy punkty dywidend, jeśli są
    if not df_div_single.empty:
        fig_div.add_scatter(
            x=df_div_single["ex_date"],
            y=df_single.set_index("date").loc[df_div_single["ex_date"], "close"],
            mode="markers",
            marker=dict(size=10, color="green"),
            name="Dywidenda",
            text=[f"Dividend: {v}" for v in df_div_single["dividend"]],
            hovertemplate="Data: %{x}<br>Cena: %{y}<br>%{text}<extra></extra>",
        )

    st.plotly_chart(fig_div, use_container_width=True)


# === TAB 3: WOLUMEN ===
with tab_volume:
    st.subheader("Wolumen obrotu")

    agg = (
        filtered_df.groupby(["date", "symbol"], as_index=False)["volume"]
        .sum()
        .sort_values("date")
    )

    fig_vol = px.bar(
        agg,
        x="date",
        y="volume",
        color="symbol",
        labels={"date": "Data", "volume": "Wolumen", "symbol": "Ticker"},
    )
    st.plotly_chart(fig_vol, use_container_width=True)

    st.markdown("### Dane wolumenowe (filtrowane)")
    st.dataframe(
        agg.sort_values(["symbol", "date"], ascending=[True, False]),
        use_container_width=True,
        height=400,
    )

# === TAB 4: INTRADAY LIVE ===
with tab_live:
    st.subheader("📡 Intraday (live)")
    live_symbol = st.selectbox(
        "Ticker (intraday):",
        options=selected_tickers,
        key="live_ticker_select",
    )

    # prosty manualny refresh – kliknięcie przeładowuje cały skrypt
    if st.button("🔄 Odśwież dane intraday"):
        st.rerun()

    df_live = load_intraday(live_symbol, limit=200)

    if df_live.empty:
        st.info("Brak danych intraday – uruchom `python stream_intraday.py` i poczekaj na ticki.")
    else:
        col1, col2 = st.columns(2)

        with col1:
            fig_live_price = px.line(
                df_live,
                x="datetime",
                y="close",
                labels={"datetime": "Czas", "close": "Cena"},
                title=f"{live_symbol} – cena intraday",
            )
            st.plotly_chart(fig_live_price, use_container_width=True)

        with col2:
            fig_live_vol = px.bar(
                df_live,
                x="datetime",
                y="volume",
                labels={"datetime": "Czas", "volume": "Wolumen"},
                title=f"{live_symbol} – wolumen intraday",
            )
            st.plotly_chart(fig_live_vol, use_container_width=True)

        st.markdown("### Ostatnie ticki")
        st.dataframe(
            df_live.sort_values("datetime", ascending=False).head(50),
            use_container_width=True,
            height=400,
        )
