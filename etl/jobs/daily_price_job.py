# etl/jobs/daily_price_job.py

from dagster import op, job

from etl.staging.extract_prices import extract_prices
from etl.staging.load_stg_price import load_stg_price
from etl.dims.load_dim_symbol import load_dim_symbol
from etl.dims.load_dim_date import load_dim_date
from etl.facts.load_fact_price import load_fact_price
from etl.staging.extract_dividends import extract_dividends
from etl.staging.load_stg_dividend import load_stg_dividend
from etl.facts.load_fact_dividend import load_fact_dividend
from etl.utils.db import engine

from etl.jobs.intraday_job import refresh_mv_intraday_ohlcv_5m
from etl.resources import db_resource



# ---------- KROK 1: pełny EOD ETL ----------

@op
def run_eod_etl(context):
    tickers = ["AAPL", "MSFT", "TSLA", "GOOGL"]
    start = "2024-01-01"
    end = "2025-12-31"

    context.log.info(f"Extracting prices for {tickers} ...")
    df_prices = extract_prices(tickers, start, end)

    context.log.info("Loading price staging ...")
    price_batch_id = load_stg_price(df_prices, engine)

    context.log.info("Extracting dividends ...")
    df_divs = extract_dividends(tickers, start, end)

    context.log.info("Loading dividend staging ...")
    div_batch_id = load_stg_dividend(df_divs, engine)

    context.log.info("Loading dimensions ...")
    load_dim_symbol(engine)
    load_dim_date(engine)

    context.log.info("Loading fact_price ...")
    load_fact_price(engine, price_batch_id)

    context.log.info("Loading fact_dividend ...")
    load_fact_dividend(engine, div_batch_id)

    context.log.info("EOD ETL completed.")
    return 1  # dummy output tylko po to, żeby mieć zależność


# ---------- KROK 2: REFRESH MARTÓW ----------

@op(required_resource_keys={"db"})
def refresh_mv_returns_daily(context, _deps):
    with context.resources.db.get_connection() as conn:
        with conn.cursor() as cur:
            context.log.info("Refreshing mart.mv_returns_daily ...")
            cur.execute(
                "REFRESH MATERIALIZED VIEW CONCURRENTLY mart.mv_returns_daily;"
            )
        conn.commit()
    context.log.info("Done mart.mv_returns_daily")
    return 1


# @op(required_resource_keys={"db"})
# def refresh_mv_intraday_ohlcv_5m(context, _deps):
#     with context.resources.db.get_connection() as conn:
#         with conn.cursor() as cur:
#             context.log.info("Refreshing mart.mv_intraday_ohlcv_5m ...")
#             cur.execute(
#                 "REFRESH MATERIALIZED VIEW CONCURRENTLY mart.mv_intraday_ohlcv_5m;"
#             )
#         conn.commit()
#     context.log.info("Done mart.mv_intraday_ohlcv_5m")


# ---------- JOB: dzienny batch ----------

@job(resource_defs={"db": db_resource})
def daily_price_job():
    etl_step = run_eod_etl()
    r1 = refresh_mv_returns_daily(etl_step)
    refresh_mv_intraday_ohlcv_5m(r1)
