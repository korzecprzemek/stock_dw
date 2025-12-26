# etl/jobs/intraday_job.py

from dagster import op, job
from etl.resources import db_resource


# ---------- KROK 1: STAGING -> FACT INTRADAY ----------

@op(required_resource_keys={"db"})
def load_intraday_from_staging(context):
    """
    Micro-batch: stg_price_intraday -> fact_price_intraday_raw
    Deduplikuje dane po (symbol_id, ts_utc), żeby ON CONFLICT nie wybuchał.
    """
    sql = """
        with symbol_map as (
            select symbol_id, symbol
            from dim_symbol
        ),
        -- zagregowany staging: JEDEN wiersz na (symbol_id, ts_utc)
        src as (
            select
                sm.symbol_id,
                s.ts_utc,
                -- tu możesz dobrać logikę agregacji, ale na razie bierzemy ostatnią cenę i max/min
                max(s.open)  as open,
                max(s.high)  as high,
                min(s.low)   as low,
                max(s.close) as close,
                sum(s.volume) as volume
            from stg_price_intraday s
            join symbol_map sm
              on sm.symbol = s.symbol
            group by sm.symbol_id, s.ts_utc
        ),
        ins as (
            insert into fact_price_intraday_raw (
                symbol_id,
                ts_utc,
                open,
                high,
                low,
                close,
                volume
            )
            select
                symbol_id,
                ts_utc,
                open,
                high,
                low,
                close,
                volume
            from src
            on conflict (symbol_id, ts_utc) do update set
                open   = excluded.open,
                high   = excluded.high,
                low    = excluded.low,
                close  = excluded.close,
                volume = excluded.volume
            returning 1
        )
        delete from stg_price_intraday;
    """

    with context.resources.db.get_connection() as conn:
        with conn.cursor() as cur:
            context.log.info("Loading intraday from staging into fact ...")
            cur.execute(sql)
        conn.commit()

    context.log.info("Intraday micro-batch finished.")
    return 1



# ---------- KROK 2: REFRESH MART INTRADAY ----------

@op(required_resource_keys={"db"})
def refresh_mv_intraday_ohlcv_5m(context, _deps):
    with context.resources.db.get_connection() as conn:
        with conn.cursor() as cur:
            context.log.info("Refreshing mart.mv_intraday_ohlcv_5m ...")
            cur.execute(
                "REFRESH MATERIALIZED VIEW CONCURRENTLY mart.mv_intraday_ohlcv_5m;"
            )
        conn.commit()
    context.log.info("Done mart.mv_intraday_ohlcv_5m")


# ---------- JOB: intraday micro-batch ----------

@job(resource_defs={"db": db_resource})
def intraday_job():
    step = load_intraday_from_staging()
    refresh_mv_intraday_ohlcv_5m(step)
