create schema if not exists mart;

drop materialized view if exists mart.mv_intraday_ohlcv_5m cascade;

create materialized view mart.mv_intraday_ohlcv_5m
as
with bucketed as (
    select
        f.symbol_id,
        s.symbol,
        f.ts_utc,
        date_trunc('hour', f.ts_utc)
            + make_interval(mins => (extract(minute from f.ts_utc)::int / 5) * 5)
            as ts_5m,
        f.open,
        f.high,
        f.low,
        f.close,
        f.volume,
        row_number() over (
            partition by f.symbol_id,
                         date_trunc('hour', f.ts_utc)
                         + make_interval(mins => (extract(minute from f.ts_utc)::int / 5) * 5)
            order by f.ts_utc
        ) as rn_open,
        row_number() over (
            partition by f.symbol_id,
                         date_trunc('hour', f.ts_utc)
                         + make_interval(mins => (extract(minute from f.ts_utc)::int / 5) * 5)
            order by f.ts_utc desc
        ) as rn_close
    from fact_price_intraday_raw f
    join dim_symbol s
        on s.symbol_id = f.symbol_id
)
select
    b.symbol_id,
    b.symbol,
    d.date_sk,
    d.date_value as trade_date,
    b.ts_5m,

    max(case when rn_open = 1 then open end)  as open_5m,
    max(high)                                as high_5m,
    min(low)                                 as low_5m,
    max(case when rn_close = 1 then close end) as close_5m,
    sum(volume)                              as volume_5m
from bucketed b
join dim_date d
    on d.date_value = date(b.ts_5m)
group by
    b.symbol_id,
    b.symbol,
    d.date_sk,
    d.date_value,
    b.ts_5m
with no data;

create unique index ux_mv_intraday_ohlcv_5m
    on mart.mv_intraday_ohlcv_5m (symbol_id, ts_5m);

create index idx_mv_intraday_ohlcv_5m_symbol_time
    on mart.mv_intraday_ohlcv_5m (symbol, ts_5m desc);


REFRESH MATERIALIZED VIEW mart.mv_intraday_ohlcv_5m;
