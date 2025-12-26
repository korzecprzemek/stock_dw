drop materialized view if exists mart.mw_returns_daily cascade;

create materialized view mart.mv_returns_daily
as
with price_base as (
    select
        fp.symbol_id,
        s.symbol,
        fp.date_sk,
        d.date_value as trade_date,
        fp.close,
        fp.adj_close,
        lag(fp.close) over (
            partition by fp.symbol_id
            order by d.date_value
        ) as prev_close
    from fact_price fp
    join dim_date d
        on d.date_sk = fp.date_sk
    join dim_symbol s
        on s.symbol_id = fp.symbol_id
),
returns as (
    select
        *,
        case
            when prev_close is null or prev_close = 0 then null
            else (close / prev_close) - 1
        end as daily_return,
        case
            when prev_close is null or prev_close = 0 then null
            else ln(close / prev_close)
        end as log_return
    from price_base
)
select
    symbol_id,
    symbol,
    date_sk,
    trade_date,
    close,
    adj_close,
    daily_return,
    log_return,

    avg(close) over (
        partition by symbol_id
        order by trade_date
        rows between 19 preceding and current row
    ) as sma_20,

    avg(close) over (
        partition by symbol_id
        order by trade_date
        rows between 49 preceding and current row
    ) as sma_50,

    stddev_samp(daily_return) over (
        partition by symbol_id
        order by trade_date
        rows between 19 preceding and current row
    ) * sqrt(252) as vol_20d_annualized
from returns
with no data;


create unique index ux_mv_returns_daily
    on mart.mv_returns_daily (symbol_id, date_sk);

create index idx_mv_returns_daily_symbol_date
    on mart.mv_returns_daily (symbol, trade_date desc);



REFRESH MATERIALIZED VIEW mart.mv_returns_daily;

