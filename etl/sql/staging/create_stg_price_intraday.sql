CREATE TABLE stg_price_intraday (
    symbol       text NOT NULL,
    ts_utc       timestamptz NOT NULL,
    open         numeric(18,6),
    high         numeric(18,6),
    low          numeric(18,6),
    close        numeric(18,6),
    volume       bigint
);
