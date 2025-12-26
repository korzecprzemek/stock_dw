CREATE TABLE fact_price_intraday_raw (
    intraday_id  bigserial PRIMARY KEY,
    symbol_id    int NOT NULL REFERENCES dim_symbol(symbol_id),
    ts_utc       timestamptz NOT NULL,

    open         numeric(18,6),
    high         numeric(18,6),
    low          numeric(18,6),
    close        numeric(18,6),
    volume       bigint,

    CONSTRAINT uq_fact_price_intraday_raw UNIQUE (symbol_id, ts_utc)
);
