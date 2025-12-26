CREATE TABLE IF NOT EXISTS fact_dividend (
    dividend_id bigserial PRIMARY KEY,
    symbol_id   integer     NOT NULL REFERENCES dim_symbol(symbol_id),
    date_sk     integer     NOT NULL REFERENCES dim_date(date_sk),
    ex_date     date        NOT NULL,
    dividend    numeric     NOT NULL,
    load_ts     timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT uq_fact_dividend UNIQUE (symbol_id, date_sk)
);
