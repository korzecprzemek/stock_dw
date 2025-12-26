CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.stg_dividend (
    load_ts   timestamptz NOT NULL DEFAULT now(),
    symbol    text        NOT NULL,
    ex_date   date        NOT NULL,
    dividend  numeric     NOT NULL,
    source    text        NOT NULL,
    batch_id  uuid        NOT NULL
);
