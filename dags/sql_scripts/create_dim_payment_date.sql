CREATE TABLE IF NOT EXISTS dim_payment_date
(
    date_key    integer PRIMARY KEY,
    date        date NOT NULL,
    year        smallint NOT NULL,
    quarter     smallint NOT NULL,
    year_quart  text NOT NULL,
    month       smallint NOT NULL,
    day         smallint NOT NULL,
    week        smallint NOT NULL,
    is_weekend  boolean
);