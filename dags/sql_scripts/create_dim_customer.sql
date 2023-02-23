CREATE TABLE IF NOT EXISTS dim_customer
(
    customer_key    integer PRIMARY KEY,
    customer_id     smallint NOT NULL,
    first_name      varchar(45) NOT NULL,
    last_name       varchar(45) NOT NULL,
    email           varchar(50),
    address         varchar(50) NOT NULL,
    address2        varchar(50),
    district        varchar(20) NOT NULL,
    city            varchar(50) NOT NULL,
    country         varchar(50) NOT NULL,
    postal_code     varchar(10),
    phone           varchar(20) NOT NULL,
    active          boolean NOT NULL,
    create_date     timestamp NOT NULL
);