CREATE TABLE IF NOT EXISTS dim_store
(
    store_key           integer PRIMARY KEY,
    store_id            smallint NOT NULL,
    address             varchar(50) NOT NULL,
    address2            varchar(50),
    district            varchar(20) NOT NULL,
    city                varchar(50) NOT NULL,
    country             varchar(50) NOT NULL,
    postal_code         varchar(10),
    manager_first_name  varchar(45) NOT NULL,
    manager_last_name   varchar(45) NOT NULL
);