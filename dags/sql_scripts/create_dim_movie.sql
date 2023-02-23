CREATE TABLE IF NOT EXISTS dim_movie
(
    movie_key           integer PRIMARY KEY,
    film_id             smallint NOT NULL,
    title               varchar(255) NOT NULL,
    description         text NOT NULL,
    release_year        smallint NOT NULL,
    language            varchar(20) NOT NULL,
    rental_duration     smallint NOT NULL,
    rental_rate         numeric(4,2) NOT NULL,
    length              smallint NOT NULL,
    rating              varchar(5) NOT NULL,
    category            varchar(15) NOT null,
    special_features    varchar(60) NOT NULL
);