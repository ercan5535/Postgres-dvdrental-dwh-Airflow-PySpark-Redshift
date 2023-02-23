CREATE TABLE IF NOT EXISTS fact_sales
    (
        sales_key           integer PRIMARY KEY,
        payment_date_key    integer REFERENCES dim_payment_date (date_key),
        rental_date_key     integer REFERENCES dim_rental_date (date_key),
        return_date_key     integer REFERENCES dim_return_date (date_key),
        customer_key        integer REFERENCES dim_customer (customer_key),
        staff_key           integer REFERENCES dim_staff (staff_key),
        store_key           integer REFERENCES dim_store (store_key),
        movie_key           integer REFERENCES dim_movie (movie_key),
        sales_amount        numeric
    );