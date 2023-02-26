# Description


Its a case study for ETL process with Postgres sample database <a href="https://www.postgresqltutorial.com/postgresql-getting-started/postgresql-sample-database/">DVD-rental</a>. <br>
The goal is transform Normalized database into Star Schema and load to AWS S3 and AWS Redshift <br>

PySpark is used for data operations<br>
Airflow used for all orchestration<br>

<img src="https://user-images.githubusercontent.com/67562422/221020463-83525f85-2ad3-4937-83b5-e47571c8383e.png" width="800" height="300">
<br>

Star Schema Transformation:

<img src="https://user-images.githubusercontent.com/67562422/221009339-b12a6ca8-699d-48f4-8c96-1401defb7377.png" width="800" height="400">


ETL pipeline

 <img src="https://user-images.githubusercontent.com/67562422/221390556-341c2cf5-7e48-4144-b0c0-9954f866c93e.png" width="1000" height="400">
 
 # Development
- Dvdrental database is loaded to Postgres by container initialization
- Everything else is constructed on <a href="https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html#fetching-docker-compose-yaml">Airflow's docker-compose example</a>.
```bash
├── docker-compose.yaml
├── docker_images
│   ├── airflow
│   │   ├── Dockerfile
│   │   └── requirements.txt
│   └── postgresql
│       ├── Dockerfile
│       ├── dvdrental.tar
│       └── init.sh
```
- SQL scripts used for creating tables with PostgresOperator on RedShift and PySpark apps used for reading data from Postgres and load into S3 and RedShift
```bash
├── dags
│   ├── dvdrental-dwh.py
│   ├── spark_apps
│   │   ├── load_dim_customer.py
│   │   ├── load_dim_movie.py
│   │   ├── load_dim_payment_date.py
│   │   ├── load_dim_rental_date.py
│   │   ├── load_dim_return_date.py
│   │   ├── load_dim_staff.py
│   │   ├── load_dim_store.py
│   │   ├── load_fact_sales.py
│   │   ├── postgresql-42.5.1.jar
│   │   └── redshift-jdbc42-2.1.0.11.jar
│   └── sql_scripts
│       ├── create_dim_customer.sql
│       ├── create_dim_movie.sql
│       ├── create_dim_payment_date.sql
│       ├── create_dim_rental_date.sql
│       ├── create_dim_return_date.sql
│       ├── create_dim_staff.sql
│       ├── create_dim_store.sql
│       └── create_fact_sales.sql
```
- Checkables dag created by custome operator 'RedshiftCheckTables' in plugin folder
```bash
├── plugins
│   ├── __init__.py
│   ├── operators
│   │   ├── data_quality.py
│   │   ├── __init__.py
```
