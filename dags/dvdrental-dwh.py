import airflow

from datetime import timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from operators.data_quality import RedshiftCheckTables
from airflow.models import Variable

# Get credentials from airfow variables
postrgres_credentials = Variable.get("postgres_jdbc_credentials", deserialize_json=True)
redshift_credentials = Variable.get("redshift_jdbc_credentials", deserialize_json=True)
s3_credentials = Variable.get("s3_credentials", deserialize_json=True)

postgres_user, postgres_password, postgres_url = postrgres_credentials.values()
redshift_user, redshift_password, redshift_url = redshift_credentials.values()
s3_access_key, s3_secret_key, s3_endpoint, s3_bucket_name = s3_credentials.values()

# Generate Bash command to run PySpark
def spark_run_command(python_script_name):
    return "spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.1 \
            --jars ${AIRFLOW_HOME}/dags/spark_apps/redshift-jdbc42-2.1.0.11.jar,${AIRFLOW_HOME}/dags/spark_apps/postgresql-42.5.1.jar \
            ${AIRFLOW_HOME}/dags/spark_apps/%s %s %s %s %s %s %s %s %s %s %s" % (
                python_script_name,
                postgres_user, postgres_password, postgres_url,
                redshift_user, redshift_password, redshift_url,
                s3_access_key, s3_secret_key, s3_endpoint, s3_bucket_name
            )

# Define DAG objects
default_args = {
    'owner': 'ercan',
    'retries': 1,
    'retry_delay': timedelta(seconds=15)   
}

with DAG(
    dag_id='dvdrental-dwh',
    default_args=default_args,
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval='@daily'
) as dag:
    # Define start dummy DAG
    start_operator = DummyOperator(task_id='begin-execution')
 
    # Define DAGs to create table on RedShift
    create_dim_payment_date = PostgresOperator(
        task_id="create_dim_payment_date",
        postgres_conn_id="redshift",
        sql="sql_scripts/create_dim_payment_date.sql"
    )

    create_dim_rental_date = PostgresOperator(
        task_id="create_dim_rental_date",
        postgres_conn_id="redshift",
        sql="sql_scripts/create_dim_rental_date.sql"
    )

    create_dim_return_date = PostgresOperator(
        task_id="create_dim_return_date",
        postgres_conn_id="redshift",
        sql="sql_scripts/create_dim_return_date.sql"
    )

    create_dim_customer = PostgresOperator(
        task_id="create_dim_customer",
        postgres_conn_id="redshift",
        sql="sql_scripts/create_dim_customer.sql"
    )

    create_dim_movie = PostgresOperator(
        task_id="create_dim_movie",
        postgres_conn_id="redshift",
        sql="sql_scripts/create_dim_movie.sql"
    )

    create_dim_staff = PostgresOperator(
        task_id="create_dim_staff",
        postgres_conn_id="redshift",
        sql="sql_scripts/create_dim_staff.sql"
    )

    create_dim_store = PostgresOperator(
        task_id="create_dim_store",
        postgres_conn_id="redshift",
        sql="sql_scripts/create_dim_store.sql"
    )

    create_fact_sales = PostgresOperator(
        task_id="create_fact_sales",
        postgres_conn_id="redshift",
        sql="sql_scripts/create_fact_sales.sql"
    )
    
    # Define DAGs to load data on RedShift
    load_dim_payment_date = BashOperator(
        task_id="load_dim_payment_date",
        bash_command=spark_run_command('load_dim_payment_date.py')
    )

    load_dim_rental_date = BashOperator(
        task_id="load_dim_rental_date",
        bash_command=spark_run_command('load_dim_rental_date.py')
    )

    load_dim_return_date = BashOperator(
        task_id="load_dim_return_date",
        bash_command=spark_run_command('load_dim_return_date.py')
    )

    load_dim_customer = BashOperator(
        task_id="load_dim_customer",
        bash_command=spark_run_command('load_dim_customer.py')
    )

    load_dim_movie = BashOperator(
        task_id="load_dim_movie",
        bash_command=spark_run_command('load_dim_movie.py')
    )

    load_dim_staff = BashOperator(
        task_id="load_dim_staff",
        bash_command=spark_run_command('load_dim_staff.py')
    )

    load_dim_store = BashOperator(
        task_id="load_dim_store",
        bash_command=spark_run_command('load_dim_store.py')
    )

    load_fact_sales = BashOperator(
        task_id="load_fact_sales",
        bash_command=spark_run_command('load_fact_sales.py')
    )

    # Define DAG to check Redshift tables
    check_tables = RedshiftCheckTables(
        task_id="check_tables",
        redshift_conn_id="redshift",
        tables=[
            "dim_payment_date", "dim_rental_date", "dim_return_date",
            "dim_customer", "dim_movie", "dim_staff", "dim_store", "fact_sales"
        ]
    )
 
    # Define data pipeline DAG structure
    # Create dim tables first
    start_operator >> [create_dim_payment_date, create_dim_rental_date, create_dim_return_date,
                    create_dim_customer, create_dim_movie, create_dim_staff, create_dim_store]

    # Create fact table
    create_dim_payment_date >> create_fact_sales
    create_dim_rental_date >> create_fact_sales
    create_dim_return_date >> create_fact_sales
    create_dim_customer >> create_fact_sales
    create_dim_movie >> create_fact_sales
    create_dim_staff >> create_fact_sales
    create_dim_store >> create_fact_sales

    # Load dim and fact tables in sequence
    create_fact_sales >> load_dim_payment_date
    load_dim_payment_date >> load_dim_rental_date 
    load_dim_rental_date >> load_dim_return_date
    load_dim_return_date >> load_dim_customer
    load_dim_customer >> load_dim_movie
    load_dim_movie >> load_dim_staff
    load_dim_staff >> load_dim_store
    load_dim_store >> load_fact_sales

    # Ensure tables are exist and has some data
    load_fact_sales >> check_tables




