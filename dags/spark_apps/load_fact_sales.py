import time
import sys

from pyspark.sql import SparkSession

# Get credentials from command line
postgres_user = sys.argv[1]
postgres_password = sys.argv[2]
postgres_url = sys.argv[3]

redshift_user = sys.argv[4]
redshift_password = sys.argv[5]
redshift_url = sys.argv[6]

s3_access_key = sys.argv[7]
s3_secret_key = sys.argv[8]
s3_endpoint = sys.argv[9]
s3_bucket = sys.argv[10]

# Define s3_path
prefix = 'pyspark-dev/fact_sales/%s.csv' % str(time.time()).split('.')[0]
s3_path = "s3a://{}/{}".format(s3_bucket, prefix)

# Create Spark Session
spark = SparkSession.builder.appName("Postgres2Aws").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", s3_access_key)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", s3_secret_key)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", s3_endpoint)

# Read data from a PostgreSQL table
df_payment = spark.read.format("jdbc").options(
    url=postgres_url,
    driver="org.postgresql.Driver",
    dbtable="payment",
    user=postgres_user,
    password=postgres_password
).load()
df_payment.createOrReplaceTempView("payment")

df_rental = spark.read.format("jdbc").options(
    url=postgres_url,
    driver="org.postgresql.Driver",
    dbtable="rental",
    user=postgres_user,
    password=postgres_password
).load()
df_rental.createOrReplaceTempView("rental")

df_inventory = spark.read.format("jdbc").options(
    url=postgres_url,
    driver="org.postgresql.Driver",
    dbtable="inventory",
    user=postgres_user,
    password=postgres_password
).load()
df_inventory.createOrReplaceTempView("inventory")

# Get dim_staff table with SQL query
df_result = spark.sql("""
    SELECT
        p.payment_id    AS sales_key,
        date_format(p.payment_date, 'yyMMdd') AS payment_date_key,
        date_format(r.rental_date, 'yyMMdd')  AS rental_date_key,
        INT(date_format(r.return_date, 'yyMMdd')) AS return_date_key,
        p.customer_id  	AS customer_key,
        p.staff_id 		AS staff_key,
        i.store_id 		AS store_key,        
        i.film_id 		AS movie_key,
        p.amount 		AS sales_amount
    FROM payment p 
    JOIN rental r ON (p.rental_id = r.rental_id)
    JOIN inventory i ON (r.inventory_id = i.inventory_id)
    WHERE r.return_date IS NULL;
""")

df_result.printSchema()
df_result.show(10)

# Load data to S3 bucket
df_result.write\
    .format("csv")\
    .option("header", "true")\
    .save(s3_path)

# Load data to RedShift
df_result.write.format("jdbc").options(
    url=redshift_url,
    dbtable="fact_sales",
    user=redshift_user,
    password=redshift_password,
    driver="com.amazon.redshift.jdbc42.Driver"
).mode("append").save()