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
prefix = 'pyspark-dev/dim_customer/%s.csv' % str(time.time()).split('.')[0]
s3_path = "s3a://{}/{}".format(s3_bucket, prefix)

# Create Spark Session
spark = SparkSession.builder.appName("Postgres2Aws").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", s3_access_key)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", s3_secret_key)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", s3_endpoint)

# Read data from a PostgreSQL table
df_customer = spark.read.format("jdbc").options(
    url=postgres_url,
    driver="org.postgresql.Driver",
    dbtable="customer",
    user=postgres_user,
    password=postgres_password
).load()
df_customer.createOrReplaceTempView("customer")

df_address = spark.read.format("jdbc").options(
    url=postgres_url,
    driver="org.postgresql.Driver",
    dbtable="address",
    user=postgres_user,
    password=postgres_password
).load()
df_address.createOrReplaceTempView("address")

df_city = spark.read.format("jdbc").options(
    url=postgres_url,
    driver="org.postgresql.Driver",
    dbtable="city",
    user=postgres_user,
    password=postgres_password
).load()
df_city.createOrReplaceTempView("city")

df_country = spark.read.format("jdbc").options(
    url=postgres_url,
    driver="org.postgresql.Driver",
    dbtable="country",
    user=postgres_user,
    password=postgres_password
).load()
df_country.createOrReplaceTempView("country")

# Get dim_customers table with SQL query
df_result = spark.sql("""
    SELECT
        c.customer_id   AS customer_key,
        c.customer_id   AS customer_id,
        c.first_name    AS first_name,
        c.last_name     AS last_name,
        c.email         AS email,
        a.address       AS address,
        a.address2      AS address2,
        a.district      AS district,
        ci.city         AS city,
        co.country      AS country,
        postal_code     AS postal_code,
        a.phone         AS phone,
        CASE WHEN c.active = 1 THEN true ELSE false END AS active,
        c.create_date   AS create_date
    FROM customer c
    JOIN address a ON (c.address_id = a.address_id)
    JOIN city ci ON (a.city_id = ci.city_id)
    JOIN country co ON (ci.country_id = co.country_id);
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
    dbtable="dim_customer",
    user=redshift_user,
    password=redshift_password,
    driver="com.amazon.redshift.jdbc42.Driver"
).mode("append").save()
