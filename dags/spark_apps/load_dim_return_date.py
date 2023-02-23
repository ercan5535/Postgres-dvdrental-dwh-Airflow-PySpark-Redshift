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
prefix = 'pyspark-dev/dim_return_date/%s.csv' % str(time.time()).split('.')[0]
s3_path = "s3a://{}/{}".format(s3_bucket, prefix)

# Create Spark Session
spark = SparkSession.builder.appName("Postgres2Aws").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", s3_access_key)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", s3_secret_key)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", s3_endpoint)

# Read data from a PostgreSQL table
df_return = spark.read.format("jdbc").options(
    url=postgres_url,
    driver="org.postgresql.Driver",
    dbtable="rental",
    user=postgres_user,
    password=postgres_password
).load()
df_return.createOrReplaceTempView("rental")

# Get dim_return_date table with SQL query
df_result = spark.sql("""
    SELECT
        DISTINCT(date_format(return_date, 'yyMMdd')) as date_key,
        date(return_date)                  AS date,
        EXTRACT(year FROM return_date)     AS year,
        EXTRACT(quarter FROM return_date)  AS quarter,
        EXTRACT(year FROM return_date) || 'Q' || EXTRACT(quarter FROM return_date) AS year_quart,
        EXTRACT(month FROM return_date)    AS month,
        EXTRACT(day FROM return_date)      AS day,
        EXTRACT(week FROM return_date)     AS week,
        CASE WHEN EXTRACT(DAYOFWEEK_ISO FROM return_date) IN (6,7) THEN 'true' ELSE 'false' END as is_weekend
    FROM rental
    WHERE return_date IS NOT NULL;
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
    dbtable="dim_return_date",
    user=redshift_user,
    password=redshift_password,
    driver="com.amazon.redshift.jdbc42.Driver"
).mode("append").save()
