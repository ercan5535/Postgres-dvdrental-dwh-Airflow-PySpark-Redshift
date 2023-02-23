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
prefix = 'pyspark-dev/dim_movie/%s.csv' % str(time.time()).split('.')[0]
s3_path = "s3a://{}/{}".format(s3_bucket, prefix)

# Create Spark Session
spark = SparkSession.builder.appName("Postgres2Aws").getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", s3_access_key)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", s3_secret_key)
spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", s3_endpoint)

# Read data from a PostgreSQL table
df_film = spark.read.format("jdbc").options(
    url=postgres_url,
    driver="org.postgresql.Driver",
    dbtable="film",
    user=postgres_user,
    password=postgres_password
).load()
df_film.createOrReplaceTempView("film")

df_language = spark.read.format("jdbc").options(
    url=postgres_url,
    driver="org.postgresql.Driver",
    dbtable="language",
    user=postgres_user,
    password=postgres_password
).load()
df_language.createOrReplaceTempView("language")

df_film_category = spark.read.format("jdbc").options(
    url=postgres_url,
    driver="org.postgresql.Driver",
    dbtable="film_category",
    user=postgres_user,
    password=postgres_password
).load()
df_film_category.createOrReplaceTempView("film_category")

df_category = spark.read.format("jdbc").options(
    url=postgres_url,
    driver="org.postgresql.Driver",
    dbtable="category",
    user=postgres_user,
    password=postgres_password
).load()
df_category.createOrReplaceTempView("category")

# Get dim_movie table with SQL query
df_result = spark.sql("""
    SELECT 
        f.film_id           AS movie_key,
        f.film_id           AS film_id,
        f.title             AS title, 
        f.description       AS description,
        f.release_year      AS release_year,
        l.name              AS language,
        f.rental_duration   AS rental_duration,
        f.rental_rate       AS rental_rate,
        f.length            AS length, 
        f.rating            AS rating,
        c.name              AS category,
        CAST(f.special_features AS STRING) AS special_features
    FROM film f
    JOIN language l ON (f.language_id = l.language_id)
    JOIN film_category ON (f.film_id = film_category.film_id)
    JOIN category c ON (film_category.category_id = c.category_id);
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
    dbtable="dim_movie",
    user=redshift_user,
    password=redshift_password,
    driver="com.amazon.redshift.jdbc42.Driver"
).mode("append").save()