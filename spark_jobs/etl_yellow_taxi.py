from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, hour, unix_timestamp
from pyspark.sql.types import IntegerType

spark = (
    SparkSession.builder
    .appName("NYC Yellow Taxi ETL")
    .config("spark.jars", "jars/postgresql.jar")
    .getOrCreate()
)

# Ignore corrupt files (temporary/workaround). See diagnostics for details.
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
# Start with 2-3 monthly files only.
# Replace these with the files you download from the official TLC page.
input_paths = [
    "data/October/yellow_tripdata_2025-10.parquet",
    "data/November/yellow_tripdata_2025-11.parquet",
    "data/December/yellow_tripdata_2025-12.parquet"
]

df = spark.read.parquet(*input_paths)

# Clean + transform
cleaned_df = (
    df
    .filter(col("tpep_pickup_datetime").isNotNull())
    .filter(col("tpep_dropoff_datetime").isNotNull())
    .filter(col("tpep_dropoff_datetime") >= col("tpep_pickup_datetime"))
    .filter(col("trip_distance") >= 0)
    .filter(col("total_amount") >= 0)
    .withColumn("trip_date", to_date(col("tpep_pickup_datetime")))
    .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
    .withColumn(
        "trip_duration_minutes",
        (unix_timestamp(col("tpep_dropoff_datetime")) - unix_timestamp(col("tpep_pickup_datetime"))) / 60.0
    )
    .select(
        col("VendorID").cast("string").alias("vendor_id"),
        col("tpep_pickup_datetime").alias("pickup_datetime"),
        col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        col("trip_date"),
        col("pickup_hour").cast(IntegerType()),
        col("passenger_count"),
        col("trip_distance"),
        col("RatecodeID").alias("rate_code_id"),
        col("PULocationID").cast(IntegerType()).alias("pu_location_id"),
        col("DOLocationID").cast(IntegerType()).alias("do_location_id"),
        col("payment_type"),
        col("fare_amount"),
        col("extra"),
        col("mta_tax"),
        col("tip_amount"),
        col("tolls_amount"),
        col("improvement_surcharge"),
        col("total_amount"),
        col("congestion_surcharge"),
        col("Airport_fee").alias("airport_fee"),
        col("trip_duration_minutes")
    )
)

cleaned_df.write \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/taxi_db") \
    .option("dbtable", "fact_taxi_trips") \
    .option("user", "postgres") \
    .option("password", "your_password") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

print("ETL completed and data loaded to PostgreSQL.")

spark.stop()