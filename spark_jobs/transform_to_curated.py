import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    hour,
    month,
    dayofweek,
    date_format,
    unix_timestamp,
    round as spark_round,
    when,
)
from config import POSTGRES_URL, POSTGRES_PROPERTIES, MONTHS


def transform_taxi_data(spark):
    # Read from Postgres staging
    df = spark.read.jdbc(
        url=POSTGRES_URL,
        table="staging.taxi_trips_cleaned",
        properties=POSTGRES_PROPERTIES,
    )

    df = (
        df.withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
        .withColumn("dropoff_date", to_date(col("tpep_dropoff_datetime")))
        .withColumn("pickup_hour", hour(col("tpep_pickup_datetime")))
        .withColumn("dropoff_hour", hour(col("tpep_dropoff_datetime")))
        .withColumn("pickup_month", month(col("tpep_pickup_datetime")))
        .withColumn("pickup_day_of_week", dayofweek(col("tpep_pickup_datetime")))
        .withColumn("pickup_day_name", date_format(col("tpep_pickup_datetime"), "EEEE"))
        .withColumn(
            "trip_duration_minutes",
            spark_round(
                (
                    unix_timestamp(col("tpep_dropoff_datetime"))
                    - unix_timestamp(col("tpep_pickup_datetime"))
                ) / 60,
                2,
            ),
        )
        .withColumn(
            "fare_per_mile",
            when(
                col("trip_distance") > 0,
                spark_round(col("fare_amount") / col("trip_distance"), 2),
            ),
        )
    )

    df = df.filter(col("trip_duration_minutes") >= 0)
    df = df.filter(col("trip_distance") >= 0)
    df = df.filter(col("total_amount") >= 0)

    print("Curated row count:", df.count())
    df.printSchema()


    # write to postgres
    df.write.jdbc(
        url="jdbc:postgresql://localhost:5432/taxi_db",
        table="curated.taxi_trips_curated",
        mode="append",
        properties={
            "user": "rajesh",
            "password": "",
            "driver": "org.postgresql.Driver",
        },
    )

    print("✅ Loaded into curated.taxi_trips_curated")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("TaxiTransform").getOrCreate()
    
    transform_taxi_data(spark)
    spark.stop()