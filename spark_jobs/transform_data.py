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


def transform_taxi_data(input_path, output_path, spark):
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
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

    print(f"{input_path} row count:", df.count())
    df.printSchema()

    df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

    print(f"✅ Transformed data written to {output_path}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("TaxiTransform").getOrCreate()

    months = ["October", "November", "December"]

    for month_name in months:
        input_path = f"clean_data/{month_name}"
        output_path = f"processed_data/{month_name}"
        transform_taxi_data(input_path, output_path, spark)

    spark.stop()