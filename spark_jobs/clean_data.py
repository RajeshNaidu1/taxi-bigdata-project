import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col
from config import POSTGRES_URL, POSTGRES_PROPERTIES, MONTHS


def clean_taxi_data(input_path, output_csv_path, table_name, month_name, spark):
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    df = df.filter(col("VendorID").cast("string") != "VendorID")

    df = df.withColumn(
        "tpep_pickup_datetime",
        to_timestamp("tpep_pickup_datetime", "EEE MMM dd HH:mm:ss 'UTC' yyyy")
    ).withColumn(
        "tpep_dropoff_datetime",
        to_timestamp("tpep_dropoff_datetime", "EEE MMM dd HH:mm:ss 'UTC' yyyy")
    )

    print(f"{input_path} row count:", df.count())

    df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_csv_path)

    # writing to postgres
    df.write.jdbc(
        url=POSTGRES_URL,
        table=table_name,
        mode="append",  # writing all the months into a single table, so appending
        properties=POSTGRES_PROPERTIES,
    )

    print(f"✅ Loaded cleaned data for {month_name} into {table_name}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("TaxiCleaning").getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    months = ["October", "November", "December"]
    base_table = "staging.taxi_trips_cleaned"

    for month_name in months:
        input_path = f"data/{month_name}"
        output_csv_path = f"clean_data/{month_name}"
        clean_taxi_data(input_path, output_csv_path, base_table, month_name, spark)

    spark.stop()