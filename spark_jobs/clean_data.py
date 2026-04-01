from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col


def clean_taxi_data(input_path, output_path, spark):
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

    df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

    print(f"✅ Cleaning completed successfully for {input_path}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("TaxiCleaning").getOrCreate()
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    months = ["October", "November", "December"]

    for month in months:
        input_path = f"data/{month}"
        output_path = f"clean_data/{month}"
        clean_taxi_data(input_path, output_path, spark)

    spark.stop()