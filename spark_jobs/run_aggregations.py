from pathlib import Path
from pyspark.sql import SparkSession


def run_sql_file(spark, sql_file_path, output_path):
    with open(sql_file_path, "r") as f:
        query = f.read()

    result_df = spark.sql(query)

    print(f"Running query from: {sql_file_path}")
    result_df.show(20, truncate=False)

    result_df.coalesce(1).write.mode("overwrite").option("header", True).csv(output_path)

    print(f"✅ Aggregated data written to {output_path}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("TaxiAggregations").getOrCreate()

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv([
            "processed_data/October",
            "processed_data/November",
            "processed_data/December",
        ])
    )

    df.createOrReplaceTempView("taxi_trips")

    aggregation_jobs = {
        "sql/trips_by_day.sql": "aggregated_data/trips_by_day",
        "sql/revenue_by_day.sql": "aggregated_data/revenue_by_day",
        "sql/peak_hours.sql": "aggregated_data/peak_hours",
        "sql/payment_type_summary.sql": "aggregated_data/payment_type_summary",
        "sql/top_pickup_locations.sql": "aggregated_data/top_pickup_locations",
    }

    for sql_file, output_path in aggregation_jobs.items():
        if Path(sql_file).exists():
            run_sql_file(spark, sql_file, output_path)
        else:
            print(f"⚠️ Skipping missing SQL file: {sql_file}")

    spark.stop()