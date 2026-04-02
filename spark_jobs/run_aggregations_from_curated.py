import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession
from config import POSTGRES_URL, POSTGRES_PROPERTIES


def run_sql_file(spark, sql_file_path, output_table):
    with open(sql_file_path, "r") as f:
        query = f.read()

    result_df = spark.sql(query)

    print(f"Running query from: {sql_file_path}")
    print(f"Writing to table: {output_table}")
    result_df.show(20, truncate=False)

    result_df.write.jdbc(
        url=POSTGRES_URL,
        table=output_table.strip(),
        mode="overwrite",
        properties=POSTGRES_PROPERTIES,
    )

    print(f"✅ Aggregated data written to {output_table}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("TaxiAggregations").getOrCreate()

    df = spark.read.jdbc(
        url=POSTGRES_URL,
        table="curated.taxi_trips_curated",
        properties=POSTGRES_PROPERTIES,
    )

    df.createOrReplaceTempView("taxi_trips")

    aggregation_jobs = {
        "sql/trips_by_day.sql": "analytics.trips_by_day",
        "sql/revenue_by_day.sql": "analytics.revenue_by_day",
        "sql/peak_hours.sql": "analytics.peak_hours",
        "sql/payment_type_summary.sql": "analytics.payment_type_summary",
        "sql/top_pickup_locations.sql": "analytics.top_pickup_locations",
    }

    for sql_file, output_table in aggregation_jobs.items():
        if Path(sql_file).exists():
            run_sql_file(spark, sql_file, output_table)
        else:
            print(f"⚠️ Skipping missing SQL file: {sql_file}")

    spark.stop()