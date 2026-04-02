from pyspark.sql import SparkSession

POSTGRES_URL = "jdbc:postgresql://localhost:5432/taxi_db"
POSTGRES_PROPERTIES = {
    "user": "rajesh",
    "password": "",   # empty
    "driver": "org.postgresql.Driver",
}

TABLES = {
    "aggregated_data/trips_by_day": "analytics.trips_by_day",
    "aggregated_data/revenue_by_day": "analytics.revenue_by_day",
    "aggregated_data/peak_hours": "analytics.peak_hours",
    "aggregated_data/payment_type_summary": "analytics.payment_type_summary",
    "aggregated_data/top_pickup_locations": "analytics.top_pickup_locations",
}


def load_table(spark, input_path, table_name):
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    print(f"Loading {input_path} -> {table_name}")
    df.printSchema()

    df.write.jdbc(
        url=POSTGRES_URL,
        table=table_name,
        mode="overwrite",
        properties=POSTGRES_PROPERTIES,
    )

    print(f"✅ Loaded into {table_name}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("LoadTaxiAggregatesToPostgres").getOrCreate()

    for input_path, table_name in TABLES.items():
        load_table(spark, input_path, table_name)

    spark.stop()