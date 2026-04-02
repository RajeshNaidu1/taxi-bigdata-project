#  Taxi Big Data Project – Version 1

## Overview
This project builds a simple end-to-end data pipeline using PySpark to process NYC taxi trip data.

## Architecture
Raw CSV → Spark Cleaning → Spark Transformation → Aggregations → PostgreSQL → Dashboard

To run the spark jobs: 
spark-submit spark_jobs/file_name.py

To check in Postgres:
psql taxi_db then do the queries

To run Dashboard:
streamlit run app/dashboard.py
