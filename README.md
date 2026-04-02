# Taxi Big Data Project – Version 2

## Overview
This project builds a production-style data pipeline using PySpark and PostgreSQL with multi-layer architecture (staging, curated, analytics).

## Architecture
Raw Data → Spark Cleaning → PostgreSQL (Staging)  
→ Spark Transformation → PostgreSQL (Curated)  
→ Spark SQL Aggregations → PostgreSQL (Analytics)  
→ Dashboard

To run the spark jobs: 
spark-submit --jars jars/postgresql-42.7.10.jar spark_jobs/file_name.py

To check in Postgres:
psql taxi_db then do the queries

To run Dashboard:
streamlit run app/dashboard.py
