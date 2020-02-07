# Airflow Data Pipelines for Sparkify
This project extracts data from S3, loads it into Redshift tables and transform those into a set of fact and dimension tables. This is done with a dynamic DAG built from reusable tasks, allowing for tasks monitoring and also easy backfills.
