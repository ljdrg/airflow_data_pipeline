# Nanodegree Data Engineering 
## Project: Data Pipelines with Airflow

This project creates a data pipeline with Airflow for the music streaming App Sparkify. First, the data is extracted from S3 buckets and inserted into staging tables on Redshift. Then the data from the staging tables are inserted into the tables. Finally, the pipeline runs data quality checks to ensure the pipeline ran correctly.