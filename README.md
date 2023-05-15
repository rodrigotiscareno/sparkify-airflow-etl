# Data Pipelines with Airflow: A Udacity Data Engineering Project

## Project Overview
In this project, I applied Apache Airflow to automate the ETL pipeline for Sparkify, a fictional, music streaming company.

The project involved processing data stored in S3 - JSON logs about user activity and song metadata - and loading it into Sparkify's Redshift data warehouse. I created custom operators in Airflow to execute tasks such as staging data in the Redshift data warehouse environment, creating fact and dimension tables from the data, and running data quality checks to finalize the ingestion process.

## Datasets
The project relied on two datasets stored in S3, which included log data and song data:

Log data: s3://udacity-dend/log_data

Song data: s3://udacity-dend/song_data

## Custom Operators


**Stage Operator**: This operator loads JSON formatted files from S3 to Amazon Redshift. It runs a SQL COPY statement based on the parameters provided, which specify the S3 file to load and the target Redshift table.

**Fact and Dimension Operators**: These operators execute data transformations using the SQL helper class provided. They take a SQL statement and the target database for running the query as inputs. The target table for the transformation results is also defined.

**Data Quality Operator**: This operator runs checks on the data. It takes one or more SQL-based test cases along with the expected results and executes the tests. If the test result and expected result do not match, the operator raises an exception, and the task retries and eventually fails.

## Data Transfer
Data for this project was initially in Udacity's S3 bucket. I copied it to my personal S3 bucket for accessibility in the development of the DAG. 

## Execution
Once the DAG was updated and the operators were implemented, the pipeline was executed and monitored via the Airflow UI.
