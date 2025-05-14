#Project: Data Warehouse

The README file includes a summary of the project, how to run the Python scripts, and an explanation of the files in the repository. Comments are used effectively and each function has a docstring.

## 1. Introduction:
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights into what songs their users are listening to.

## 2. Project file contents:

- setup_AWS_resources.ipynb         # Infrastructure as code; set up for the required resources on AWS 
- dwh.cfg                           # Configuration file
- sql_queries.py                    # SQL queries for schema, table creation, loading data, analytics examples and shutting down resources
- inspect_datasets.ipynb            # notebook for inspects the datasets
- create_tables.py                  # python file for creating tables in Redshift
- etl.py                            # python file for the etl 
- analytics.py                      # script for analytics

## 3. Datasets

For this project I worked with two datasets stored in S3 (us-west-2 region):

- Song data: s3://udacity-dend/song_data
This dataset contains information about artists and songs, such as shown in this example:


- Log data: s3://udacity-dend/log_data
This dataset has information about the app usage, such as events and streaming information, as shown: 

Additionaly, a third file was provided with metadata required by AWS to correctly load the Log dataset using COPY command:

Log metadata: s3://udacity-dend/log_json_path.json

## 4. Setting Up Workspace
- Create IAM User: save its key and secret (password)
- fill [configuration file](dwh.cfg) with the required information about AWS IAM User and database configuration
- on [sql_queries.py](sql_queries.py) file, build queries to:
 - drop any existing tables; 
 - create tables for staging and schema; 
 - extract data from json files in the 'udacity-dend' bucket into the staging tables;
 - insert data into schema tables;
 - additionaly, build analytical queries to check if the process was successful;
This collection of queries will be used for [create_tables.py](create_tables.py) and [etl.py](etl.py).
- AWS Resources: run the [setup_AWS_resources.ipynb](setup_AWS_resources.ipynb) . I followed the exercise on Infrastructure as Code on module 4 from this course.


## 5. Dataset exploration
Run [inspect_datasets.ipynb](inspect_datasets.ipynb) to inspect the datasets used for the project.

## 6. Database structure
Staging tables: data from json files in the S3 bucket
    - staging_events
    - staging_songs

Fact table:
    - songplays: data related to song plays, in which the information is recorded with page='NextSong'

Dimension tables:
    - users: users in the app
    - songs: information about the songs in the database
    - artists: information about the artists in the database
    - time: more deatiled information on timestamp records of song plays

## ETL Pipeline

1. Run [create_tables.py](create_tables.py) to drop tables if exists and create staging, fact and dimension tables schema. The script is in pyhton, referencing the queries from [sql_queries.py](sql_queries.py)

2. Run [etl.py](etl.py) to load raw data from S3 into Redshift staging tables and transform the data from staging tables into star-schema tables.

3. Run [analytics.py](analytics.py) to validate the etl process. The results are shown below:


4. Run the 'Cleaning up resources' section on [sql_queries.py](sql_queries.py) to shutdown all AWS resources. 