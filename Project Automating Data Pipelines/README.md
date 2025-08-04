# Data Engineering with AWS - Automated Data Pipeline

## Project Overview

This project implements an automated data pipeline using Apache Airflow to process and transform music streaming data. The pipeline extracts data from S3, stages it in Amazon Redshift, and loads it into a dimensional model for analytics.

## Architecture

The project follows a modern data engineering architecture:

- **Data Source**: S3 buckets containing JSON log files and song metadata
- **Data Warehouse**: Amazon Redshift for data storage and processing
- **Orchestration**: Apache Airflow for pipeline scheduling and monitoring
- **Data Model**: Star schema with fact and dimension tables

## Project Structure

```
├── dags/
│   ├── final_project.py          # Main Airflow DAG
│   ├── create_tables.py          # Table creation script
│   └── create_tables.sql         # SQL schema definitions
├── operators/
│   ├── __init__.py
│   ├── stage_redshift.py         # S3 to Redshift staging operator
│   ├── load_fact.py              # Fact table loading operator
│   ├── load_dimension.py         # Dimension table loading operator
│   └── data_quality.py           # Data quality check operator
└── helpers/
    └── sql_queries.py            # SQL query definitions
```

## Data Model

### Fact Table
- **songplays**: Records of song plays with user activity data

### Dimension Tables
- **users**: User information and demographics
- **songs**: Song metadata and details
- **artists**: Artist information and location data
- **time**: Time-based dimensions for analytics

### Staging Tables
- **staging_events**: Raw event data from S3
- **staging_songs**: Raw song metadata from S3

## Pipeline Workflow

1. **Data Staging**: Load raw data from S3 into staging tables
2. **Fact Table Loading**: Transform and load songplay events
3. **Dimension Table Loading**: Load user, song, artist, and time dimensions
4. **Data Quality Checks**: Validate data integrity and completeness

## Setup Instructions

### Prerequisites

- Python 3.8+
- Apache Airflow
- Amazon Redshift cluster
- AWS S3 bucket with data files
- AWS credentials configured

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd data-engineering-aws-pipeline
   ```

2. **Install dependencies**
   ```bash
   pip install apache-airflow
   pip install boto3
   pip install psycopg2-binary
   ```

3. **Configure Airflow**
   ```bash
   export AIRFLOW_HOME=~/airflow
   airflow db init
   airflow users create \
       --username admin \
       --firstname Admin \
       --lastname User \
       --role Admin \
       --email admin@example.com \
       --password admin
   ```

4. **Set up Airflow connections**
   - Add AWS credentials connection
   - Add Redshift connection
   - Configure S3 bucket access

5. **Create tables in Redshift**
   ```bash
   psql -h <redshift-endpoint> -U <username> -d <database> -f dags/create_tables.sql
   ```

### Configuration

Update the following parameters in `dags/final_project.py`:

- `s3_bucket`: Your S3 bucket name
- `redshift_conn_id`: Your Redshift connection ID
- `aws_credentials_id`: Your AWS credentials connection ID

## Usage

1. **Start Airflow webserver**
   ```bash
   airflow webserver --port 8080
   ```

2. **Start Airflow scheduler**
   ```bash
   airflow scheduler
   ```

3. **Access Airflow UI**
   - Open http://localhost:8080
   - Login with admin/admin
   - Navigate to DAGs and enable `final_project`

## Data Sources

The pipeline processes two types of data:

### Song Data
- Location: `s3://bucket-name/song_data/`
- Format: JSON files
- Contains: Song metadata, artist information

### Log Data
- Location: `s3://bucket-name/log_data/`
- Format: JSON files
- Contains: User activity logs, session data

## Custom Operators

### StageToRedshiftOperator
- Copies data from S3 to Redshift staging tables
- Handles JSON parsing and data validation
- Supports custom JSON paths

### LoadFactOperator
- Loads data into fact tables
- Executes complex SQL transformations
- Handles data type conversions

### LoadDimensionOperator
- Loads dimension tables
- Supports append-only and full-refresh modes
- Implements data deduplication

### DataQualityOperator
- Validates data quality and completeness
- Checks for null values and data integrity
- Ensures referential integrity

## Monitoring and Logging

- Airflow UI provides real-time pipeline monitoring
- Task logs available for debugging
- Data quality checks ensure data integrity
- Email notifications for failures (configurable)

## Performance Considerations

- Redshift COPY command for efficient data loading
- Partitioned data processing
- Optimized SQL queries for large datasets
- Configurable retry logic for resilience


---

**Note**: This project was created as part of the Udacity Data Engineering with AWS Nanodegree program. 