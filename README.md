# Data Pipelines with Airflow – Final Project

## Overview

This project implements an end-to-end data pipeline using Apache Airflow, Amazon S3, and Amazon Redshift. The pipeline loads JSON data from Amazon S3 into Redshift staging tables, transforms the data into a star schema, and performs data quality checks to validate the final dataset.

The project demonstrates the use of custom Airflow operators, task dependencies, parameterized SQL execution, and automated data quality validation.

## Architecture

### Source Data

The pipeline ingests two datasets stored in Amazon S3:

* Event Log Data (`log-data`)
* Song Metadata (`song-data`)

### Target Database

Amazon Redshift serves as the data warehouse and contains:

#### Staging Tables

* staging_events
* staging_songs

#### Fact Table

* songplays

#### Dimension Tables

* users
* songs
* artists
* time

## DAG Workflow

The DAG follows the sequence below:

```text
Begin_execution
        |
-------------------------
|                       |
Stage_events       Stage_songs
        |               |
        -----------------
                |
Load_songplays_fact_table
                |
---------------------------------------------
|            |            |                 |
users       songs      artists            time
---------------------------------------------
                |
Run_data_quality_checks
                |
Stop_execution
```

## Project Components

### DAG

**final_project.py**

Defines:

* Scheduling configuration
* Retry policies
* Task dependencies
* Custom operator execution

### Custom Operators

#### StageToRedshiftOperator

Loads JSON files from Amazon S3 into Redshift staging tables using dynamically generated COPY commands.

Features:

* Parameterized S3 paths
* Configurable destination tables
* Redshift connection management
* Airflow templating support

#### LoadFactOperator

Loads data into the songplays fact table.

Features:

* Parameterized SQL execution
* Append-only loading strategy

#### LoadDimensionOperator

Loads dimension tables from staging data.

Features:

* Supports truncate-and-load
* Supports append-only mode
* Reusable across dimensions

#### DataQualityOperator

Executes configurable validation rules against Redshift tables.

Features:

* Parameterized quality checks
* Failure handling
* Automated validation before pipeline completion

## Data Quality Checks

The following validations are performed:

* songplays contains no NULL songplay IDs
* users contains no NULL user IDs
* songs contains no NULL song IDs
* artists contains no NULL artist IDs
* time contains no NULL start_time values

The pipeline fails if any validation does not meet the expected result.

## Technologies Used

* Apache Airflow
* Amazon S3
* Amazon Redshift
* PostgreSQL Hook
* Python
* SQL

## Project Structure

```text
.
├── dags
    ├── final_project.py
├── final_project_sql_statements.py
├── create_tables.sql
└── final_project_operators
    ├── stage_redshift.py
    ├── load_fact.py
    ├── load_dimension.py
    └── data_quality.py
```

## Running the Pipeline

1. Create Redshift tables using `create_tables.sql`.
2. Configure Airflow connections:

   * aws_credentials
   * redshift
3. Start Airflow services.
4. Trigger the `final_project` DAG.
5. Monitor execution through the Airflow UI.
6. Review task logs and data quality results.

## Outcome

The pipeline successfully stages raw JSON data from S3, transforms it into a star schema within Redshift, and validates the resulting warehouse tables through automated quality checks.
