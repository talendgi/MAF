# MySQL to Snowflake ETL Workflow

This project demonstrates an ETL pipeline built using **Microsoft Agent Framework Workflows**. It extracts data from a MySQL table, transforms the records, checks whether the target Snowflake table exists, creates it if needed, and then loads the data into Snowflake.

## Overview

The workflow is designed to automate a simple but practical data pipeline:

1. Extract data from MySQL.
2. Transform the records by adding a `processed_at` timestamp.
3. Check if the Snowflake destination table exists.
4. Create the table if it does not exist.
5. Load the data into Snowflake.

## Tech Stack

- **Python**
- **Microsoft Agent Framework**
- **MySQL Connector/Python**
- **Snowflake Connector for Python**
- **dotenv** for environment variable management
- **logging** for execution tracing

## Workflow Architecture

The workflow uses a graph of executors connected by edges:

- **ExtractExecutor**: Reads data from MySQL.
- **TransformExecutor**: Adds transformation logic, such as `processed_at`.
- **CheckTableExistsExecutor**: Checks whether the Snowflake table exists.
- **CreateTableExecutor**: Creates the Snowflake table if missing.
- **LoadExecutor**: Inserts transformed rows into Snowflake.

## Process Flow

```text
MySQL Table
   ↓
ExtractExecutor
   ↓
TransformExecutor
   ↓
CheckTableExistsExecutor
   ├── if table exists ─→ LoadExecutor
   └── if table missing ─→ CreateTableExecutor ─→ LoadExecutor
```

## Key Features

- Uses a modular executor-based design.
- Supports conditional routing in the workflow.
- Automatically creates the Snowflake table when required.
- Adds a `processed_at` field during transformation.
- Uses environment variables for database credentials.
- Logs each major step for debugging and monitoring.

## Environment Variables

Set the following variables before running the workflow:

### MySQL
- `MYSQL_HOST`
- `MYSQL_PORT`
- `MYSQL_USER`
- `MYSQL_PASSWORD`
- `MYSQL_DATABASE`

### Snowflake
- `SF_ACCOUNT`
- `SF_USER`
- `SF_PASSWORD`
- `SF_DATABASE`
- `SF_SCHEMA`
- `SF_WAREHOUSE`
- `SF_ROLE`

## How It Works

### 1. Extract
The workflow connects to MySQL and reads all rows from the source table.

### 2. Transform
Each record is updated with a `processed_at` timestamp.

### 3. Check Table
The workflow checks whether the Snowflake destination table already exists.

### 4. Create Table
If the table does not exist, it is created using the source schema plus the `processed_at` column.

### 5. Load
The transformed records are inserted into Snowflake.


## Purpose

This workflow is a sample ETL pipeline that shows how Microsoft Agent Framework can be used to orchestrate real data movement between MySQL and Snowflake.
