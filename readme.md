# Data Engineering Workflow with Apache Airflow

This repository contains an Airflow DAG designed for automating the ETL (Extract, Transform, Load) process for a data engineering project. The workflow involves uploading raw data, performing transformations, and loading the processed data into AWS Redshift.

## Project Overview

The DAG is composed of multiple stages that handle different aspects of the ETL pipeline:

### **1. Data Upload and Download**
- **`upload_s3`**: Uploads raw data files to an S3 bucket.
- **`download_s3_clean`**: Downloads and cleans the data for further processing.

### **2. Data Transformation**
Transforms datasets into a format optimized for loading into the data warehouse:
- **`location_Transform`**: Processes location data.
- **`product_Transform`**: Processes product data.
- **`customer_Transform`**: Processes customer data.
- **`order_Transform`**: Processes order data.
- **`time_Transform`**: Processes time-related data.
- **`ftc_profit_Transform`**: Calculates and processes profit-related data.

### **3. Data Loading**
Loads the transformed datasets into AWS Redshift:
- **`location_Load_redshift`**
- **`product_Load_redshift`**
- **`customer_Load_redshift`**
- **`order_Load_redshift`**
- **`time_Load_redshift`**
- **`ftc_profit_Load_redshift`**

## DAG Structure

The workflow is designed with the following dependencies:

1. **Initial Stages**
   - The process starts with `upload_s3`, followed by `download_s3_clean`.

2. **Parallel Transformations**
   - The datasets (`location`, `product`, `customer`, `order`, `time`) are transformed in parallel.

3. **Sequential Loading**
   - Each transformed dataset is loaded into AWS Redshift.

4. **Final Stage**
   - The profit dataset (`ftc_profit`) is transformed and loaded after all other datasets are processed.

![Workflow Diagram](path_to_dag_image.png) <!-- Replace with the actual diagram if available -->

## Getting Started

### Prerequisites
- **Apache Airflow** installed and running.
- Access to **AWS S3** and **AWS Redshift**.
- Python modules:
  - `boto3` for interacting with S3.
  - `psycopg2` or `redshift_connector` for Redshift.

### Setting Up
1. Clone this repository:

