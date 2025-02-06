# The-Retail-ELT-Pipeline-End-To-End-project
## Overview
&emsp;The goal of this project is to design and implement an ETL data pipeline that ingests raw retail data, processes it, and stores it in a data warehouse for analysis and visualization. The pipeline is orchestrated using Apache Airflow, hosted locally with Docker Compose. AWS S3 serves as the data lake, while AWS Redshift functions as the data warehouse. Finally, the processed data is visualized using Looker Studio for insights and reporting.

## Architecture
<img src="readme_pic/architecture_pic.jpg" alt="Architecture" width="800">
1. Upload raw data to AWS S3 (data lake) to handle data from multiple sources.<br>
2. Download raw data from AWS S3 for processing.<br>
3. Transform the raw data into a suitable format for the data warehouse using Pandas and upload it back to AWS S3.<br>
4. Load the processed data from AWS S3 into AWS Redshift (data warehouse).<br>
5. Use data from the warehouse to create dashboards in Looker Studio for insights and reporting.<br>

## Dashboard
<img src="readme_pic/dashboard_pic.png" alt="Dashboard" width="800">
I use Looker Studio to create dashboards using data from the data warehouse. !!  You can view the dashboard here.  !!<br>

[View Looker Studio Report](https://lookerstudio.google.com/reporting/0ef16b84-55f2-47dc-afd0-f62f6d1a8bd4)

