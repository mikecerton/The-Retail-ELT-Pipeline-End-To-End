# The-Retail-ELT-Pipeline-End-To-End-project
## Overview
&emsp;The goal of this project is to design and implement an ETL data pipeline that ingests raw retail data, processes it, and stores it in a data warehouse for analysis and visualization. The pipeline is orchestrated using Apache Airflow, hosted locally with Docker Compose. AWS S3 serves as the data lake, while AWS Redshift functions as the data warehouse. Finally, the processed data is visualized using Looker Studio for insights and reporting. <br>
!! You can view the dashboard [here. ](https://lookerstudio.google.com/reporting/0ef16b84-55f2-47dc-afd0-f62f6d1a8bd4)!! <br>
## Architecture
<img src="readme_pic/architecture_pic.jpg" alt="Architecture" width="800">
1. Upload raw data to AWS S3 (data lake) to handle data from multiple sources.<br>
2. Download raw data from AWS S3 for processing.<br>
3. Transform the raw data into a suitable format for the data warehouse using Pandas and upload it back to AWS S3.<br>
4. Load the processed data from AWS S3 into AWS Redshift (data warehouse).<br>
5. Use data from the warehouse to create dashboards in Looker Studio for insights and reporting.<br>

## Dashboard
<img src="readme_pic/dashboard_pic.png" alt="Dashboard" width="800">
I use Looker Studio to create dashboards using data from the data warehouse.

!! You can view the dashboard [here. ](https://lookerstudio.google.com/reporting/0ef16b84-55f2-47dc-afd0-f62f6d1a8bd4)!! <br>
#### A special note
While developing this project, I connected Looker Studio to AWS Redshift for data. However, due to AWS free tier limits, Redshift cannot run continuously. As a result, the dashboard now uses data from a CSV file exported from Redshift, but it appears the same as when directly connected to Redshift.

### Data Warehouse
<img src="readme_pic/datawarehouse_pic.png" alt="DataWarehouse" width="600">

### DAG
<img src="readme_pic/dag_pic.png" alt="DAG" width="600">

## Tools & Technologies
- Cloud - Amazon Web Services (AWS) <br>
- Containerization - Docker, Docker Compose <br>
- Orchestration - Airflow <br>
- Transformation - pandas <br>
- Data Lake - AWS S3 <br>
- Data Warehouse - AWS Redshift <br>
- Data Visualization - Looker Studio <br>
- Language - Python <br>

## Set up
1. Check that your Docker has more than 4 GB of RAM. (to use airflow)
```bash
docker run --rm "debian:bookworm-slim" bash -c "numfmt --to iec $(echo $(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE))))"
```
2. clone this github repository
```bash
git clone https://github.com/mikecerton/The-Retail-ELT-Pipeline-End-To-End-project.git
cd The-Retail-ELT-Pipeline-End-To-End-project
```
3. put you data into .env file like this
```bash
AIRFLOW_UID=50000

bucket_name = your bucket_name
aws_access_key_id = your aws_access_key_id
aws_secret_access_key = your aws_secret_access_key
region_name = your region_name

redshift_host = your redshift_host
redshift_port = your redshift_port
redshift_db = your redshift_db
redshift_user = your redshift_user
redshift_password = your redshift_password
iam_role = your iam_role
```
4. run (airflow-init)
```bash
docker-compose up airflow-init
```
5. run (start docker-compose)
```bash
docker-compose up
```
6. you can start activate dag at http://localhost:8080

## Disclaimer
- airflow : <br>
&emsp;https://github.com/mikecerton/Apache_Airflow_Tutorial <br>
- AWS : <br>
&emsp;https://docs.aws.amazon.com/s3/ <br>
&emsp;https://docs.aws.amazon.com/redshift/ <br>
&emsp;https://www.youtube.com/watch?v=WAjPQZ8Osmg&list=LL&index=14&t=2947s <br>
&emsp;https://www.youtube.com/watch?v=7r2z3Qn3Qz8&list=LL&index=27&t=1672s
