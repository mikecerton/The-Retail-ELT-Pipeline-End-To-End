import os 
import boto3
from dotenv import load_dotenv

def upload_s3():

    load_dotenv("/opt/airflow/my_data/.env")

    bucket_name = os.getenv("bucket_name")
    session = boto3.Session(
        aws_access_key_id = os.getenv("aws_access_key_id"),
        aws_secret_access_key = os.getenv("aws_secret_access_key"),
        region_name = os.getenv("region_name"))

    s3 = session.client('s3')

    with open("/opt/airflow/my_data/online_retail_II.csv", "rb") as f:
        s3.upload_fileobj(f, bucket_name, "my_retail_s3.csv")
    print("!_S3_upload_complete_onlineII.csv!")

def download_s3():

    load_dotenv("/opt/airflow/my_data/.env")

    bucket_name = os.getenv("bucket_name")
    session = boto3.Session(
        aws_access_key_id = os.getenv("aws_access_key_id"),
        aws_secret_access_key = os.getenv("aws_secret_access_key"),
        region_name = os.getenv("region_name"))

    s3 = session.client('s3')


    key = "my_retail_s3.csv"
    download_path = "/opt/airflow/my_data/my_retail_s3.csv"

    s3.download_file(bucket_name, key, download_path)

    print("!_S3_download_complete_my_retail_s3.csv!")