import os 
import boto3
from dotenv import load_dotenv
import pandas as pd
import boto3
import psycopg2


def product_Transform():

    load_dotenv("/opt/airflow/my_data/.env")

    data_Fname = "product_data.csv"         # change
    bucket_name = os.getenv("bucket_name")

    session = boto3.Session(
        aws_access_key_id = os.getenv("aws_access_key_id"),
        aws_secret_access_key = os.getenv("aws_secret_access_key"),
        region_name = os.getenv("region_name"))

    s3 = session.client('s3')

    df = pd.read_csv("/opt/airflow/my_data/my_retail_s3.csv")          # change
    unique_stockcodes = df[['StockCode', 'Description']].drop_duplicates(subset=['StockCode'])
    unique_stockcodes.to_csv(data_Fname, index=False)

    with open(data_Fname, "rb") as f:
        s3.upload_fileobj(f, bucket_name, data_Fname)
    print("!_upload_to_S3_complete_!")

def prodct_Load():

    load_dotenv("/opt/airflow/my_data/.env")

    data_Fname = "product_data.csv"         # change
    bucket_name = os.getenv("bucket_name")

    conn = psycopg2.connect(
        host = os.getenv("redshift_host"),
        port = os.getenv("redshift_port"),
        dbname = os.getenv("redshift_db"),
        user = os.getenv("redshift_user"),
        password = os.getenv("redshift_password")
    )

    table_name = "Product_dim"          # change
    s3_path = f"s3://{bucket_name}/{data_Fname}"
    iam_role = os.getenv("iam_role")

    copy_query = f"""
    COPY {table_name}
    FROM '{s3_path}'
    IAM_ROLE '{iam_role}'
    CSV
    IGNOREHEADER 1;
    """

    cursor = conn.cursor()
    try:
        cursor.execute(copy_query)
        conn.commit()
        print("Data loaded successfully into table!")
    except Exception as e:
        conn.rollback()
        print(f"Error loading data into table: {e}")
    finally:
        cursor.close()
        conn.close()

def customer_Transform():

    load_dotenv("/opt/airflow/my_data/.env")

    data_Fname = "customer_data.csv"         # change
    bucket_name = os.getenv("bucket_name")

    session = boto3.Session(
        aws_access_key_id = os.getenv("aws_access_key_id"),
        aws_secret_access_key = os.getenv("aws_secret_access_key"),
        region_name = os.getenv("region_name"))

    s3 = session.client('s3')

    df = pd.read_csv("/opt/airflow/my_data/my_retail_s3.csv")          # change
    cus = df[["Customer ID", "Country"]]
    cus = cus.drop_duplicates(subset=['Customer ID'])
    cus = cus.dropna()
    cus["Customer ID"] = cus["Customer ID"].astype(int)
    cus = cus.sort_values(by="Customer ID")
    cus.to_csv(data_Fname, index=False)

    with open(data_Fname, "rb") as f:
        s3.upload_fileobj(f, bucket_name, data_Fname)
    print("!_upload_to_S3_complete_!")

def customer_Load():

    load_dotenv("/opt/airflow/my_data/.env")

    data_Fname = "customer_data.csv"         # change
    bucket_name = os.getenv("bucket_name")

    conn = psycopg2.connect(
        host = os.getenv("redshift_host"),
        port = os.getenv("redshift_port"),
        dbname = os.getenv("redshift_db"),
        user = os.getenv("redshift_user"),
        password = os.getenv("redshift_password")
    )

    table_name = "Customer_dim"          # change
    s3_path = f"s3://{bucket_name}/{data_Fname}"
    iam_role = os.getenv("iam_role")

    copy_query = f"""
    COPY {table_name}
    FROM '{s3_path}'
    IAM_ROLE '{iam_role}'
    CSV
    IGNOREHEADER 1;
    """

    cursor = conn.cursor()
    try:
        cursor.execute(copy_query)
        conn.commit()
        print("Data loaded successfully into table!")
    except Exception as e:
        conn.rollback()
        print(f"Error loading data into table: {e}")
    finally:
        cursor.close()
        conn.close()

def time_Transform():

    load_dotenv("/opt/airflow/my_data/.env")

    data_Fname = "time_data.csv"         # change
    bucket_name = os.getenv("bucket_name")

    session = boto3.Session(
        aws_access_key_id = os.getenv("aws_access_key_id"),
        aws_secret_access_key = os.getenv("aws_secret_access_key"),
        region_name = os.getenv("region_name"))

    s3 = session.client('s3')

    df = pd.read_csv("/opt/airflow/my_data/my_retail_s3.csv")          # change
    time = df["InvoiceDate"].drop_duplicates()
    time = pd.to_datetime(time)
    time_new = pd.DataFrame({
        'time_ID' : df["InvoiceDate"].drop_duplicates(),
        'fulldate': time,
        'year': time.dt.year,
        'month': time.dt.month,
        'day': time.dt.day,
        'hour': time.dt.hour,
        'minute': time.dt.minute,
    })
    time_new.to_csv(data_Fname, index=False)

    with open(data_Fname, "rb") as f:
        s3.upload_fileobj(f, bucket_name, data_Fname)
    print("!_upload_to_S3_complete_!")

def time_Load():

    load_dotenv("/opt/airflow/my_data/.env")

    data_Fname = "time_data.csv"         # change
    bucket_name = os.getenv("bucket_name")

    conn = psycopg2.connect(
        host = os.getenv("redshift_host"),
        port = os.getenv("redshift_port"),
        dbname = os.getenv("redshift_db"),
        user = os.getenv("redshift_user"),
        password = os.getenv("redshift_password")
    )

    table_name = "Time_dim"          # change
    s3_path = f"s3://{bucket_name}/{data_Fname}"
    iam_role = os.getenv("iam_role")

    copy_query = f"""
    COPY {table_name}
    FROM '{s3_path}'
    IAM_ROLE '{iam_role}'
    CSV
    IGNOREHEADER 1;
    """

    cursor = conn.cursor()
    try:
        cursor.execute(copy_query)
        conn.commit()
        print("Data loaded successfully into table!")
    except Exception as e:
        conn.rollback()
        print(f"Error loading data into table: {e}")
    finally:
        cursor.close()
        conn.close()

def sale_Transform():

    load_dotenv("/opt/airflow/my_data/.env")

    data_Fname = "sale_data.csv"         # change
    bucket_name = os.getenv("bucket_name")

    session = boto3.Session(
        aws_access_key_id = os.getenv("aws_access_key_id"),
        aws_secret_access_key = os.getenv("aws_secret_access_key"),
        region_name = os.getenv("region_name"))

    s3 = session.client('s3')

    df = pd.read_csv("/opt/airflow/my_data/my_retail_s3.csv")          # change
    fact_data = df[["Invoice", 'StockCode', 'Customer ID', 'InvoiceDate', 'Price', 'Quantity']]
    fact_data = fact_data.dropna()
    fact_data["Customer ID"] = fact_data["Customer ID"].astype(int)
    fact_data["Quantity"] = fact_data["Quantity"].astype(int)
    fact_data = fact_data[fact_data["Quantity"].apply(lambda x: str(x).isdigit())]
    fact_data["Quantity"] = fact_data["Quantity"].apply(lambda x: abs(x))
    fact_data.to_csv(data_Fname, index=False)

    with open(data_Fname, "rb") as f:
        s3.upload_fileobj(f, bucket_name, data_Fname)
    print("!_upload_to_S3_complete_!")

def sale_Load():

    load_dotenv("/opt/airflow/my_data/.env")

    data_Fname = "sale_data.csv"         # change
    bucket_name = os.getenv("bucket_name")

    conn = psycopg2.connect(
        host = os.getenv("redshift_host"),
        port = os.getenv("redshift_port"),
        dbname = os.getenv("redshift_db"),
        user = os.getenv("redshift_user"),
        password = os.getenv("redshift_password")
    )

    table_name = "Sales_fct_table"          # change
    s3_path = f"s3://{bucket_name}/{data_Fname}"
    iam_role = os.getenv("iam_role")

    copy_query = f"""
    COPY {table_name}
    FROM '{s3_path}'
    IAM_ROLE '{iam_role}'
    CSV
    IGNOREHEADER 1;
    """

    cursor = conn.cursor()
    try:
        cursor.execute(copy_query)
        conn.commit()
        print("Data loaded successfully into table!")
    except Exception as e:
        conn.rollback()
        print(f"Error loading data into table: {e}")
    finally:
        cursor.close()
        conn.close()