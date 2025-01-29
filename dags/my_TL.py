import os 
import boto3
from dotenv import load_dotenv
import pandas as pd
import boto3
import psycopg2


def location_Transform():

    load_dotenv("/opt/airflow/my_data/.env")

    data_Fname = "location_data.csv"         # change
    bucket_name = os.getenv("bucket_name")

    session = boto3.Session(
        aws_access_key_id = os.getenv("aws_access_key_id"),
        aws_secret_access_key = os.getenv("aws_secret_access_key"),
        region_name = os.getenv("region_name"))

    s3 = session.client('s3')

    df = pd.read_csv("/opt/airflow/my_data/my_retail_s3.csv")          # change
    regions = df[['Postal Code', 'Country', 'Region', 'State', 'City']].drop_duplicates(subset=['Postal Code'])
    print(regions.info())
    regions.to_csv(data_Fname, index=False)

    with open(data_Fname, "rb") as f:
        s3.upload_fileobj(f, bucket_name, data_Fname)
    print("!_upload_to_S3_complete_!")

def location_Load():

    load_dotenv("/opt/airflow/my_data/.env")

    data_Fname = "location_data.csv"         # change
    bucket_name = os.getenv("bucket_name")

    conn = psycopg2.connect(
        host = os.getenv("redshift_host"),
        port = os.getenv("redshift_port"),
        dbname = os.getenv("redshift_db"),
        user = os.getenv("redshift_user"),
        password = os.getenv("redshift_password")
    )

    table_name = "dim_locations"          # change
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
    product = df[['Product ID', 'Category', 'Sub-Category', 'Product Name']].drop_duplicates(subset=['Product ID'])
    print(product.info())
    product.to_csv(data_Fname, index=False)

    with open(data_Fname, "rb") as f:
        s3.upload_fileobj(f, bucket_name, data_Fname)
    print("!_upload_to_S3_complete_!")

def product_Load():

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

    table_name = "dim_products"          # change
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
    customer = df[['Customer ID', 'Customer Name', 'Segment']].drop_duplicates(subset=['Customer ID'])
    print(customer.info())
    customer.to_csv(data_Fname, index=False)

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

    table_name = "dim_customers"          # change
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

def order_Transform():

    load_dotenv("/opt/airflow/my_data/.env")

    data_Fname = "order_data.csv"         # change
    bucket_name = os.getenv("bucket_name")

    session = boto3.Session(
        aws_access_key_id = os.getenv("aws_access_key_id"),
        aws_secret_access_key = os.getenv("aws_secret_access_key"),
        region_name = os.getenv("region_name"))

    s3 = session.client('s3')

    df = pd.read_csv("/opt/airflow/my_data/my_retail_s3.csv")          # changes
    order = df[['Order ID', 'Order Date', 'Ship Date', 'Ship Mode']].drop_duplicates(subset=['Order ID'])
    order['Order Date'] = pd.to_datetime(order['Order Date'], dayfirst=True)
    order['Ship Date'] = pd.to_datetime(order['Ship Date'], dayfirst=True)
    order.to_csv(data_Fname, index=False)

    with open(data_Fname, "rb") as f:
        s3.upload_fileobj(f, bucket_name, data_Fname)
    print("!_upload_to_S3_complete_!")

def order_Load():

    load_dotenv("/opt/airflow/my_data/.env")

    data_Fname = "order_data.csv"         # change
    bucket_name = os.getenv("bucket_name")

    conn = psycopg2.connect(
        host = os.getenv("redshift_host"),
        port = os.getenv("redshift_port"),
        dbname = os.getenv("redshift_db"),
        user = os.getenv("redshift_user"),
        password = os.getenv("redshift_password")
    )

    table_name = "dim_orders"          # change
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

    df = pd.read_csv("/opt/airflow/my_data/my_retail_s3.csv")          # changes
    time = df['Order Date'].drop_duplicates()
    time_new = pd.to_datetime(time, dayfirst=True)
    time = pd.DataFrame({
        'time_ID' : time,
        'fulldate': time_new,
        'year': time_new.dt.year,
        'month': time_new.dt.month,
        'day': time_new.dt.day,
        'hour': time_new.dt.hour,
        'minute': time_new.dt.minute,
    })
    time.to_csv(data_Fname, index=False)

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

    table_name = "dim_time"          # change
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