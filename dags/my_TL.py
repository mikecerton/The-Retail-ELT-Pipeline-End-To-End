import os 
import boto3
from dotenv import load_dotenv
import pandas as pd
import boto3
import psycopg2
from io import StringIO


def upload_s3(my_Fname, csv_buffer):

    load_dotenv("/opt/airflow/my_data/.env")

    bucket_name = os.getenv("bucket_name")
    session = boto3.Session(
        aws_access_key_id = os.getenv("aws_access_key_id"),
        aws_secret_access_key = os.getenv("aws_secret_access_key"),
        region_name = os.getenv("region_name"))

    s3 = session.client('s3')

    s3.put_object(Bucket=bucket_name, Key=my_Fname, Body=csv_buffer.getvalue())
    print(f"!_S3_upload_complete_{my_Fname}!")

def upload_redshift(my_Fname, table_name):

    load_dotenv("/opt/airflow/my_data/.env")

    bucket_name = os.getenv("bucket_name")

    conn = psycopg2.connect(
        host = os.getenv("redshift_host"),
        port = os.getenv("redshift_port"),
        dbname = os.getenv("redshift_db"),
        user = os.getenv("redshift_user"),
        password = os.getenv("redshift_password")
    )

    s3_path = f"s3://{bucket_name}/{my_Fname}"
    iam_role = os.getenv("iam_role")

    copy_query = f"""
    COPY {table_name}
    FROM '{s3_path}'
    IAM_ROLE '{iam_role}'
    CSV
    IGNOREHEADER 1;
    """

    cursor = conn.cursor()
    cursor.execute(copy_query)
    conn.commit()
    print("Data loaded successfully into table!")
    cursor.close()
    conn.close()


def location_Transform_Load():

    my_Fname = "location_data.csv"        

    df = pd.read_csv("/opt/airflow/my_data/my_retail_s3.csv")         
    csv_buffer = StringIO()
    regions = df[['Postal Code', 'Country', 'Region', 'State', 'City']].drop_duplicates(subset=['Postal Code'])
    # print(regions.info())
    regions.to_csv(csv_buffer, index=False)

    upload_s3(my_Fname, csv_buffer)

    upload_redshift(my_Fname, "dim_locations")



def product_Transform_Load():

    my_Fname = "product_data.csv"       

    df = pd.read_csv("/opt/airflow/my_data/my_retail_s3.csv")        
    csv_buffer = StringIO()

    product = df[['Product ID', 'Category', 'Sub-Category', 'Product Name']].drop_duplicates(subset=['Product ID'])
    # print(product.info())

    product.to_csv(csv_buffer, index=False)

    upload_s3(my_Fname, csv_buffer)

    upload_redshift(my_Fname, "dim_products")


def customer_Transform_Load():

    my_Fname = "customer_data.csv"       

    df = pd.read_csv("/opt/airflow/my_data/my_retail_s3.csv")        
    csv_buffer = StringIO()
    customer = df[['Customer ID', 'Customer Name', 'Segment']].drop_duplicates(subset=['Customer ID'])
    print(customer.info())
    customer.to_csv(csv_buffer, index=False)

    upload_s3(my_Fname, csv_buffer)

    upload_redshift(my_Fname, "dim_customers")

def order_Transform_Load():

    my_Fname = "order_data.csv"         

    df = pd.read_csv("/opt/airflow/my_data/my_retail_s3.csv")        
    csv_buffer = StringIO()
    order = df[['Order ID', 'Order Date', 'Ship Date', 'Ship Mode']].drop_duplicates(subset=['Order ID'])
    order['Order Date'] = pd.to_datetime(order['Order Date'], dayfirst=True)
    order['Ship Date'] = pd.to_datetime(order['Ship Date'], dayfirst=True)
    order.to_csv(csv_buffer, index=False)

    upload_s3(my_Fname, csv_buffer)

    upload_redshift(my_Fname, "dim_orders")


def time_Transform_Load():

    my_Fname = "time_data.csv"         
  
    df = pd.read_csv("/opt/airflow/my_data/my_retail_s3.csv")          
    csv_buffer = StringIO()
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
    time.to_csv(csv_buffer, index=False)

    upload_s3(my_Fname, csv_buffer)

    upload_redshift(my_Fname, "dim_time")


def ftc_profit_Transform_Load():

    my_Fname = "ftc_profit_data.csv"         

    df = pd.read_csv("/opt/airflow/my_data/my_retail_s3.csv")          
    csv_buffer = StringIO()
    ftc_profit = df[['Order Date', 'Order ID', 'Customer ID', 'Product ID', 'Postal Code', 'Sales', 'Profit', 'Quantity', 'Discount', 'Segment', 'State', 'Region', 'Category']]
    ftc_profit.to_csv(csv_buffer, index=False)

    upload_s3(my_Fname, csv_buffer)

    upload_redshift(my_Fname, "fact_profit_rep")
