import os 
import boto3
from dotenv import load_dotenv
import pandas as pd
import boto3
import psycopg2
from io import StringIO

# upload data to S3
def upload_s3(my_Fname, csv_buffer):
    
    # Load data from .env file
    load_dotenv("/opt/airflow/.env")

    # connect to s3
    bucket_name = os.getenv("bucket_name")
    session = boto3.Session(
        aws_access_key_id = os.getenv("aws_access_key_id"),
        aws_secret_access_key = os.getenv("aws_secret_access_key"),
        region_name = os.getenv("region_name"))

    s3 = session.client('s3')

    # Upload CSV to S3
    s3.put_object(Bucket=bucket_name, Key=my_Fname, Body=csv_buffer.getvalue())
    print(f"!_S3_upload_complete_{my_Fname}!")

# copy data from S3 to data warehouse in Redshift
def upload_redshift(my_Fname, table_name, sql_table):

    # Load data from .env file
    load_dotenv("/opt/airflow/.env")

    bucket_name = os.getenv("bucket_name")

    # Connect to Redshift
    conn = psycopg2.connect(
        host = os.getenv("redshift_host"),
        port = os.getenv("redshift_port"),
        dbname = os.getenv("redshift_db"),
        user = os.getenv("redshift_user"),
        password = os.getenv("redshift_password")
    )
    s3_path = f"s3://{bucket_name}/{my_Fname}"
    iam_role = os.getenv("iam_role")

    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute(sql_table)
    conn.commit()
    print("table pass")

    # Copy data from S3 to Redshif
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

# ETL for dim_locations
def location_Transform_Load():

    my_Fname = "location_data.csv"        

    # create location data from csv
    df = pd.read_csv("/opt/airflow/my_data/my_retail_s3.csv")         
    csv_buffer = StringIO()
    regions = df[['Postal Code', 'Country', 'Region', 'State', 'City']].drop_duplicates(subset=['Postal Code'])
    regions.to_csv(csv_buffer, index=False)

    sql_table = """
    CREATE TABLE IF NOT EXISTS dim_locations (
        postal_code INT PRIMARY KEY,
        country VARCHAR(50) NOT NULL,
        region VARCHAR(50) NOT NULL,
        state VARCHAR(50) NOT NULL,
        city VARCHAR(100) NOT NULL);
    """

    # upload data to S3 --> create table if not exist --> copy data from S3 to data warehouse in Redshift
    upload_s3(my_Fname, csv_buffer)
    upload_redshift(my_Fname, "dim_locations", sql_table)

# ETL for dim_products
def product_Transform_Load():

    my_Fname = "product_data.csv"       

    # create product data from csv
    df = pd.read_csv("/opt/airflow/my_data/my_retail_s3.csv")        
    csv_buffer = StringIO()
    product = df[['Product ID', 'Category', 'Sub-Category', 'Product Name']].drop_duplicates(subset=['Product ID'])
    product.to_csv(csv_buffer, index=False)

    sql_table = """
    CREATE TABLE IF NOT EXISTS dim_products (
        product_id VARCHAR(20) PRIMARY KEY,
        category VARCHAR(50),
        sub_category VARCHAR(50),
        product_name VARCHAR(255));
    """

    # upload data to S3 --> create table if not exist --> copy data from S3 to data warehouse in Redshift
    upload_s3(my_Fname, csv_buffer)
    upload_redshift(my_Fname, "dim_products", sql_table)

# ETL for dim_customers
def customer_Transform_Load():

    my_Fname = "customer_data.csv"       

    # create customer data from csv
    df = pd.read_csv("/opt/airflow/my_data/my_retail_s3.csv")        
    csv_buffer = StringIO()
    customer = df[['Customer ID', 'Customer Name', 'Segment']].drop_duplicates(subset=['Customer ID'])
    customer.to_csv(csv_buffer, index=False)

    sql_table = """
    CREATE TABLE IF NOT EXISTS dim_customers (
        customer_id VARCHAR(20) PRIMARY KEY,
        customer_name VARCHAR(100),
        segment VARCHAR(50));
    """

    # upload data to S3 --> create table if not exist --> copy data from S3 to data warehouse in Redshift
    upload_s3(my_Fname, csv_buffer)
    upload_redshift(my_Fname, "dim_customers", sql_table)

# ETL for dim_orders
def order_Transform_Load():

    my_Fname = "order_data.csv"         

    # create order data from csv
    df = pd.read_csv("/opt/airflow/my_data/my_retail_s3.csv")        
    csv_buffer = StringIO()
    order = df[['Order ID', 'Order Date', 'Ship Date', 'Ship Mode']].drop_duplicates(subset=['Order ID'])
    order['Order Date'] = pd.to_datetime(order['Order Date'], dayfirst=True)
    order['Ship Date'] = pd.to_datetime(order['Ship Date'], dayfirst=True)
    order.to_csv(csv_buffer, index=False)

    sql_table = """
    CREATE TABLE IF NOT EXISTS dim_orders (
        order_id VARCHAR(20) PRIMARY KEY,
        order_date DATE NOT NULL,
        ship_date DATE NOT NULL,
        ship_mode VARCHAR(50));
    """

    # upload data to S3 --> create table if not exist --> copy data from S3 to data warehouse in Redshift
    upload_s3(my_Fname, csv_buffer)
    upload_redshift(my_Fname, "dim_orders", sql_table)

# ETL for dim_time    
def time_Transform_Load():

    my_Fname = "time_data.csv"         
    
    # create time data from csv
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

    sql_table = """
    CREATE TABLE IF NOT EXISTS dim_time (
        time_id VARCHAR(255) NOT NULL PRIMARY KEY,
        fulldate TIMESTAMP NOT NULL,
        year INT NOT NULL,
        month INT NOT NULL,
        day INT NOT NULL,
        hour INT NOT NULL,
        minute INT NOT NULL);
    """

    # upload data to S3 --> create table if not exist --> copy data from S3 to data warehouse in Redshift
    upload_s3(my_Fname, csv_buffer)
    upload_redshift(my_Fname, "dim_time", sql_table)

# ETL for fact_profit_rep
def ftc_profit_Transform_Load():

    my_Fname = "ftc_profit_data.csv"         

    # create fact_profit_rep data from csv
    df = pd.read_csv("/opt/airflow/my_data/my_retail_s3.csv")          
    csv_buffer = StringIO()
    ftc_profit = df[['Order Date', 'Order ID', 'Customer ID', 'Product ID', 'Postal Code', 'Sales', 'Profit', 'Quantity', 'Discount', 'Segment', 'State', 'Region', 'Category']]
    ftc_profit.to_csv(csv_buffer, index=False)

    sql_table = """
    CREATE TABLE IF NOT EXISTS fact_profit_rep (
        sale_id BIGINT IDENTITY(1, 1) PRIMARY KEY,
        time_id VARCHAR(255) NOT NULL,
        order_id VARCHAR(20) NOT NULL,
        customer_id VARCHAR(20) NOT NULL,
        product_id VARCHAR(20) NOT NULL,
        postal_code INT NOT NULL,
        sales DECIMAL(10, 2),
        profit DECIMAL(10, 2),
        quantity INTEGER,
        discount DECIMAL(5, 2),
        segment VARCHAR(50),         
        state VARCHAR(50),           
        region VARCHAR(50),          
        category VARCHAR(50),
        FOREIGN KEY (time_id) REFERENCES dim_time(time_id),        
        FOREIGN KEY (order_id) REFERENCES dim_orders(order_id),
        FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
        FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
        FOREIGN KEY (postal_code) REFERENCES dim_locations(postal_code));
    """

    # upload data to S3 --> create table if not exist --> copy data from S3 to data warehouse in Redshift
    upload_s3(my_Fname, csv_buffer)
    upload_redshift(my_Fname, "fact_profit_rep", sql_table)
