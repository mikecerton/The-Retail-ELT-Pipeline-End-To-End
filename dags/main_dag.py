from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from raw_data_func import upload_s3, download_s3_clean

from my_TL import location_Transform_Load,  product_Transform_Load,  customer_Transform_Load, order_Transform_Load, time_Transform_Load,  ftc_profit_Transform_Load


my_default_args = {
    'owner': 'itsMe!!',
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'main_dag',
    description = 'data engineer project',
    default_args = my_default_args,
    start_date = datetime(2025, 1, 26),  
    schedule_interval = '@once',         
    catchup = False,
) as dag:

    up_s3 = PythonOperator(
        task_id='upload_s3',
        python_callable = upload_s3,
    )

    download_s3 = PythonOperator(
        task_id='download_s3_clean',
        python_callable = download_s3_clean,
    )

    dim_location_Load = PythonOperator(
        task_id='location_Transform_Load',               
        python_callable = location_Transform_Load,
    )

    dim_product_Load = PythonOperator(
        task_id='product_Transform_Load',              
        python_callable = product_Transform_Load,
    )

    dim_customer_Load = PythonOperator(
        task_id='customer_Transform_Load',              
        python_callable = customer_Transform_Load,
    )

    dim_order_Load = PythonOperator(
        task_id='order_Transform_Load',              
        python_callable = order_Transform_Load,
    )

    dim_time_Load = PythonOperator(
        task_id='time_Transform_Load',              
        python_callable = time_Transform_Load,
    )

    ftc_profit_Load = PythonOperator(
        task_id='ftc_profit_Transform_Load',              
        python_callable = ftc_profit_Transform_Load,
    )

    up_s3 >> download_s3 >> [dim_location_Load, dim_product_Load, dim_customer_Load, dim_order_Load, dim_time_Load] >> ftc_profit_Load