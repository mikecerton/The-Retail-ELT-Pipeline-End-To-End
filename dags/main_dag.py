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

    task1 = PythonOperator(
        task_id='upload_s3',
        python_callable = upload_s3,
    )

    task2 = PythonOperator(
        task_id='download_s3_clean',
        python_callable = download_s3_clean,
    )

    task3 = PythonOperator(
        task_id='location_Transform_Load',               
        python_callable = location_Transform_Load,
    )

    task4 = PythonOperator(
        task_id='product_Transform_Load',              
        python_callable = product_Transform_Load,
    )

    task5 = PythonOperator(
        task_id='customer_Transform_Load',              
        python_callable = customer_Transform_Load,
    )

    task6 = PythonOperator(
        task_id='order_Transform_Load',              
        python_callable = order_Transform_Load,
    )

    task7 = PythonOperator(
        task_id='time_Transform_Load',              
        python_callable = time_Transform_Load,
    )

    task8 = PythonOperator(
        task_id='ftc_profit_Transform_Load',              
        python_callable = ftc_profit_Transform_Load,
    )

    task1 >> task2 >> [task3, task4, task5, task6, task7] >> task8