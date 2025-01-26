from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

from my_func import upload_s3, download_s3
from my_TL import product_Transform, prodct_Load, customer_Transform, customer_Load, time_Transform, time_Load, sale_Transform, sale_Load


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
        task_id='download_s3',
        python_callable = download_s3,
    )

    task3 = PythonOperator(
        task_id='product_Transform',               
        python_callable = product_Transform,
    )

    task4 = PythonOperator(
        task_id='product_Load_redshift',              
        python_callable = prodct_Load,
    )

    task5 = PythonOperator(
        task_id='customer_Transform',              
        python_callable = customer_Transform,
    )

    task6 = PythonOperator(
        task_id='customer_Load_redshift',              
        python_callable = customer_Load,
    )

    task7 = PythonOperator(
        task_id='time_Transform',              
        python_callable = time_Transform,
    )

    task8 = PythonOperator(
        task_id='time_Load_redshift',              
        python_callable = time_Load,
    )

    task9 = PythonOperator(
        task_id='sale_Transform',              
        python_callable = sale_Transform,
    )

    task10 = PythonOperator(
        task_id='sale_Load_redshift',              
        python_callable = sale_Load,
    )

    # task1 >> task2 >> [[task3 >> task4], [task5 >> task6], [task7 >> task8]] >> task9 >> task10

    task1 >> task2 
    task2 >> [task3, task5, task7]  # Split into parallel tasks
    task3 >> task4  # Product transform and load
    task5 >> task6  # Customer transform and load
    task7 >> task8  # Time transform and load
    [task4, task6, task8] >> task9  # Merge back for sale transform
    task9 >> task10  # Sale load to Redshift
