from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from raw_data_func import upload_s3, download_s3_clean


from my_TL import location_Transform, location_Load, product_Transform, product_Load, customer_Transform, customer_Load, order_Transform, order_Load, time_Transform, time_Load, ftc_profit_Transform, ftc_profit_Load


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
        task_id='location_Transform',               
        python_callable = location_Transform,
    )

    task4 = PythonOperator(
        task_id='location_Load_redshift',              
        python_callable = location_Load,
    )

    task5 = PythonOperator(
        task_id='product_Transform',              
        python_callable = product_Transform,
    )

    task6 = PythonOperator(
        task_id='product_Load_redshift',              
        python_callable = product_Load,
    )

    task7 = PythonOperator(
        task_id='customer_Transform',              
        python_callable = customer_Transform,
    )

    task8 = PythonOperator(
        task_id='customer_Load_redshift',              
        python_callable = customer_Load,
    )

    task9 = PythonOperator(
        task_id='order_Transform',              
        python_callable = order_Transform,
    )

    task10 = PythonOperator(
        task_id='order_Load_redshift',              
        python_callable = order_Load,
    )

    task11 = PythonOperator(
        task_id='time_Transform',              
        python_callable = time_Transform,
    )

    task12 = PythonOperator(
        task_id='time_Load_redshift',              
        python_callable = time_Load,
    )

    task13 = PythonOperator(
        task_id='ftc_profit_Transform',              
        python_callable = ftc_profit_Transform,
    )

    task14 = PythonOperator(
        task_id='ftc_profit_Load_redshift',              
        python_callable = ftc_profit_Load,
    )



    task1 >> task2
    
    task2 >> [task3, task5, task7, task9, task11]

    task3 >> task4
    task5 >> task6
    task7 >> task8
    task9 >> task10
    task11 >> task12

    [task4, task6, task8, task10, task12] >> task13


    task13 >> task14
