import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 17)
}


with DAG(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    description="This is a custom description for a beautiful day!",
    default_args=default_args,
    schedule_interval='0 */12 * * *'
) as dag:

    # Print task
    def print_message():
        print('Running for hours on end...')

    print_task = PythonOperator(
        task_id='print',
        python_callable=print_message
    )

    dummy_task = DummyOperator(
        task_id='dummy'
    )

    print_task >> dummy_task
