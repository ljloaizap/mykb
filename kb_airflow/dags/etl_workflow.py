import os
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator, get_current_context
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pendulum.datetime import DateTime
from pendulum import from_format, now
from sqlalchemy import create_engine

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 1)
}


with DAG(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    description='ETL to process e-commerce product data',
    default_args=default_args,
    schedule_interval='*/20 * * * *'
) as dag:

    ###################### Functions ######################

    def extracts_data():
        '''PH'''
        load_dotenv()
        conn_str = os.getenv('DB_CONN_STR')

        try:
            engine = create_engine(conn_str)
            context = get_current_context()
            str_format = '%Y-%m-%d %H:%M:%S'
            start_date = context['data_interval_start'].strftime(str_format)
            end_date = context['data_interval_end'].strftime(str_format)
            print(f'>> start={start_date}; end={end_date}')

            with engine.connect() as connection:
                result = connection.execute(f"""
                    SELECT count(1)
                    FROM e_commerce.product AS p
                    WHERE p.created_at BETWEEN '{start_date}' AND '{end_date}'
                """)
                row_count = result.scalar()
                print(
                    f">> Row count in 'e_commerce.product' table: {row_count}")
        except Exception as ex:
            print(f'>> Errorcillo: {ex} <<<')

    def transform_data():
        '''PH'''
        print('Transforming data...')

    def load_data():
        '''PH'''
        print('Loading data...')

    ###################### Tasks ######################
    task_extract = PythonOperator(
        task_id='extract_data',
        python_callable=extracts_data
    )

    task_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    task_load = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    task_done = DummyOperator(
        task_id='done'
    )

    task_extract >> task_transform >> task_load >> task_done
