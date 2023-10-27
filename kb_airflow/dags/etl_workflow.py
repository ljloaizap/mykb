import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
from sqlalchemy import create_engine

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 18)
}


def get_csv_list_per_interval(ini_date: datetime, end_date: datetime):
    '''PH'''

    return ['product_20231026-170000.csv']


with DAG(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    description='ETL to process e-commerce product data from .CSV files',
    default_args=default_args,
    schedule_interval='0 5 * * *'
) as dag:

    ###################### Functions ######################
    def connect_to_db_with_sql_alchemy(conn_str):
        '''PH'''
        conn_str = 'postgresql://postgres:postgres@172.20.0.1:5432/postgres'
        engine = create_engine(conn_str)

        try:
            with engine.connect() as connection:
                print('>> Connection was successful!!!!')
                result = connection.execute(
                    "SELECT count(*) FROM e_commerce.product;")
                row_count = result.scalar()
                print(f">> Row count in 'product' table: {row_count}")
        except Exception as ex:
            print(f'>> Errorcillo: {ex} <<<')

    def extracts_csv_data():
        '''PH'''
        load_dotenv()
        conn_str = os.getenv('DB_CONN_STR')
        print(f'Extracting .CSV data... {conn_str} -- v10')
        connect_to_db_with_sql_alchemy(conn_str)

    def transform_data():
        '''PH'''
        print('Transforming data...')

    def load_data():
        '''PH'''
        print('Loading data...')

    ###################### Tasks ######################
    task_extract = PythonOperator(
        task_id='extract_data',
        python_callable=extracts_csv_data
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
