from datetime import datetime, timedelta
from dotenv import load_dotenv
from faker import Faker
from sqlalchemy import create_engine
import argparse
import csv
import math
import os
import pandas as pd
import random


load_dotenv()
min_date = datetime(2023, 10, 18, 1, 20, 0)
ENGINE = create_engine(os.getenv('DB_CONN_STR'))
DIR_DATA = "./data_dag1/"  # Directory where the CSV reports will be saved


def introduce_errors(data):
    '''Introduce inconsistencies, missing values and formatting errors'''
    faker = Faker()

    data['product_name'] = data['product_name'].apply(
        lambda x: x + 'Pr0ductN4m3' if random.random() < 0.06 else x)
    data['category'] = data['category'].apply(
        lambda x: None if random.random() < 0.17 else x)
    data['price'] = data['price'].apply(
        lambda x: None if random.random() < 0.04 else x)
    data['stock_quantity'] = data['stock_quantity'].apply(
        lambda x: None if random.random() < 0.05 else x)
    data['image_url'] = data['image_url'].apply(
        lambda x: faker.word() if random.random() < 0.22 else x)
    data['product_rating'] = data['product_rating'].apply(
        lambda x: round(random.uniform(5, 10), 2) if random.random() < 0.095 else x)
    return data


def generate_csv_file(ini_date: datetime, end_date: datetime, file_number: int, flg_force: bool):
    '''PH'''
    csv_file_name = f"{DIR_DATA}product_{ini_date.strftime('%Y%m%d-%H%M%S')}.csv"
    message = f'File #{file_number} ({csv_file_name}): '
    if os.path.exists(csv_file_name) and not flg_force:
        message += 'already exists...'
    else:
        sql_query = f"""
            SELECT *
            FROM e_commerce.product
            WHERE created_at >= '{ini_date}' AND created_at <= '{end_date}'
        """
        df = pd.read_sql(sql_query, ENGINE)
        if df.shape[0] == 0:
            message += 'no records found'
        else:
            df = introduce_errors(df)
            df.to_csv(csv_file_name, index=False, quoting=csv.QUOTE_NONNUMERIC)
            message += '.CSV file was generated successfully'
    print(message)


def main(flg_force: bool, flg_min_date: bool):
    '''PH'''
    flg_force = flg_force or not flg_min_date
    ini_date = get_start_date(flg_min_date)
    file_number = 0
    while True:
        file_number += 1
        end_date = ini_date + timedelta(minutes=10, seconds=-1)
        generate_csv_file(ini_date, end_date, file_number, flg_force)
        ini_date = end_date + timedelta(seconds=1)

        if ini_date > datetime.now() + timedelta(hours=5):  # +5 to cover UTC gap
            print('-- !! Done. No more files will be generated. --')
            break


def get_start_date(flg_min_date: bool):
    '''PH'''

    if flg_min_date:
        return min_date

    current_time = datetime.now()
    new_minutes = math.floor(current_time.minute / 10) * 10
    new_start_date = (current_time.replace(
        minute=new_minutes, second=0) - timedelta(minutes=10))
    return new_start_date


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pythoncito to generate .CSV files for e-commerce products")
    parser.add_argument('-f', '--force',
                        action='store_true',
                        help='.CSV will be generated even if they already exist for N-interval')
    parser.add_argument('-m', '--min_date',
                        action='store_true',
                        help='Data generation will start from the min date. If not, it will use now().')
    args = parser.parse_args()
    main(args.force, args.min_date)
