from datetime import datetime, timedelta
from dotenv import load_dotenv
from faker import Faker
from sqlalchemy import create_engine
import argparse
import csv
import os
import pandas as pd
import random


load_dotenv()
start_date = datetime(2023, 10, 18, 1, 22, 0)
ENGINE = create_engine(os.getenv('DB_CONN_STR'))
DIR_DATA = "./data/"  # Directory where the CSV reports will be saved


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


def generate_csv_file(current_timestamp: datetime, end_timestamp: datetime, force: bool):
    '''PH'''
    csv_file_name = f"{DIR_DATA}product_{current_timestamp.strftime('%Y%m%d-%H%M%S')}.csv"
    if os.path.exists(csv_file_name) and not force:
        print(f'{csv_file_name} already exists...')
    else:
        sql_query = f"SELECT * FROM e_commerce.product WHERE created_at >= '{current_timestamp}' AND created_at < '{end_timestamp}'"
        df = pd.read_sql_query(sql_query, ENGINE)
        df = introduce_errors(df)
        df.to_csv(csv_file_name, index=False, quoting=csv.QUOTE_NONNUMERIC)
        print(f"CSV report generated and saved to: {csv_file_name}")


def main(force: bool):
    '''PH'''
    current_timestamp = start_date
    # Infinite loop to generate CSV reports between specified interval
    while True:
        end_timestamp = current_timestamp + timedelta(minutes=10, seconds=-1)
        generate_csv_file(current_timestamp, end_timestamp, force)
        current_timestamp = end_timestamp + timedelta(seconds=1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pythoncito to generate .CSV files for e-commerce products")
    parser.add_argument('-f', '--force',
                        action='store_true',
                        help='.CSV will be generated even if they already exist for N-interval')
    args = parser.parse_args()
    main(args.force)
