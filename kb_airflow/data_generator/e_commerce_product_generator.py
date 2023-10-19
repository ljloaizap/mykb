import random
import time
from datetime import datetime
from faker import Faker
import psycopg2
from psycopg2.extras import execute_values
import sys


def get_product_categories():
    """PH"""

    return [
        "Electronics",
        "Clothing",
        "Home and Garden",
        "Health and Beauty",
        "Automotive",
        "Sports and Outdoors",
        "Toys and Games",
        "Jewelry",
        "Books and Literature",
        "Food and Groceries",
        "Furniture",
        "Pet Supplies",
        "Office Supplies",
        "Music and Instruments",
        "Baby and Toddler",
        "Art and Craft Supplies",
        "Shoes and Footwear",
        "Watches",
        "Tools and Hardware",
        "Kitchen and Dining",
        "Travel and Luggage",
        "Fitness and Exercise",
        "Home Improvement",
        "Computers and Accessories",
        "Party and Event Supplies",
        "Gardening and Outdoor Decor",
        "Beauty and Personal Care",
        "Cameras and Photography",
        "School and Educational Supplies",
        "Home Appliances"
    ]


def generate_fake_report():
    '''PH'''

    # Establish a connection to your PostgreSQL database
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="postgres",
        host="localhost"
    )
    cursor = conn.cursor()
    categories = get_product_categories()

    # Create a Faker instance to generate dummy data
    fake = Faker()

    while True:
        try:
            # Generate a single record
            record = (
                fake.word(),                    # Product Name
                fake.sentence(),                # Description
                random.choice(categories),      # Category
                random.uniform(10, 1000),       # Price
                random.randint(0, 100),         # Stock Quantity
                fake.word(),                    # SKU
                fake.company(),                 # Brand
                fake.image_url(),               # Image URL
                random.uniform(0.1, 50),        # Weight
                random.uniform(1, 100),         # Length
                random.uniform(1, 100),         # Width
                random.uniform(1, 100),         # Height
                round(random.uniform(1, 5), 2),  # Product Rating
            )

            # Insert the record into the "products" table
            cursor = conn.cursor()
            insert_query = '''
                INSERT INTO e_commerce.product
                    (product_name, description, category, price, stock_quantity, sku, brand, image_url, weight, length, width, height, product_rating)
                VALUES %s
            '''
            execute_values(cursor, insert_query, [record])
            conn.commit()
            cursor.close()
            print(
                f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f")}] Record created...')

            # Wait for a random time before creating the next record
            time.sleep(get_sleep_time())
        except KeyboardInterrupt:
            # Stop the program when interrupted (e.g., using Ctrl+C)
            conn.close()
            sys.exit(1)
        except Exception as e:
            print(f'There was an error: {e=}')
            sys.exit(1)


def get_sleep_time():
    '''PH'''
    random_part = int(random.uniform(1, 11))
    if random_part in (1, 2, 3, 4, 5):
        return random.uniform(0, 0.4)
    if random_part in (6, 7, 8, 9):
        return random.uniform(0.5, 4)
    if random_part == 10:
        return random.uniform(3, 10)
    return random.uniform(11, 37)


if __name__ == "__main__":
    generate_fake_report()
