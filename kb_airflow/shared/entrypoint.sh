#!/bin/bash

# Initialize the Airflow metadata database (only run once)
airflow db init

# Apply any pending migrations to the database
airflow db upgrade

# Create an admin user (change the credentials as needed)
airflow users create -r Admin -u admin -p admin -e admin@example.com -f Admin -l User

# Start the scheduler in the background
airflow scheduler &

# Start the webserver in the foreground
airflow webserver
