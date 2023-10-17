# Basic level of complexity

## 1. ETL Workflow

Create a DAG that extracts data from a source (e.g., a `.CSV` file), transforms it (e.g., applying simple data cleaning), and loads it into a destination (e.g., a database).


## 2. Data Pipeline Scheduler

Build a DAG that schedules and orchestrates a series of data pipelines. Each pipeline extracts data from a different source, transforms it, and loads it into a common data warehouse.


## 3. Daily Report Generator

Develop a DAG that generates a daily report. The DAG should trigger a script or task to collect data, process it, and then email the report to specified recipients.


## 4. File Transfer Automation

Create a DAG that monitors a directory for new files, and when a new file is detected, transfers it to another location (e.g., from a local folder to a cloud storage bucket).


## 5. API Data Retrieval

Design a DAG that retrieves data from a public API at regular intervals (e.g., weather data, financial data). Process the data and store it in a database.

<br>

# Increasing Complexity

## 7. Workflow Orchestration

Extend the ETL workflow example to include conditional logic and error handling. For example, if data extraction fails, the DAG should handle the error gracefully.

## 8. Parameterized Reporting

Build a DAG for generating custom reports. Allow users to submit parameters (e.g., date range, filters), and use those parameters to tailor the report content.

## 9. Machine Learning Training and Deployment

Create a DAG that trains a machine learning model at regular intervals, evaluates its performance, and deploys it for predictions. This could involve model versioning and A/B testing.

## 10. Multi-Environment Deployment

Develop a DAG for deploying applications to multiple environments (e.g., development, staging, production). The DAG should allow you to promote code through different stages with defined testing and approval steps.

## 11. Complex Event Processing

Design a DAG that processes complex events, such as analyzing real-time sensor data from IoT devices. The DAG should detect patterns, generate alerts, and store aggregated data.
