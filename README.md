# AG_Assignment

## 1. Create Python Virtual Environment

### Create a Python virtual environment and activate it:

`python3 -m venv assignment_env`
`source assignment_env/bin/activate  # On Windows, use 'assignment_env\Scripts\activate'`

## 2. Set Up BigQuery Project

### Create a project, a dataset, and a service account in Google BigQuery.

`Download the service account key and make sure to store it within the same folder as csv_to_bigquery.py file.`

## 3. Run the Project with Docker Compose

### Once the project has been cloned, use Docker Compose to build and run the containers:

`docker-compose up --build -d`

## 4. Trigger the DAG in Airflow

### Navigate to localhost:8080 in your browser.

`Trigger the DAG named bigquery_etl_dag to run the ETL process.`

## 5. The SQL query exists in the Airflow/SQL folder.