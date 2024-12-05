import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.email_operator import EmailOperator
from airflow.exceptions import AirflowFailException
from utils.csv_to_bigquery import upload_to_bigquery
from utils.generate_data import generate_user_data, generate_transactions_data, generate_user_preferences_data
from utils.create_obt_table import create_obt_table


def load_and_transform_data(**kwargs):
    
    # Generate sample data
    users_df = generate_user_data
    transactions_df = generate_transactions_data()
    user_preferences_df = generate_user_preferences_data()

    # Define your BigQuery dataset and table names
    project_id = "assignment-arben"
    dataset_id = "AssignmentDataset"
    credentials_path = "/opt/airflow/dags/utils/service-account.json"

    users_table_id = f"{project_id}.{dataset_id}.Users"
    transactions_table_id = f"{project_id}.{dataset_id}.Transactions"
    user_preferences_table_id = f"{project_id}.{dataset_id}.UserPreferences"

    # Upload data to BigQuery
    upload_to_bigquery(users_df, users_table_id, credentials_path)
    upload_to_bigquery(transactions_df, transactions_table_id, credentials_path)
    upload_to_bigquery(user_preferences_df, user_preferences_table_id, credentials_path)


    # Create Combined Table in BigQuery
    create_obt_table(credentials_path, project_id, dataset_id)

# Define the Airflow DAG
with DAG(
    'bigquery_etl_dag',
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(1),
        'email': ['arbenos20@gmail.com'],
        'email_on_failure': True,
        'retries': 1,
    },
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1
) as dag:
    load_and_transform_task = PythonOperator(
        task_id='load_and_transform_data',
        python_callable=load_and_transform_data,
        provide_context=True
    )

    send_email_on_failure = EmailOperator(
        task_id='send_email_on_failure',
        to='arbenos20@gmail.com',
        subject='Airflow Task Failed',
        html_content='One of your Airflow tasks has failed. Please check the logs for details.',
        trigger_rule='one_failed'
    )

    load_and_transform_task >> send_email_on_failure