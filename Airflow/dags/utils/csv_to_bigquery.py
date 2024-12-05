import logging
from google.cloud import bigquery
from google.oauth2 import service_account
from airflow.exceptions import AirflowFailException

def upload_to_bigquery(df, table_id, credentials_path):
    try:
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete
        logging.info(f"Uploaded data to {table_id}")
    except Exception as e:
        logging.error(f"Failed to upload data to {table_id}: {e}")
        raise AirflowFailException("BigQuery upload failed")
