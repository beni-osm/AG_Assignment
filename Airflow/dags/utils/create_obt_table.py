import logging
from google.cloud import bigquery
from google.oauth2 import service_account
from airflow.exceptions import AirflowFailException



# Step 3: Create Combined Table in BigQuery
def create_obt_table(credentials_path, project_id, dataset_id):
    try:
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)

        combined_table_id = f"{project_id}.{dataset_id}.OneBigTable"

        query = f"""
        CREATE OR REPLACE TABLE `{combined_table_id}` AS
        SELECT 
            u.id AS user_id,
            u.name,
            u.email,
            u.registration_date,
            t.id AS transaction_id,
            t.transaction_date,
            t.amount,
            t.type AS transaction_type,
            p.preferred_language,
            p.notifications_enabled,
            p.marketing_opt_in,
            p.updated_at AS preference_updated_at
        FROM 
            `{project_id}.{dataset_id}.Users` u
        LEFT JOIN 
            `{project_id}.{dataset_id}.Transactions` t
        ON 
            u.id = t.user_id
        LEFT JOIN 
            `{project_id}.{dataset_id}.UserPreferences` p
        ON 
            u.id = p.user_id;
        """

        query_job = client.query(query)
        query_job.result()  # Wait for the query to complete
        logging.info(f"Combined table `{combined_table_id}` created successfully.")
    except Exception as e:
        logging.error(f"Failed to create obt table: {e}")
        raise AirflowFailException("obt table creation failed")
