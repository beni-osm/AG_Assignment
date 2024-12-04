import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

def upload_to_bigquery(df, table_id, credentials_path):
    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"Uploaded data to {table_id}")

if __name__ == "__main__":
    
    users_csv_path = "Data/Users.csv"
    transactions_csv_path = "Data/Transactions.csv"
    user_preferences_csv_path = "Data/UserPreferences.csv"

    # Load data from CSV files
    users_df = pd.read_csv(users_csv_path)
    transactions_df = pd.read_csv(transactions_csv_path)
    user_preferences_df = pd.read_csv(user_preferences_csv_path)

    # Define your BigQuery dataset and table names
    project_id = "assignment-arben"
    dataset_id = "AssignmentDataset"
    credentials_path = "Secrets/assignment-arben-1c928f7196f1.json"

    users_table_id = f"{project_id}.{dataset_id}.Users"
    transactions_table_id = f"{project_id}.{dataset_id}.Transactions"
    user_preferences_table_id = f"{project_id}.{dataset_id}.UserPreferences"

    # Upload data to BigQuery
    upload_to_bigquery(users_df, users_table_id, credentials_path)
    upload_to_bigquery(transactions_df, transactions_table_id, credentials_path)
    upload_to_bigquery(user_preferences_df, user_preferences_table_id, credentials_path)
