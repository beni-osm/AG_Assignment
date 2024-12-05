import pandas as pd
import random
import logging
from faker import Faker
from airflow.exceptions import AirflowFailException


# Initialize Faker
fake = Faker()


def generate_user_data(num_users = 100):
    # Generating sample data for Users table
    try:
        users_data = {
            'id': [i for i in range(1, num_users + 1)],
            'name': [fake.name() for _ in range(num_users)],
            'registration_date': [fake.date_between(start_date='-1y', end_date='today') for _ in range(num_users)],
            'email': [fake.email() for _ in range(num_users)]
        }
        return pd.DataFrame(users_data)
    except Exception as e:
        logging.error(f"Failed to save User data: {e}")
        raise AirflowFailException("User data creation failed")

def generate_transactions_data(num_users = 100):
    
    try:
        # Generating sample data for Transactions table
        transactions_data = {
            'id': [i for i in range(1, num_users * 2 + 1)],
            'user_id': [random.randint(1, num_users) for _ in range(num_users * 2)],
            'transaction_date': [fake.date_between(start_date='-1y', end_date='today') for _ in range(num_users * 2)],
            'amount': [round(random.uniform(10.0, 1000.0), 2) for _ in range(num_users * 2)],
            'type': [random.choice(['deposit', 'withdrawal']) for _ in range(num_users * 2)]
        }
        return pd.DataFrame(transactions_data)
    except Exception as e:
        logging.error(f"Failed to save Transaction data: {e}")
        raise AirflowFailException("Transaction data creation failed")

def generate_user_preferences_data(num_users = 100):
    
    try:
        # Generating sample data for UserPreferences table
        user_preferences_data = {
            'id': [i for i in range(1, num_users + 1)],
            'user_id': [i for i in range(1, num_users + 1)],
            'preferred_language': [random.choice(['English', 'Spanish', 'French', 'German']) for _ in range(num_users)],
            'notifications_enabled': [random.choice([True, False]) for _ in range(num_users)],
            'marketing_opt_in': [random.choice([True, False]) for _ in range(num_users)],
            'created_at': [fake.date_time_between(start_date='-1y', end_date='now') for _ in range(num_users)],
            'updated_at': [fake.date_time_between(start_date='-1y', end_date='now') for _ in range(num_users)]
        }
        return pd.DataFrame(user_preferences_data)
    except Exception as e:
        logging.error(f"Failed to save UserPreferences data: {e}")
        raise AirflowFailException("UserPreferences data creation failed")
