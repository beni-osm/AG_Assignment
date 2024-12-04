import pandas as pd
import random
import datetime
from faker import Faker

# Initialize Faker
fake = Faker()

def generate_user_data():
    # Generating sample data for Users table
    num_users = 100
    users_data = {
        'id': [i for i in range(1, num_users + 1)],
        'name': [fake.name() for _ in range(num_users)],
        'registration_date': [fake.date_between(start_date='-1y', end_date='today') for _ in range(num_users)],
        'email': [fake.email() for _ in range(num_users)]
    }
    pd.DataFrame(users_data).to_csv('./Data/Users.csv', index = False)

def generate_transactions_data():
    num_users = 100
    # Generating sample data for Transactions table
    transactions_data = {
        'id': [i for i in range(1, num_users * 2 + 1)],
        'user_id': [random.randint(1, num_users) for _ in range(num_users * 2)],
        'transaction_date': [fake.date_between(start_date='-1y', end_date='today') for _ in range(num_users * 2)],
        'amount': [round(random.uniform(10.0, 1000.0), 2) for _ in range(num_users * 2)],
        'type': [random.choice(['deposit', 'withdrawal']) for _ in range(num_users * 2)]
    }
    pd.DataFrame(transactions_data).to_csv('./Data/Transactions.csv', index = False)

def generate_user_preferences_data():
    num_users = 100
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
    pd.DataFrame(user_preferences_data).to_csv('./Data/UserPreferences.csv', index = False)

# Generate sample data
generate_user_data()
generate_transactions_data()
generate_user_preferences_data()