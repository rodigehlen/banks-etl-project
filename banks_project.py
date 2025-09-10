import requests
import sqlite3
import numpy as np
import pandas as pd
from datetime import datetime

DATA_URL = (
    'https://web.archive.org/web/20230908091635/'
    'https://en.wikipedia.org/wiki/List_of_largest_banks'
)
EXCHANGE_DATA_URL = (
    'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/'
    'IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv'
)

FILE_PATH = 'Largest_banks_data.csv'
DB_NAME = 'Banks.db'
TABLE_NAME = 'Largest_banks'
LOG_FILE = 'code_log.txt'


def log_progress(message):
    """Helper function for logging."""
    timestamp_format = '%Y-%b-%d-%H-%M-%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(timestamp + ': ' + message + '\n')


def extract():
    """Extract data from URL sources."""
    try:
        tables = pd.read_html(DATA_URL)
        data = tables[1]
        exchange = pd.read_csv(EXCHANGE_DATA_URL)
        log_progress('Data Extraction successful')
        return data, exchange
    except Exception as e:
        log_progress(f'Data extraction fauled: {e}')
        raise


def transform(data, exchange):
    """Transform extracted data."""
    try:
        data = data.iloc[:, 1:]
        data = data.rename(columns={
            data.columns[0]: 'Name',
            data.columns[1]: 'MC_USD_Billion'
        })
        
        exchange = exchange.set_index(exchange.columns[0])
        data['MC_GBP_Billion'] = round(
            data['MC_USD_Billion'] * exchange.loc['GBP', 'Rate'],
            2
        )
        data['MC_EUR_Billion'] = round(
            data['MC_USD_Billion'] * exchange.loc['EUR', 'Rate'],
            2
        )
        data['MC_INR_Billion'] = round(
            data['MC_USD_Billion'] * exchange.loc['INR', 'Rate'],
            2
        )
        
        log_progress('Data Transformation successful')
        return data
    except Exception as e:
        log_progress(f'Data transformation failed: {e}')
        raise


def load_to_csv(data):
    """Save dataframe to CSV."""
    try:
        data.to_csv(FILE_PATH, index=False)
        log_progress('Data saved to CSV')
    except Exception as e:
        log_progress(f'CSV load failed: {e}')
        raise


def load_to_db(data):
    """Load dataframe to SQLite database."""
    try:
        with sqlite3.connect(DB_NAME) as conn:
            data.to_sql(TABLE_NAME, conn, if_exists='replace', index=False)
        log_progress('Data loaded to database')
    except Exception as e:
        log_progress(f'Database load failed: {e}')
        raise


def run_query(statement):
    """Run a SQL query agaisnt the database."""
    try:
        with sqlite3.connect(DB_NAME) as conn:
            query_output = pd.read_sql(statement, conn)
            print(query_output)
        log_progress(f'Query executed: {statement}')
    except Exception as e:
        log_progress(f'Query failed: {e}')
        raise


def main():
    """Orchestrates ETL pipeline."""
    log_progress('ETL Job Started')
    try:
        data, exchange = extract()
        data = transform(data, exchange)
        load_to_csv(data)
        load_to_db(data)
        run_query('SELECT * FROM Largest_banks')
        run_query('SELECT AVG(MC_GBP_Billion) FROM Largest_banks')
        run_query('SELECT Name FROM Largest_banks LIMIT 5')
        log_progress('ETL Job Finished Successfuly')
    except Exception as e:
        log_progress(f'ETL Job Failed: {e}')


if __name__ == '__main__':
    main()
