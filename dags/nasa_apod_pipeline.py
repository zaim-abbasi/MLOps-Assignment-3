"""
NASA APOD Data Pipeline DAG
This DAG implements a complete ETL pipeline with versioning:
1. Extract: Fetch data from NASA APOD API
2. Transform: Clean and structure data using Pandas
3. Load: Save to PostgreSQL and CSV simultaneously
4. DVC: Version control the CSV file
5. Git: Commit DVC metadata files
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import pandas as pd
import os
import json
from pathlib import Path

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'nasa_apod_pipeline',
    default_args=default_args,
    description='NASA APOD ETL Pipeline with DVC and Git versioning',
    schedule_interval='@daily',  # Run daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['nasa', 'apod', 'etl', 'dvc', 'git'],
)

# Constants
NASA_API_URL = "https://api.nasa.gov/planetary/apod"
NASA_API_KEY = "3vyMWXzx05fzDfQKgb5LQEbTj16WhAct3IsD7xND"
DATA_DIR = "/usr/local/airflow/data"
CSV_FILE = f"{DATA_DIR}/apod_data.csv"
DVC_DIR = "/usr/local/airflow"


def extract_apod_data(**context):
    """
    Step 1: Extract data from NASA APOD API
    """
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    # Fetch data from NASA APOD API
    params = {
        'api_key': NASA_API_KEY,
        'date': date_str
    }
    
    print(f"Fetching APOD data for date: {date_str}")
    response = requests.get(NASA_API_URL, params=params)
    response.raise_for_status()
    
    data = response.json()
    print(f"Successfully fetched data: {data.get('title', 'Unknown')}")
    
    # Store raw data in XCom for next task
    return data


def transform_apod_data(**context):
    """
    Step 2: Transform data using Pandas
    Select specific fields and restructure the data
    """
    # Get data from previous task
    ti = context['ti']
    raw_data = ti.xcom_pull(task_ids='extract_apod_data')
    
    # Extract fields of interest
    transformed_data = {
        'date': raw_data.get('date'),
        'title': raw_data.get('title'),
        'url': raw_data.get('url'),
        'explanation': raw_data.get('explanation'),
        'media_type': raw_data.get('media_type', 'unknown')
    }
    
    # Create DataFrame
    df = pd.DataFrame([transformed_data])
    
    # Ensure date is datetime type
    df['date'] = pd.to_datetime(df['date'])
    
    print(f"Transformed data shape: {df.shape}")
    print(f"Transformed data:\n{df.head()}")
    
    # Convert to dict and ensure date is string for JSON serialization
    result = df.to_dict('records')[0]
    # Convert Timestamp to string for XCom serialization
    if isinstance(result['date'], pd.Timestamp):
        result['date'] = result['date'].strftime('%Y-%m-%d')
    
    # Store DataFrame as JSON in XCom (for passing to next task)
    return result


def load_apod_data(**context):
    """
    Step 3: Load data to PostgreSQL and CSV simultaneously
    """
    # Get transformed data from previous task
    ti = context['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_apod_data')
    
    # Ensure data directory exists
    Path(DATA_DIR).mkdir(parents=True, exist_ok=True)
    
    # Create DataFrame from transformed data
    df = pd.DataFrame([transformed_data])
    df['date'] = pd.to_datetime(df['date'])
    
    # Load to PostgreSQL
    # Connection should be set up in Airflow UI: Admin > Connections
    # Connection ID: postgres_default
    postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Check if record exists (upsert logic)
    check_query = "SELECT COUNT(*) FROM apod_data WHERE date = %s"
    date_value = df['date'].iloc[0].date()
    exists = postgres_hook.get_first(check_query, parameters=(date_value,))[0]
    
    if exists > 0:
        # Update existing record
        update_query = """
            UPDATE apod_data 
            SET title = %s, url = %s, explanation = %s, 
                media_type = %s, updated_at = CURRENT_TIMESTAMP
            WHERE date = %s
        """
        postgres_hook.run(
            update_query,
            parameters=(
                df['title'].iloc[0],
                df['url'].iloc[0],
                df['explanation'].iloc[0],
                df['media_type'].iloc[0],
                date_value
            )
        )
        print(f"Updated existing record for date: {date_value}")
    else:
        # Insert new record
        insert_query = """
            INSERT INTO apod_data (date, title, url, explanation, media_type)
            VALUES (%s, %s, %s, %s, %s)
        """
        postgres_hook.run(
            insert_query,
            parameters=(
                date_value,
                df['title'].iloc[0],
                df['url'].iloc[0],
                df['explanation'].iloc[0],
                df['media_type'].iloc[0]
            )
        )
        print(f"Inserted new record for date: {date_value}")
    
    # Load to CSV (append mode)
    file_exists = os.path.exists(CSV_FILE)
    
    if file_exists:
        # Read existing CSV and append
        existing_df = pd.read_csv(CSV_FILE)
        existing_df['date'] = pd.to_datetime(existing_df['date'])
        
        # Remove duplicate if exists
        existing_df = existing_df[existing_df['date'] != df['date'].iloc[0]]
        
        # Append new data
        combined_df = pd.concat([existing_df, df], ignore_index=True)
        combined_df = combined_df.sort_values('date')
    else:
        combined_df = df
    
    # Save to CSV
    combined_df.to_csv(CSV_FILE, index=False)
    print(f"Data saved to CSV: {CSV_FILE}")
    print(f"Total records in CSV: {len(combined_df)}")
    
    return f"Loaded {len(df)} record(s) to both PostgreSQL and CSV"


def initialize_dvc_if_needed():
    """
    Helper function to initialize DVC if not already initialized
    """
    dvc_path = Path(DVC_DIR) / ".dvc"
    if not dvc_path.exists():
        import subprocess
        subprocess.run(['dvc', 'init', '--no-scm'], cwd=DVC_DIR, check=True)
        print("DVC initialized successfully")


# Task 1: Extract
extract_task = PythonOperator(
    task_id='extract_apod_data',
    python_callable=extract_apod_data,
    dag=dag,
)

# Task 2: Transform
transform_task = PythonOperator(
    task_id='transform_apod_data',
    python_callable=transform_apod_data,
    dag=dag,
)

# Task 3: Load
load_task = PythonOperator(
    task_id='load_apod_data',
    python_callable=load_apod_data,
    dag=dag,
)

# Task 4: DVC - Version control CSV file
dvc_task = BashOperator(
    task_id='version_data_with_dvc',
    bash_command=f"""
    cd {DVC_DIR} && \
    dvc init --no-scm 2>/dev/null || true && \
    dvc add {CSV_FILE} && \
    echo "DVC versioning completed for {CSV_FILE}"
    """,
    dag=dag,
)

# Task 5: Git - Commit DVC metadata
git_task = BashOperator(
    task_id='commit_dvc_metadata',
    bash_command=f"""
    cd {DVC_DIR} && \
    git config --global user.email "airflow@example.com" && \
    git config --global user.name "Airflow" && \
    git init 2>/dev/null || true && \
    git add data/*.dvc .dvc/ 2>/dev/null || true && \
    git commit -m "Add DVC metadata for APOD data - {{{{ ds }}}}" || echo "No changes to commit" && \
    echo "Git commit completed"
    """,
    dag=dag,
)

# Define task dependencies
extract_task >> transform_task >> load_task >> dvc_task >> git_task

