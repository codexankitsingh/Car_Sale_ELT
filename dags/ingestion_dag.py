import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Fetch settings from container environment variables instead of hard-coding
# These environment variables come from our local .env file
GCS_BUCKET_NAME = os.getenv("GCP_GCS_BUCKET", "default_bucket")
LOCAL_DATA_PATH = "/opt/airflow/data/car_sales_data.csv"
SCRIPT_PATH = "/opt/airflow/scripts/upload_to_gcs.py"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '1_car_sales_data_ingestion',
    default_args=default_args,
    description='Execute python script to extract local data and load to GCS landing zone',
    schedule_interval=None,
    catchup=False,
    tags=['car_sales', 'ingestion', 'gcs', 'python_script']
) as dag:

    # Task 1: Upload to GCS using a Python Script via BashOperator
    upload_script_task = BashOperator(
        task_id='run_gcs_upload_script',
        bash_command=f"python {SCRIPT_PATH} --source_file {LOCAL_DATA_PATH} --bucket_name {GCS_BUCKET_NAME}",
    )

upload_script_task
