import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.trigger_rule import TriggerRule


# CONFIGURATION
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "car-sales-elt")
REGION = "us-central1"
BUCKET = os.getenv("GCP_GCS_BUCKET", "default_bucket")

LOCAL_DATA_PATH = "/opt/airflow/data/car_sales_data.csv"
UPLOAD_TO_GCS_SCRIPT_PATH = "/opt/airflow/scripts/upload_to_gcs.py"

LOCAL_PYSPARK_PATH = "/opt/airflow/scripts/dataproc_transformation.py"
PYSPARK_GCS_DST = "scripts/dataproc_transformation.py"
PYSPARK_URI = f"gs://{BUCKET}/{PYSPARK_GCS_DST}"

# RAW input: unique per run
RAW_OBJECT = "raw_data/car_sales_data_{{ ts_nodash }}.csv"
RAW_DATA_PATH = f"gs://{BUCKET}/{RAW_OBJECT}"

# PROCESSED output: unique per run (what you want)
# Example becomes: processed_data/car_sales_data_20260305T075311/part-....parquet
PROCESSED_RUN_PREFIX = "processed_data/car_sales_data_{{ ts_nodash }}/"
PROCESSED_DATA_PATH = f"gs://{BUCKET}/{PROCESSED_RUN_PREFIX}"

BQ_DATASET = "car_sales_data"
BQ_TABLE = "cleaned_car_sales"

CLUSTER_NAME = "airflow-dataproc-cluster-{{ ts_nodash | lower }}"

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "gce_cluster_config": {"zone_uri": "us-central1-a"},
    "software_config": {"properties": {"dataproc:dataproc.allow.zero.workers": "true"}},
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": PYSPARK_URI,
        "args": [RAW_DATA_PATH, PROCESSED_DATA_PATH],  # <-- now run-specific output path
    },
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="car_sales_end_to_end_ingest_transform_load",
    default_args=default_args,
    description="Upload CSV to GCS, Dataproc transform, load to BigQuery, cleanup",
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["car_sales", "gcs", "dataproc", "bigquery", "end_to_end"],
) as dag:

    upload_raw_data_to_gcs = BashOperator(
        task_id="upload_raw_data_to_gcs",
        bash_command=(
            "set -euo pipefail; "
            f"python {UPLOAD_TO_GCS_SCRIPT_PATH} "
            f"--source_file {LOCAL_DATA_PATH} "
            f"--bucket_name {BUCKET} "
            f'--destination_blob_name "{RAW_OBJECT}"'
        ),
    )

    upload_pyspark_script = LocalFilesystemToGCSOperator(
        task_id="upload_pyspark_script",
        src=LOCAL_PYSPARK_PATH,
        dst=PYSPARK_GCS_DST,
        bucket=BUCKET,
    )

    check_raw_data_exists_in_gcs = GCSObjectExistenceSensor(
        task_id="check_raw_data_exists_in_gcs",
        bucket=BUCKET,
        object=RAW_OBJECT,  # object name only (no gs://)
        mode="reschedule",
        poke_interval=15,
        timeout=10 * 60,
    )

    check_pyspark_script_exists_in_gcs = GCSObjectExistenceSensor(
        task_id="check_pyspark_script_exists_in_gcs",
        bucket=BUCKET,
        object=PYSPARK_GCS_DST,  # object name only (no gs://)
        mode="reschedule",
        poke_interval=15,
        timeout=10 * 60,
    )

    create_dataproc_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        job=PYSPARK_JOB,
        region=REGION,
        project_id=PROJECT_ID,
    )

    load_processed_data_to_bq = GCSToBigQueryOperator(
        task_id="load_processed_data_to_bq",
        bucket=BUCKET,
        # load only THIS run's parquet outputs
        source_objects=[f"{PROCESSED_RUN_PREFIX}*.parquet"],
        destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "MONTH", "field": "sales_yearmonth"},
        autodetect=True,
    )

    delete_dataproc_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        upload_raw_data_to_gcs
        >> upload_pyspark_script
        >> [check_raw_data_exists_in_gcs, check_pyspark_script_exists_in_gcs]
        >> create_dataproc_cluster
        >> submit_pyspark_job
        >> load_processed_data_to_bq
        >> delete_dataproc_cluster
    )
