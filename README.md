# Car Sales Data Pipeline (ELT)

An automated Data Engineering pipeline built with Apache Airflow (Dockerized), Google Cloud Storage (GCS), Dataproc (PySpark), and BigQuery.

## 🏗 Architecture
1. **Local Environment:** Airflow running in Docker.
2. **Data Ingestion:** Airflow `LocalFilesystemToGCSOperator` and custom Python scripts upload local raw CSV data to GCS.
3. **Data Transformation:** Airflow dynamically provisions an ephemeral **GCP Dataproc Cluster**. It submits a PySpark job that cleans null values, formats date/time strings into native `DATE` types, adds an ETL timestamp, and saves the partitioned Parquet files back to GCS.
4. **Data Loading:** Airflow `GCSToBigQueryOperator` loads the processed Parquet files into a time-partitioned BigQuery table.
5. **Cleanup:** Airflow automatically tears down the Dataproc cluster to save cloud costs.

## 🚀 How to Run on a New Machine

### Prerequisites
* Docker / Docker Desktop installed
* Git installed
* A Google Cloud Project with Billing Enabled

### 1. Clone the Repository
```bash
git clone <your-repo-url>
cd Car_Sale_ELT
```

### 2. Configure Secrets and Credentials
> **⚠️ IMPORTANT:** Never commit `.env` or Service Account JSON keys to version control!

1. Create a `.env` file in the root directory:
```text
GCP_PROJECT_ID=your-gcp-project-id
GCP_GCS_BUCKET=your-gcs-bucket-name
GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/.google/credentials/gcp-key.json
```

2. Add your Google Cloud Service Account JSON key:
```bash
mkdir -p .google/credentials
# Place your gcp-key.json file inside this new folder
```
*(Ensure the Service Account has roles: Storage Object Admin, BigQuery Admin, Dataproc Administrator, and Service Account User).*

### 3. Add Raw Data
Ensure your source data file (`car_sales_data.csv`) is placed inside the local `./data/` folder.

### 4. Start the Airflow Docker Environment
```bash
# This will build the custom image with GCP/PySpark dependencies and start the containers
docker compose up -d --build
```

### 5. Configure Airflow GCP Connection
1. Navigate to the Airflow UI at `http://localhost:8080` (Default login: `airflow` / `airflow`).
2. Go to **Admin** -> **Connections**.
3. Click the **+** button to add a new connection.
4. Configure as follows:
   * **Connection Id:** `google_cloud_default`
   * **Connection Type:** `Google Cloud`
   * **Project Id:** `your-gcp-project-id`
   * **Keyfile Path:** `/opt/airflow/.google/credentials/gcp-key.json`
5. Save the connection.

### 6. Run the Pipeline
In the Airflow UI, trigger the DAGs in this order:
1. `1_local_to_gcs_ingestion`
2. `2_dataproc_to_bigquery_pipeline`
