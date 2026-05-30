import os
import requests
from datetime import datetime
from airflow.decorators import dag, task
from airflow import Asset
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator

# ==========================================
# KONFIGURASI & DEKLARASI ASSET
# ==========================================
# Mendeklarasikan Asset yang akan memicu DAG dbt di Sesi 4
SALES_ASSET = Asset("bq://sales-prod.raw.sales")

# Ubah konfigurasi di bawah ini sesuai dengan environment GCP & GitHub Anda
GITHUB_RAW_BASE_URL = "https://raw.githubusercontent.com/saipulrx/dbt-bigquery-colibri/refs/heads/main/seeds/"
GCP_PROJECT_ID = "dwh-bootcamp-bigquery"           # Ganti dengan Project ID GCP Anda
GCS_BUCKET = "raw_data_dwh_modeling"        # Ganti dengan nama Bucket GCS Anda
BQ_DATASET_NAME = "workshop_dwh_bq_dbt"
BQ_LOCATION = "US"
LOCAL_TMP_DIR = "/tmp/airflow_sales_data"

@dag(
    dag_id="lab1_github_csv_to_gcs_to_bq",
    schedule="@daily",
    start_date=datetime(2026, 5, 29),
    catchup=False,
    tags=["workshop", "lab1", "airflow3"]
)
def upload_sales_csv():

    # ------------------------------------------
    # 1. TASK: Download CSV dari GitHub
    # ------------------------------------------
    @task
    def download_from_github() -> list[str]:
        os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
        
        csv_files = [
            "calendar.csv",
            "lookup_product_category.csv",
            "lookup_region_mapping.csv",
            "raw_customer.csv",
            "raw_location.csv",
            "raw_product.csv",
            "raw_sales.csv"
        ]
        
        local_filepaths = []
        for file in csv_files:
            file_url = f"{GITHUB_RAW_BASE_URL}/{file}"
            local_path = os.path.join(LOCAL_TMP_DIR, file)
            
            # Request ke URL raw GitHub (Asumsi Public Repo)
            response = requests.get(file_url)
            response.raise_for_status() 
            
            with open(local_path, "wb") as f:
                f.write(response.content)
            
            local_filepaths.append(local_path)
            
        return local_filepaths

    # ------------------------------------------
    # 2. TASK: Validasi Skema / File
    # ------------------------------------------
    @task
    def validate_schema(filepaths: list[str]) -> list[str]:
        for path in filepaths:
            if os.path.getsize(path) == 0:
                raise ValueError(f"File {path} kosong!")
        return filepaths

    # ------------------------------------------
    # 3. TASK PREPARATION (Untuk Dynamic Mapping)
    # ------------------------------------------
    @task
    def prep_upload_kwargs(filepaths: list[str]) -> list[dict]:
        return [
            {
                "src": path, 
                "dst": f"sales_data/{os.path.basename(path)}"
            } for path in filepaths
        ]

    @task
    def prep_bq_kwargs(filepaths: list[str]) -> list[dict]:
        return [
            {
                "source_objects": [f"sales_data/{os.path.basename(path)}"],
                "destination_project_dataset_table": f"{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{os.path.basename(path).replace('.csv', '')}"
            } for path in filepaths
        ]

    # ==========================================
    # ALUR EKSEKUSI (WORKFLOW)
    # ==========================================
    
    # Menjalankan task Python
    downloaded_files = download_from_github()
    validated_files = validate_schema(downloaded_files)
    
    # Task Upload ke GCS (Paralel)
    upload_kwargs = prep_upload_kwargs(validated_files)
    upload_to_gcs = LocalFilesystemToGCSOperator.partial(
        task_id="upload_to_gcs",
        bucket=GCS_BUCKET,
    ).expand_kwargs(upload_kwargs)

    # Task Membuat Dataset BigQuery Otomatis (Idempoten)
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=BQ_DATASET_NAME,
        project_id=GCP_PROJECT_ID,
        location=BQ_LOCATION,
        exists_ok=True
    )

    # Task Load dari GCS ke BigQuery (Paralel)
    bq_kwargs = prep_bq_kwargs(validated_files)
    load_to_bq = GCSToBigQueryOperator.partial(
        task_id="load_to_bq",
        bucket=GCS_BUCKET,
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        outlets=[SALES_ASSET]  # Men-trigger Airflow Assets
    ).expand_kwargs(bq_kwargs)

    # Task Verifikasi Tabel Utama
    verify_load = BigQueryCheckOperator(
        task_id="verify_load",
        sql=f"SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.raw_sales`",
        use_legacy_sql=False
    )

    # Mendefinisikan urutan eksekusi antar operator (Dependencies)
    upload_to_gcs >> create_dataset >> load_to_bq >> verify_load

# Inisialisasi DAG
upload_sales_csv()