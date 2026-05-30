import os
import requests
import pandas as pd
from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

# ==========================================
# KONFIGURASI
# ==========================================
# Menggunakan URL Raw GitHub yang tepat
GITHUB_RAW_URL = "https://raw.githubusercontent.com/saipulrx/dbt-bigquery-colibri/refs/heads/main/seeds/raw_sales.csv"
GCP_PROJECT_ID = "dwh-bootcamp-bigquery"           # Ganti dengan Project ID GCP Anda
GCS_BUCKET = "raw_data_dwh_modeling"        # Ganti dengan nama Bucket GCS Anda
BQ_DATASET_NAME = "workshop_dwh_bq_dbt"
BQ_LOCATION = "US"
LOCAL_TMP_DIR = "/tmp/airflow_etl_data"

@dag(
    dag_id="lab2_etl_pandas_to_bq",
    schedule="@daily",
    start_date=datetime(2026, 5, 30),
    catchup=False,
    tags=["workshop", "etl", "pandas"]
)
def etl_sales_data():

    # ------------------------------------------
    # 1. EXTRACT: Download dari GitHub
    # ------------------------------------------
    @task
    def extract_data() -> str:
        os.makedirs(LOCAL_TMP_DIR, exist_ok=True)
        local_raw_path = os.path.join(LOCAL_TMP_DIR, "raw_sales.csv")
        
        print(f">> Mengunduh data dari: {GITHUB_RAW_URL}")
        response = requests.get(GITHUB_RAW_URL)
        response.raise_for_status()
        
        with open(local_raw_path, "wb") as f:
            f.write(response.content)
            
        return local_raw_path

    # ------------------------------------------
    # 2. TRANSFORM: Manipulasi Universal dengan Pandas
    # ------------------------------------------
    @task
    def transform_data(raw_file_path: str) -> str:
        print(f">> Membaca data mentah dari {raw_file_path}")
        df = pd.read_csv(raw_file_path)
        
        # Transformasi 1: Membersihkan spasi berlebih (whitespace) di awal/akhir teks
        # Ini mencegah masalah di mana "Jakarta " dianggap berbeda dengan "Jakarta"
        text_columns = df.select_dtypes(include=['object']).columns
        for col in text_columns:
            df[col] = df[col].astype(str).str.strip()
            
        # Transformasi 2: Menghapus baris yang datanya benar-benar duplikat identik
        df = df.drop_duplicates()
        
        # Transformasi 3: Mengisi data kosong (NaN) pada kolom angka menjadi 0
        numeric_columns = df.select_dtypes(include=['number']).columns
        df[numeric_columns] = df[numeric_columns].fillna(0)
            
        # Transformasi 4: Menambahkan kolom metadata (Kapan ETL ini berjalan)
        df['etl_processed_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Menyimpan hasil transformasi ke file CSV baru
        clean_file_path = os.path.join(LOCAL_TMP_DIR, "clean_sales.csv")
        df.to_csv(clean_file_path, index=False)
        print(f">> Data berhasil ditransformasi dan disimpan di {clean_file_path}")
        
        return clean_file_path

    # ------------------------------------------
    # ALUR EKSEKUSI (WORKFLOW)
    # ------------------------------------------
    
    raw_path = extract_data()
    clean_path = transform_data(raw_path)
    
    upload_clean_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_clean_to_gcs",
        src=clean_path,
        dst="processed_data/clean_sales.csv",
        bucket=GCS_BUCKET,
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=BQ_DATASET_NAME,
        project_id=GCP_PROJECT_ID,
        location=BQ_LOCATION,
        exists_ok=True
    )

    load_to_bq = GCSToBigQueryOperator(
        task_id="load_to_bq",
        bucket=GCS_BUCKET,
        source_objects=["processed_data/clean_sales.csv"],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.fact_sales",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
    )

    clean_path >> upload_clean_to_gcs >> create_dataset >> load_to_bq

etl_sales_data()