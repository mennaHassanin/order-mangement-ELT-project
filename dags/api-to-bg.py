from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import requests
import logging

GCS_BUCKET = 'ready-d25-postgres-to-gcs'
BQ_PROJECT = 'ready-de-25'
BQ_DATASET = 'olist_menna'
FOLDER_NAME = 'menna'

API_ENDPOINTS = {
    'order_payments': 'https://us-central1-ready-de-25.cloudfunctions.net/order_payments_table',
    'sellers': 'https://us-central1-ready-de-25.cloudfunctions.net/sellers_table',
}

DEFAULT_ARGS = {
    'retries': 2,
}

def create_api_to_bq_dag(endpoint_name, api_url):
    def fetch_data_from_api():
        gcs_hook = GCSHook()
        gcs_path = f'{FOLDER_NAME}/{endpoint_name}/{endpoint_name}.csv'
        
        # fetch data from api
        response = requests.get(api_url)
        if response.status_code != 200:
            raise Exception(f"failed to fetch data from api: {response.status_code}, {response.text}")
        if not response.content :
            raise Exception("the response is empty")
        
        gcs_hook.upload(
            bucket_name=GCS_BUCKET,
            object_name=gcs_path,
            data=response.content,
            mime_type='text/csv',
        )
        logging.info(f"Data from {endpoint_name} uploaded to GCS: {gcs_path}")


    with DAG(
        f'transfer_{endpoint_name}_api_to_bg_menna',
        default_args=DEFAULT_ARGS,
        description=f'DAG to transfer data from {endpoint_name} API to BigQuery',
        start_date=days_ago(1),
        catchup=False,
    ) as dag:
        fetch_api_to_gcs= PythonOperator(
            task_id=f'fetch_{endpoint_name}_to_gcs',
            python_callable=fetch_data_from_api,
        )
        load_to_bq_task = GCSToBigQueryOperator(
            task_id=f'load_{endpoint_name}_to_bq',
            bucket=GCS_BUCKET,
            source_objects=f'{FOLDER_NAME}/{endpoint_name}/{endpoint_name}.csv',
            destination_project_dataset_table=f'{BQ_PROJECT}.{BQ_DATASET}.{endpoint_name}',
            source_format='CSV',
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED',
            autodetect=True,
        )
        
        fetch_api_to_gcs >> load_to_bq_task
    
    return dag


for endpoint_name, api_url in API_ENDPOINTS.items(): # generate a dag for each api endpoint
    globals()[f'transfer_{endpoint_name}_api_to_bq_menna'] = create_api_to_bq_dag(endpoint_name, api_url)
