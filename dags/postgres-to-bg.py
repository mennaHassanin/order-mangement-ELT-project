from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
import logging
import pandas as pd
import io
from datetime import timedelta

GCS_BUCKET = 'ready-d25-postgres-to-gcs'
BQ_PROJECT = 'ready-de-25'
BQ_DATASET = 'playground'
PG_CONN_ID = 'postgres_conn'
FOLDER_NAME = 'menna'
BATCH_SIZE = 10000

TABLES_TO_TRANSFER = ['products', 'product_category_name_translation', 'orders','order_items','customers','geolocation']

with DAG(
    'transfer_postgres_to_bigquery',
    description='transfer tables from postgres database to bigquery dwh',
    start_date=days_ago(3),
    catchup=False,
) as dag_pg:

    def handle_failure(context):
        logging.error(f"Task {context['task_instance'].task_id} failed")
        raise AirflowException("Postgres extraction is failed")
    def append_to_gcs(table, batch_file, **context):
        hook = GCSHook()
        gcs_path = f'{FOLDER_NAME}/{table}/final_table.csv'
        try:
            existing_data = hook.download(bucket_name=GCS_BUCKET, object_name=gcs_path)
            existing_df = pd.read_csv(io.StringIO(existing_data.decode('utf-8')))
        except Exception as e:
            existing_df = pd.DataFrame()
        new_data = hook.download(bucket_name=GCS_BUCKET, object_name=batch_file)
        new_df = pd.read_csv(io.StringIO(new_data.decode('utf-8')))

        combined_df = pd.concat([existing_df, new_df])

        combined_csv = combined_df.to_csv(index=False)
        hook.upload(bucket_name=GCS_BUCKET, object_name=gcs_path, data=combined_csv, timeout=300)

    extract_tasks = []
    
    for table in TABLES_TO_TRANSFER:
        offset = 0
        while True:
            batch_file = f'{FOLDER_NAME}/{table}/batch_{offset}.csv'
            transfer_pg_to_gcs = PostgresToGCSOperator(
                task_id=f'extract_{table}_batch_{offset}_to_gcs',
                postgres_conn_id=PG_CONN_ID,
                sql=f'select * from {table} limit {BATCH_SIZE} offset {offset};',
                bucket=GCS_BUCKET,
                filename=batch_file,
                export_format='csv',
                on_failure_callback=handle_failure,
                execution_timeout=timedelta(hours=1),
            )
            append_task = PythonOperator(
                task_id=f'append_{table}_batch_{offset}_to_gcs',
                python_callable=append_to_gcs,
                op_kwargs={'table': table, 'batch_file':f'{FOLDER_NAME}/{table}/{batch_file}'},
                on_failure_callback=handle_failure,
            )
            transfer_pg_to_gcs >> append_task
            extract_tasks.append(append_task)
            offset += BATCH_SIZE

            if transfer_pg_to_gcs.output is None:
                break

            
        load_gcs_to_bq = GCSToBigQueryOperator(
                task_id=f'load_{table}_to_bq',
                bucket=GCS_BUCKET,
                source_objects=f'{FOLDER_NAME}/{table}/final_table.csv',
                destination_project_dataset_table=f'{BQ_PROJECT}.{BQ_DATASET}.{table}',
                autodetect= True,
                source_format='csv',
                write_disposition='WRITE_TRUNCATE', 
                create_disposition='CREATE_IF_NEEDED',
                on_failure_callback=handle_failure,
                execution_timeout=timedelta(hours=1),
            )

        extract_tasks[-1] >> load_gcs_to_bq
