from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
import json
from pathlib import Path

GCS_BUCKET = 'ready-d25-postgres-to-gcs'
BQ_PROJECT = 'ready-de-25'
BQ_DATASET = 'olist_menna'
PG_CONN_ID = 'postgres_conn'
FOLDER_NAME = 'menna'
TABLES_TO_TRANSFER = ['products', 'product_category_name_translation', 'orders','order_items','customers','geolocation']


def create_table_execution_dag(table):
    current_file_path = Path(__file__).resolve()
    parent_directory = current_file_path.parent
    schema_file_path = parent_directory / f"{table}.json"

    with open(schema_file_path) as schema_file:
        schema_fields = json.load(schema_file)
    
    with DAG(
        f'transfer_{table}_pg-to-bq-menna',
        start_date=days_ago(1),
        catchup=False,
    ) as dag:
        extract_pg_to_gcs = PostgresToGCSOperator(
            task_id=f'extract_{table}_to_gcs',
            postgres_conn_id=PG_CONN_ID,
            sql=f'select * from {table};',
            bucket=GCS_BUCKET,
            filename=f'{FOLDER_NAME}/{table}/{table}.csv',
            export_format='csv',
        )


        load_pg_to_bq = GCSToBigQueryOperator(
            task_id=f'load_{table}_to_bq',
            bucket=GCS_BUCKET,
            source_objects=[f'{FOLDER_NAME}/{table}/{table}.csv'],
            destination_project_dataset_table=f'{BQ_PROJECT}.{BQ_DATASET}.{table}',
            source_format='CSV',
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED',
            skip_leading_rows=1,
            schema_fields = schema_fields,
        )


        extract_pg_to_gcs >> load_pg_to_bq
        return dag

for table in TABLES_TO_TRANSFER:
    globals()[f'transfer_{table}_pg_to_bq_menna'] = create_table_execution_dag(table)
