import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from web.operators.plg_api_to_pg_to_gcs import PolygonToPGOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator



AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
DESTINATION_BUCKET = os.environ.get("GCP_GCS_BUCKET")
KEY = os.environ.get("POLYGON_API_KEY")
# TYPE_OF_DATA = "stock"
DATASET="finance"

# Get the current date and time
current_datetime = datetime.now()

# Calculate the timedelta for one day
one_day = timedelta(days=1)

# Subtract one day from the current date and time
yesterday_datetime = current_datetime - one_day
YESTERDAY = yesterday_datetime.date()

# DESTINATION_PATH = f'polygon_{TYPE_OF_DATA}_{YESTERDAY}.csv'
# TABLE = f'polygon_{TYPE_OF_DATA}'
# TIME = "{{ dag_run.logical_date.strftime('%Y-%m-%d') }}"
TIME = "{{ dag_run.logical_date.strftime('%Y-%m-%d') }}"



DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2023, 11, 20),
    "email": [os.getenv("ALERT_EMAIL", "")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="Load-Polygon-Finance-Data-From-API-To-PG-To-GCS-TO-BQ",
    description="Job to move data from Eviction website to Google Cloud Storage and then transfer from GCS to BigQuery, and finally create a data model using dbt",
    default_args=DEFAULT_ARGS,
    schedule_interval="30 0 * * 2-6",
    max_active_runs=1,
    catchup=True,
    tags=["Polygon-API-to-Postgres-Table-to-GCS-Bucket-to-BQ"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Create a TaskGroup for each tasks
    with TaskGroup(
        group_id="extract_data_tasks",
        tooltip= "Extract data tasks for different types") as extract_data_tasks:
        # for TYPE_OF_DATA in ["stock", "crypto", "forex"]:
            #extract_data_tasks_start = EmptyOperator(task_id=f"extract_data_tasks_{TYPE_OF_DATA}")
        extract_task_1 = PolygonToPGOperator(
            task_id=f"extract_stock_data_from_API_and_load_to_Postgres",
            type_of_data="stock",
            table='polygon_stock',
            #destination_path=f'polygon_stock_{YESTERDAY}.csv',
            key=KEY,
            yesterday=YESTERDAY,
            time=TIME,
        )
        extract_task_2 = PolygonToPGOperator(
            task_id=f"extract_forex_data_from_API_and_load_to_Postgres",
            type_of_data="forex",
            table='polygon_forex',
            #destination_path=f'polygon_forex_{YESTERDAY}.csv',
            key=KEY,
            yesterday=YESTERDAY,
            time=TIME,
        )
        extract_task_3 = PolygonToPGOperator(
            task_id=f"extract_crypto_data_from_API_and_load_to_Postgres",
            type_of_data="crypto",
            table='polygon_crypto',
            #destination_path=f'polygon_crypto_{YESTERDAY}.csv',
            key=KEY,
            yesterday=YESTERDAY,
            time=TIME,
        )
        [extract_task_1,extract_task_2,extract_task_3]
    with TaskGroup(
        group_id="get_data_tasks",
        tooltip= "Get data tasks for different types") as get_data_tasks:
        get_data_1 = PostgresToGCSOperator(
            task_id="transfer_stock_data_from_Postgres_to_GCS",
            postgres_conn_id='postgres_default',
            sql=f"SELECT * FROM polygon_stock WHERE date = '{YESTERDAY}'",
            bucket=DESTINATION_BUCKET,
            filename=f"polygon/polygon_stock_{YESTERDAY}.json",
            gzip=False,
        )
        get_data_2 = PostgresToGCSOperator(
            task_id="transfer_forex_data_from_Postgres_to_GCS",
            postgres_conn_id='postgres_default',
            sql=f"SELECT * FROM polygon_forex WHERE date = '{YESTERDAY}'",
            bucket=DESTINATION_BUCKET,
            filename=f"polygon/polygon_forex_{YESTERDAY}.json",
            gzip=False,
        )
        get_data_3 = PostgresToGCSOperator(
            task_id="transfer_crypto_data_from_Postgres_to_GCS",
            postgres_conn_id='postgres_default',
            sql=f"SELECT * FROM polygon_crypto WHERE date = '{YESTERDAY}'",
            bucket=DESTINATION_BUCKET,
            filename=f"polygon/polygon_crypto_{YESTERDAY}.json",
            gzip=False,
        )
        [get_data_1, get_data_2, get_data_3]
    with TaskGroup(
        group_id="load_data_tasks",
        tooltip= "Load data tasks for different types") as load_data_tasks:
        load_gcs_to_bigquery_1 =  GCSToBigQueryOperator(
            task_id = "load_stock_data_from_gcs_to_bigquery",
            bucket=f"{DESTINATION_BUCKET}", #BUCKET
            source_objects=[f"polygon/polygon_stock_{YESTERDAY}.json"], # SOURCE OBJECT
            destination_project_dataset_table=f"{DATASET}.polygon_stock", # `nyc.green_dataset_data` i.e table name
            autodetect=True, #DETECT SCHEMA : the columns and the type of data in each columns of the CSV file
            write_disposition="WRITE_APPEND", # command to update table from the  latest (or last row) row number upon every job run or task run
            source_format="NEWLINE_DELIMITED_JSON",
        )
        load_gcs_to_bigquery_2 =  GCSToBigQueryOperator(
            task_id = "load_forex_data_from_gcs_to_bigquery",
            bucket=f"{DESTINATION_BUCKET}", #BUCKET
            source_objects=[f"polygon/polygon_forex_{YESTERDAY}.json"], # SOURCE OBJECT
            destination_project_dataset_table=f"{DATASET}.polygon_forex", # `nyc.green_dataset_data` i.e table name
            autodetect=True, #DETECT SCHEMA : the columns and the type of data in each columns of the CSV file
            write_disposition="WRITE_APPEND", # command to update table from the  latest (or last row) row number upon every job run or task run
            source_format="NEWLINE_DELIMITED_JSON",
        )
        load_gcs_to_bigquery_3 =  GCSToBigQueryOperator(
            task_id = "load_crypto_data_from_gcs_to_bigquery",
            bucket=f"{DESTINATION_BUCKET}", #BUCKET
            source_objects=[f"polygon/polygon_crypto_{YESTERDAY}.json"], # SOURCE OBJECT
            destination_project_dataset_table=f"{DATASET}.polygon_crypto", # `nyc.green_dataset_data` i.e table name
            autodetect=True, #DETECT SCHEMA : the columns and the type of data in each columns of the CSV file
            write_disposition="WRITE_APPEND", # command to update table from the  latest (or last row) row number upon every job run or task run
            source_format="NEWLINE_DELIMITED_JSON",
        )
        [load_gcs_to_bigquery_1, load_gcs_to_bigquery_2, load_gcs_to_bigquery_3]
        
    trigger_job_run = DbtCloudRunJobOperator(
        task_id="trigger_job_run_for_Polygon_Finnce_data-Stock-Forex-Crypto",
        #dbt_cloud_conn_id = "eviction_dbt_job", 221209
        job_id=462857,
        check_interval=10,
        timeout=300,
    )

    end = EmptyOperator(task_id="end")

    start >> extract_data_tasks >> get_data_tasks >> load_data_tasks >> trigger_job_run >> end
