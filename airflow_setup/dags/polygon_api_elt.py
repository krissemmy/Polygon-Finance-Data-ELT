import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
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
        

    start >> extract_data_tasks >> get_data_tasks >> load_data_tasks


    # extract_data = PolygonToPGOperator(
    #     task_id="extract_data_from_API_and_load_to_Postgres",
    #     key=KEY,
    #     table=TABLE,
    #     type_of_data=TYPE_OF_DATA,
    #     destination_path=DESTINATION_PATH,
    #     yesterday=YESTERDAY,
    #     time=TIME,
    # )

    # get_data = PostgresToGCSOperator(
    #     task_id="transfer_data_from_Postgres_to_GCS",
    #     postgres_conn_id='postgres_default',
    #     sql=f"SELECT * FROM {TABLE} WHERE date = '{YESTERDAY}'",
    #     bucket=DESTINATION_BUCKET,
    #     filename="polygon/"+DESTINATION_PATH.replace('.csv', '.json'),
    #     gzip=False,
    # )

    # load_gcs_to_bigquery =  GCSToBigQueryOperator(
    #     task_id = "load_gcs_to_bigquery",
    #     bucket=f"{DESTINATION_BUCKET}", #BUCKET
    #     source_objects=[f"polygon/{DESTINATION_PATH.replace('.csv', '.json')}"], # SOURCE OBJECT
    #     destination_project_dataset_table=f"{DATASET}.{TABLE}", # `nyc.green_dataset_data` i.e table name
    #     autodetect=True, #DETECT SCHEMA : the columns and the type of data in each columns of the CSV file
    #     write_disposition="WRITE_APPEND", # command to update table from the  latest (or last row) row number upon every job run or task run
    #     source_format="NEWLINE_DELIMITED_JSON",
    # )

    # delete_file = BashOperator(
    #      task_id = "delete_file",
    #      bash_command = f'rm {AIRFLOW_HOME}/{DESTINATION_PATH}'
    # )
 
    # start >> extract_data #>> get_data >> load_gcs_to_bigquery >> delete_file
    


# >> python_task
# def copy_data_to_postgres_table(**kwargs):

#     # Convert the CSV string to a file-like object
#     data_file = DESTINATION_PATH

#     # Specify the target PostgreSQL table
#     target_table = TABLE
#     sql_command = f"COPY {target_table} FROM STDIN CSV"

#     # Instantiate the PostgreSQL hook
#     postgres_hook = PostgresHook(postgres_conn_id='postgres_default')

#     # Execute the copy_expert method
#     postgres_hook.copy_expert(sql_command, data_file)
    
# # Define a PythonOperator to execute the my_python_function function
# python_task = PythonOperator(
#     task_id='execute_copy_of_data_to_postgres_table',
#     python_callable=copy_data_to_postgres_table,
#     dag=dag,
# )
# ingest = PostgresHook.copy_expert(f"COPY {TABLE} FROM STDIN", DESTINATION_PATH)

# get_birth_date = PostgresOperator(
#     task_id="get_birth_date",
#     postgres_conn_id="postgres_default",
#     sql="sql/birth_date.sql",
#     params={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
# )

# get_data = PostgresToGCSOperator(
#     task_id="get_data",
#     postgres_conn_id=CONNECTION_ID,
#     sql=SQL_SELECT,
#     bucket=BUCKET_NAME,
#     filename=FILE_NAME,
#     gzip=False,
# )

# load_gcs_to_bigquery =  GCSToBigQueryOperator(
#     task_id = "load_gcs_to_bigquery",
#     bucket=f"{BUCKET}", #BUCKET
#     source_objects=[f"{OBJECT}.parquet"], # SOURCE OBJECT
#     destination_project_dataset_table=f"{DATASET}.{OBJECT}", # `nyc.green_dataset_data` i.e table name
#     autodetect=True, #DETECT SCHEMA : the columns and the type of data in each columns of the CSV file
#     write_disposition="WRITE_APPEND", # command to update table from the  latest (or last row) row number upon every job run or task run
#     source_format="PARQUET",
# )

# trigger_job_run = DbtCloudRunJobOperator(
#     task_id="trigger_job_run_for_eviction",
#     #dbt_cloud_conn_id = "eviction_dbt_job",
#     job_id=451408,
#     check_interval=10,
#     timeout=300,
# )

# end = EmptyOperator(task_id="end")