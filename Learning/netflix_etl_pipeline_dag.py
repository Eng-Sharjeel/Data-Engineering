from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from Netflix_ETL_Pipeline import Netflix_ETL

etl=Netflix_ETL()

def run_initialize_logger(**kwargs):
    etl.initialize_logger()
    #print("Current dir:", os.getcwd())
    #print("Files:", os.listdir(os.getcwd()))


def run_detect_encoding(**kwargs):
    encd=etl.detect_encoding()
    print(encd)
    return encd

def run_extract_csv_data(**kwargs):
    ti=kwargs['ti']
    encd=ti.xcom_pull(task_ids='detect_encoding_task')
    df=etl.extract_csv_data(encd)
    #return df

def run_transform():
    #ti=kwargs['ti']
    #df=ti.xcom_pull(task_ids='extract_csv_data_task')
    df_t=etl.transform()


def run_load_to_snowflake():
    etl.load_to_snowflake()


default_args={
    'owner':'airflow',
    'depends_on_past':False,
    'email_on_failure':False,
    'email_on_retry':False,
    'retries':1,
    'retry_delay':timedelta(minutes=5)
}

dag=DAG(
    'netflix_etl_pipeline',
    default_args=default_args,
    description='DAG for running Netflix ETL Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2025, 10, 3),
    catchup=False
    #tags=['netflix', 'etl']
)

initialize_logger_task=PythonOperator(
    task_id='initialize_logger_task',
    python_callable=run_initialize_logger,
    dag=dag
)

detect_encoding_task=PythonOperator(
    task_id='detect_encoding_task',
    python_callable=run_detect_encoding,
    dag=dag
)

extract_csv_data_task=PythonOperator(
    task_id='extract_csv_data_task',
    python_callable=run_extract_csv_data,
    dag=dag
)

transform_task=PythonOperator(
    task_id='transform_task',
    python_callable=run_transform,
    dag=dag
)

load_to_snowflake_task=PythonOperator(
    task_id='load_to_snowflake_task',
    python_callable=run_load_to_snowflake,
    dag=dag
)

netflix_locations_snowflake_task=SnowflakeOperator(
    task_id='fetch_from_snowflake_task',
    snowflake_conn_id='snowflake_default',
    sql="""
    CREATE OR REPLACE TABLE "netflix_shows_locations" AS
        SELECT 
            "show_id",
            TRIM(f.value) AS country
        FROM "netflix_shows",
        LATERAL FLATTEN(input => SPLIT("country", ',')) f;
    """
)


initialize_logger_task >> detect_encoding_task >> extract_csv_data_task >> transform_task >> load_to_snowflake_task >> netflix_locations_snowflake_task

