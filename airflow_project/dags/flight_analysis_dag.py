import os
import logging
import pandas as pd
import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Import custom modules from scripts folder
from ingest_data import ingest_csv_to_mysql
from validate_transform_kpi import validate_and_transform_data
from load_data_postgres import load_data_to_postgres

# Setup simple logger
logger = logging.getLogger("flight_analysis_dag_logger")
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)

MYSQL_CONN_ID = "mysql_staging_conn"
POSTGRES_CONN_ID = "postgres_analytics_conn"

MYSQL_STAGING_TABLE = os.getenv('STAGING_TABLE_NAME')
RAW_DATA_FILE = os.getenv('RAW_DATA_FILE_PATH')

def get_mysql_conn_details(conn_id):
    try:
        hook = MySqlHook(mysql_conn_id=conn_id)
        conn = hook.get_connection(conn_id)
        return {
            "user": conn.login,
            "password": conn.password,
            "host": conn.host,
            "port": int(conn.port) if conn.port else 3306,
            "db": conn.schema
        }
    except Exception as e:
        logger.warning(f"Failed to get MySQL connection '{conn_id}': {e}")
        return {
            "user": os.getenv('MYSQL_USER'),
            "password": os.getenv('MYSQL_PASSWORD'),
            "host": os.getenv('MYSQL_HOST_FOR_AIRFLOW'),
            "port": int(os.getenv('MYSQL_PORT', 3306)),
            "db": os.getenv('MYSQL_DATABASE')
        }

def get_postgres_conn_details(conn_id):
    try:
        hook = PostgresHook(postgres_conn_id=conn_id)
        conn = hook.get_connection(conn_id)
        return {
            "user": conn.login,
            "password": conn.password,
            "host": conn.host,
            "port": int(conn.port) if conn.port else 5432,
            "db": conn.schema
        }
    except Exception as e:
        logger.warning(f"Failed to get Postgres connection '{conn_id}': {e}")
        return {
            "user": os.getenv('POSTGRES_USER'),
            "password": os.getenv('POSTGRES_PASSWORD'),
            "host": os.getenv('POSTGRES_HOST_FOR_AIRFLOW'),
            "port": int(os.getenv('POSTGRES_PORT', 5432)),
            "db": os.getenv('POSTGRES_DB')
        }

def ingest_task_callable(**kwargs):
    try:
        conn = get_mysql_conn_details(MYSQL_CONN_ID)
        if not all(conn.values()) or not MYSQL_STAGING_TABLE or not RAW_DATA_FILE:
            raise ValueError("Missing MySQL connection details, staging table, or raw data file path.")
        ingest_csv_to_mysql(
            csv_file_path=RAW_DATA_FILE,
            db_user=conn["user"],
            db_password=conn["password"],
            db_host=conn["host"],
            db_port=conn["port"],
            db_name=conn["db"],
            table_name=MYSQL_STAGING_TABLE
        )
        logger.info("Ingest task completed successfully.")
    except Exception as e:
        logger.error(f"Ingest task failed: {e}")
        raise

def validate_transform_kpi_task_callable(**kwargs):
    ti = kwargs['ti']
    try:
        conn = get_mysql_conn_details(MYSQL_CONN_ID)
        if not all(conn.values()) or not MYSQL_STAGING_TABLE:
            raise ValueError("Missing MySQL connection details or staging table.")
        kpi_dfs = validate_and_transform_data(
            db_user=conn["user"],
            db_password=conn["password"],
            db_host=conn["host"],
            db_port=conn["port"],
            db_name=conn["db"],
            input_table_name=MYSQL_STAGING_TABLE
        )
        if not kpi_dfs:
            logger.warning("No KPI dataframes returned from validation/transform.")
            return
        for key, df in kpi_dfs.items():
            ti.xcom_push(key=f"df_{key}_json", value=df.to_json(orient='split', date_format='iso'))
        logger.info("Validate and transform task completed successfully.")
    except Exception as e:
        logger.error(f"Validate/transform task failed: {e}")
        raise

def load_to_postgres_task_callable(**kwargs):
    ti = kwargs['ti']
    try:
        conn = get_postgres_conn_details(POSTGRES_CONN_ID)
        if not all(conn.values()):
            raise ValueError("Missing Postgres connection details.")
        keys = ['main_data', 'avg_fare_airline', 'seasonal_fare_variation', 'booking_count_airline', 'popular_routes']
        data_frames = {}
        for key in keys:
            df_json = ti.xcom_pull(task_ids='validate_transform_kpi_task', key=f'df_{key}_json')
            if df_json:
                data_frames[key] = pd.read_json(df_json, orient='split')
            else:
                logger.warning(f"No data for key df_{key}_json; loading empty DataFrame.")
                data_frames[key] = pd.DataFrame()
        load_data_to_postgres(
            data_frames_dict=data_frames,
            db_user=conn["user"],
            db_password=conn["password"],
            db_host=conn["host"],
            db_port=conn["port"],
            db_name=conn["db"]
        )
        logger.info("Load to Postgres task completed successfully.")
    except Exception as e:
        logger.error(f"Load to Postgres task failed: {e}")
        raise

default_args = {
    'owner': 'airflow_admin',
    'depends_on_past': False,
    'email': ['cl.kofisena@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    dag_id='flight_analysis_dag_Courage_Sena',
    default_args=default_args,
    description='Airflow DAG for Flight Analysis Project',
    schedule='@daily',
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=['flight_analysis'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    ingest_task = PythonOperator(
        task_id='ingest_task',
        python_callable=ingest_task_callable,
    )

    validate_transform_kpi_task = PythonOperator(
        task_id='validate_transform_kpi_task',
        python_callable=validate_transform_kpi_task_callable,
    )

    load_to_postgres_task = PythonOperator(
        task_id='load_to_postgres_task',
        python_callable=load_to_postgres_task_callable,
    )

    start >> ingest_task >> validate_transform_kpi_task >> load_to_postgres_task >> end
