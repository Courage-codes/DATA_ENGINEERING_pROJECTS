import pytest
from unittest.mock import patch, MagicMock
import pandas as pd
import flight_analysis_dag  # Import your DAG module normally due to PYTHONPATH

@pytest.fixture(scope="session")
def dagbag():
    from airflow.models import DagBag
    return DagBag(dag_folder="dags/", include_examples=False)

def test_dag_imports(dagbag):
    assert dagbag.import_errors == {}

def test_dag_loaded(dagbag):
    dag = dagbag.get_dag("flight_analysis_dag_Courage_Sena")
    assert dag is not None
    assert dag.dag_id == "flight_analysis_dag_Courage_Sena"
    assert len(dag.tasks) == 5  # start, end + 3 tasks

def test_task_dependencies(dagbag):
    dag = dagbag.get_dag("flight_analysis_dag_Courage_Sena")
    ingest = dag.get_task("ingest_task")
    validate = dag.get_task("validate_transform_kpi_task")
    load = dag.get_task("load_to_postgres_task")

    assert ingest.downstream_task_ids == {"validate_transform_kpi_task"}
    assert validate.downstream_task_ids == {"load_to_postgres_task"}

@patch("flight_analysis_dag.MySqlHook")
def test_get_mysql_conn_details(mock_mysql_hook):
    mock_conn = MagicMock()
    mock_conn.login = "user"
    mock_conn.password = "pass"
    mock_conn.host = "host"
    mock_conn.port = 3306
    mock_conn.schema = "db"
    mock_mysql_hook.return_value.get_connection.return_value = mock_conn

    conn = flight_analysis_dag.get_mysql_conn_details("mysql_staging_conn")

    assert conn["user"] == "user"
    assert conn["password"] == "pass"
    assert conn["host"] == "host"
    assert conn["port"] == 3306
    assert conn["db"] == "db"

@patch("flight_analysis_dag.ingest_csv_to_mysql")
@patch("flight_analysis_dag.get_mysql_conn_details")
def test_ingest_task_callable_success(mock_get_conn, mock_ingest):
    mock_get_conn.return_value = {
        "user": "user",
        "password": "pass",
        "host": "host",
        "port": 3306,
        "db": "db"
    }

    import os
    os.environ['STAGING_TABLE_NAME'] = "test_table"
    os.environ['RAW_DATA_FILE_PATH'] = "/tmp/fake.csv"

    flight_analysis_dag.ingest_task_callable()

    mock_ingest.assert_called_once()

@patch("flight_analysis_dag.validate_and_transform_data")
def test_validate_transform_kpi_task_callable_success(mock_validate_transform):
    df = pd.DataFrame({"A": [1, 2]})
    mock_validate_transform.return_value = {
        "main_data": df,
        "avg_fare_airline": df,
        "seasonal_fare_variation": df,
        "booking_count_airline": df,
        "popular_routes": df,
    }

    class DummyTI:
        def __init__(self):
            self.pushed = {}

        def xcom_push(self, key, value):
            self.pushed[key] = value

    ti = DummyTI()

    flight_analysis_dag.validate_transform_kpi_task_callable(ti=ti)

    for key in mock_validate_transform.return_value.keys():
        assert f"df_{key}_json" in ti.pushed

@patch("flight_analysis_dag.load_data_to_postgres")
@patch("flight_analysis_dag.get_postgres_conn_details")
def test_load_to_postgres_task_callable_success(mock_get_conn, mock_load):
    df = pd.DataFrame({"A": [1, 2]}).to_json(orient='split')
    xcom_data = {
        'df_main_data_json': df,
        'df_avg_fare_airline_json': df,
        'df_seasonal_fare_variation_json': df,
        'df_booking_count_airline_json': df,
        'df_popular_routes_json': df,
    }

    class DummyTI:
        def xcom_pull(self, task_ids, key):
            return xcom_data.get(key, None)

    ti = DummyTI()

    mock_get_conn.return_value = {
        "user": "user",
        "password": "pass",
        "host": "host",
        "port": 5432,
        "db": "db"
    }

    flight_analysis_dag.load_to_postgres_task_callable(ti=ti)

    mock_load.assert_called_once()
