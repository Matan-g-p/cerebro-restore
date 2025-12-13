from airflow import DAG
import sqlalchemy
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from helpers.minio.operations import read_csv_from_minio, read_json_from_minio, read_parquet_from_minio

import pandas as pd
import json
import io
from datetime import datetime, timedelta


BUCKET_NAME = "cerebro-data"
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def create_schemas():
    """Creates the target schemas in Postgres if they don't exist."""
    hook = PostgresHook(postgres_conn_id='cerebro_db')
    hook.run("CREATE SCHEMA IF NOT EXISTS raw;")
    hook.run("CREATE SCHEMA IF NOT EXISTS cerebro;")
    print("Schemas created successfully.")


def ingest_heroes_to_raw(**context):
    heroes = read_csv_from_minio("heroes.csv", bucket=BUCKET_NAME)
    
    postgres = PostgresHook(postgres_conn_id="cerebro_db")
    engine = postgres.get_sqlalchemy_engine()
        
    heroes.to_sql('heroes', engine, if_exists='append', index=False, schema='raw')
    print(f"Loaded {len(heroes)} rows to raw.heroes")


def ingest_missions_to_raw(**context):
    missions_data = read_json_from_minio("mission_logs.json", bucket=BUCKET_NAME)
    missions = pd.DataFrame(missions_data)
    
    def safe_json_dumps(x):
        if isinstance(x, (list, dict)):
            return json.dumps(x)
        if pd.isna(x):
            return None
        return json.dumps(x)

    if 'hero_ids' in missions.columns:
        missions['hero_ids'] = missions['hero_ids'].apply(safe_json_dumps)
    
    if 'damage_report' in missions.columns:
        missions['damage_report'] = missions['damage_report'].apply(safe_json_dumps)

    postgres = PostgresHook(postgres_conn_id="cerebro_db")
    engine = postgres.get_sqlalchemy_engine()
        
    missions.to_sql('missions', engine, if_exists='append', index=False, schema='raw')
    print(f"Loaded {len(missions)} rows to raw.missions")


def ingest_biometrics_to_raw(**context):
    biometrics = read_parquet_from_minio("biometrics.parquet", bucket=BUCKET_NAME)

    if biometrics['hero_id'].isnull().any():
        print("Warning: Null hero_ids found in biometrics")

    biometrics['timestamp'] = pd.to_datetime(biometrics['timestamp'])

    aggregated_biometrics = (
        biometrics
        .set_index("timestamp")
        .groupby("hero_id")
        .resample("1min")
        .mean()
        .reset_index()
    )

    postgres = PostgresHook(postgres_conn_id="cerebro_db")
    engine = postgres.get_sqlalchemy_engine()
    
    aggregated_biometrics.to_sql('biometrics', engine, if_exists='append', index=False, schema='raw')
    print(f"Loaded {len(aggregated_biometrics)} gathered rows to raw.biometrics (aggregated)")


def truncate_raw_data():
    hook = PostgresHook(postgres_conn_id="cerebro_db")
    engine = hook.get_sqlalchemy_engine()
    engine.execute("truncate table raw.heroes")
    engine.execute("truncate table raw.missions")
    engine.execute("truncate table raw.biometrics")
    print("Raw data truncated successfully.")

with DAG(
    "cerebro_restore_pipeline",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["cerebro", "restore", "dbt"],
) as dag:

    create_schemas_task = PythonOperator(
        task_id="create_schemas",
        python_callable=create_schemas
    )

    ingest_heroes_task = PythonOperator(
        task_id="ingest_heroes_raw",
        python_callable=ingest_heroes_to_raw
    )

    ingest_missions_task = PythonOperator(
        task_id="ingest_missions_raw",
        python_callable=ingest_missions_to_raw
    )

    ingest_biometrics_task = PythonOperator(
        task_id="ingest_biometrics_raw",
        python_callable=ingest_biometrics_to_raw
    )

    # DBT Tasks
    # Using BashOperator assuming dbt is installed in the Airflow environment
    # Project dir is mapped to /opt/airflow/dags/dbt


    dbt_deps = BashOperator(
        task_id='dbt_install_deps',
        bash_command='dbt deps --project-dir /opt/airflow/dags/dbt --profiles-dir /opt/airflow/dags/dbt'
    )


    # dbt_run = BashOperator(
    #     task_id='dbt_run',
    #     bash_command='dbt run --project-dir /opt/airflow/dags/dbt --profiles-dir /opt/airflow/dags/dbt'
    # )
    
    # dbt_test = BashOperator(
    #     task_id='dbt_test',
    #     bash_command='dbt test --project-dir /opt/airflow/dags/dbt --profiles-dir /opt/airflow/dags/dbt'
    # )

    dbt_build = BashOperator(
        task_id='dbt_build',
        bash_command='dbt build --project-dir /opt/airflow/dags/dbt --profiles-dir /opt/airflow/dags/dbt'
    )

    truncate_raw_data_task = PythonOperator(
        task_id="truncate_raw_data",
        python_callable=truncate_raw_data
    )
    
    create_schemas_task >> [ingest_heroes_task, ingest_missions_task, ingest_biometrics_task] >> dbt_deps >> dbt_build >> truncate_raw_data_task
