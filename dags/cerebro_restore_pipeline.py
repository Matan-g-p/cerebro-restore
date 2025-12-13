from airflow import DAG
import sqlalchemy
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from helpers.minio import read_csv_from_minio, read_json_from_minio, read_parquet_from_minio
from config.settings import Config

import pandas as pd
import json
import io
import logging
from datetime import datetime, timedelta

# Configure logging
logger = logging.getLogger(__name__)

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
    logger.info("Starting schema creation")
    hook = PostgresHook(postgres_conn_id=Config.POSTGRES_CONN_ID)
    hook.run("CREATE SCHEMA IF NOT EXISTS raw;")
    print("Schemas created successfully.")


def ingest_heroes_to_raw(**context):
    """Ingest heroes data from MinIO to PostgreSQL raw schema."""
    start_time = datetime.now()
    logger.info("Starting heroes ingestion", extra={
        "dag_id": context["dag"].dag_id,
        "run_id": context["run_id"]
    })
    
    try:
        heroes = read_csv_from_minio("heroes.csv", bucket=Config.BUCKET_NAME)
        logger.info(f"Read {len(heroes)} heroes from MinIO")
        
        postgres = PostgresHook(postgres_conn_id=Config.POSTGRES_CONN_ID)
        engine = postgres.get_sqlalchemy_engine()
            
        heroes.to_sql('heroes', engine, if_exists='append', index=False, schema=Config.RAW_SCHEMA)
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Successfully loaded {len(heroes)} rows to raw.heroes", extra={
            "duration_seconds": duration,
            "row_count": len(heroes)
        })
        
    except Exception as e:
        logger.error(f"Failed to ingest heroes: {str(e)}", exc_info=True)
        raise


def ingest_missions_to_raw(**context):
    """Ingest missions data from MinIO to PostgreSQL raw schema."""
    start_time = datetime.now()
    logger.info("Starting missions ingestion", extra={
        "dag_id": context["dag"].dag_id,
        "run_id": context["run_id"]
    })
    
    try:
        missions_data = read_json_from_minio("mission_logs.json", bucket=Config.BUCKET_NAME)
        missions = pd.DataFrame(missions_data)
        logger.info(f"Read {len(missions)} missions from MinIO")
        
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

        postgres = PostgresHook(postgres_conn_id=Config.POSTGRES_CONN_ID)
        engine = postgres.get_sqlalchemy_engine()
            
        missions.to_sql('missions', engine, if_exists='append', index=False, schema=Config.RAW_SCHEMA)
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Successfully loaded {len(missions)} rows to raw.missions", extra={
            "duration_seconds": duration,
            "row_count": len(missions)
        })
        
    except Exception as e:
        logger.error(f"Failed to ingest missions: {str(e)}", exc_info=True)
        raise


def ingest_biometrics_to_raw(**context):
    """Ingest biometrics data from MinIO to PostgreSQL raw schema with aggregation."""
    start_time = datetime.now()
    logger.info("Starting biometrics ingestion", extra={
        "dag_id": context["dag"].dag_id,
        "run_id": context["run_id"]
    })
    
    try:
        biometrics = read_parquet_from_minio("biometrics.parquet", bucket=Config.BUCKET_NAME)
        logger.info(f"Read {len(biometrics)} biometric records from MinIO")

        if biometrics['hero_id'].isnull().any():
            null_count = biometrics['hero_id'].isnull().sum()
            logger.warning(f"Found {null_count} null hero_ids in biometrics data")

        biometrics['timestamp'] = pd.to_datetime(biometrics['timestamp'])

        logger.info("Aggregating biometrics to 1-minute intervals")
        aggregated_biometrics = (
            biometrics
            .set_index("timestamp")
            .groupby("hero_id")
            .resample("1min")
            .mean()
            .reset_index()
        )
        logger.info(f"Aggregated to {len(aggregated_biometrics)} records")

        postgres = PostgresHook(postgres_conn_id=Config.POSTGRES_CONN_ID)
        engine = postgres.get_sqlalchemy_engine()
        
        aggregated_biometrics.to_sql('biometrics', engine, if_exists='append', index=False, schema=Config.RAW_SCHEMA)
        
        duration = (datetime.now() - start_time).total_seconds()
        logger.info(f"Successfully loaded {len(aggregated_biometrics)} rows to raw.biometrics", extra={
            "duration_seconds": duration,
            "row_count": len(aggregated_biometrics),
            "original_count": len(biometrics)
        })
        
    except Exception as e:
        logger.error(f"Failed to ingest biometrics: {str(e)}", exc_info=True)
        raise


def truncate_raw_data():
    """Truncate raw tables after successful dbt transformation."""
    logger.info("Starting raw data truncation")
    
    try:
        hook = PostgresHook(postgres_conn_id=Config.POSTGRES_CONN_ID)
        engine = hook.get_sqlalchemy_engine()
        
        tables = ['raw.heroes', 'raw.missions', 'raw.biometrics']
        for table in tables:
            logger.info(f"Truncating {table}")
            engine.execute(f"truncate table {table}")
        
        logger.info("Raw data truncated successfully")
        
    except Exception as e:
        logger.error(f"Failed to truncate raw data: {str(e)}", exc_info=True)
        raise

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


    dbt_deps = BashOperator(
        task_id='dbt_install_deps',
        bash_command='dbt deps --project-dir /opt/airflow/dags/dbt --profiles-dir /opt/airflow/dags/dbt'
    )


    dbt_build = BashOperator(
        task_id='dbt_build',
        bash_command='dbt build --project-dir /opt/airflow/dags/dbt --profiles-dir /opt/airflow/dags/dbt'
    )

    truncate_raw_data_task = PythonOperator(
        task_id="truncate_raw_data",
        python_callable=truncate_raw_data
    )
    
    create_schemas_task >> [ingest_heroes_task, ingest_missions_task, ingest_biometrics_task] >> dbt_deps >> dbt_build >> truncate_raw_data_task
