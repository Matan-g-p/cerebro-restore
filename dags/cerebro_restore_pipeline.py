from airflow import DAG
import sqlalchemy
from airflow.operators.python import PythonOperator
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


def create_tables():
    """Creates the target tables in Postgres if they don't exist."""
    hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Create Schema
    hook.run("CREATE SCHEMA IF NOT EXISTS cerebro;")

    # Heroes Table
    hook.run("""
        CREATE TABLE IF NOT EXISTS cerebro.dim_heroes (
            hero_id VARCHAR(50) PRIMARY KEY,
            name VARCHAR(100),
            alter_ego VARCHAR(100),
            first_appearance VARCHAR(50)
        );
    """)
    
    # Missions Table
    hook.run("""
        CREATE TABLE IF NOT EXISTS cerebro.dim_missions (
            mission_id VARCHAR(50),
            hero_id VARCHAR(50),
            location VARCHAR(100),
            timestamp TIMESTAMP,
            outcome VARCHAR(50),
            damage INTEGER,
            PRIMARY KEY (mission_id, hero_id)
        );
    """)
    
    # Biometrics Table
    hook.run("""
        CREATE TABLE IF NOT EXISTS cerebro.fact_biometrics (
            timestamp TIMESTAMP,
            hero_id VARCHAR(50),
            heart_rate INTEGER,
            stress_level FLOAT,
            energy_output INTEGER,
            PRIMARY KEY (timestamp, hero_id)
        );
    """)
    
    # Quarantine Table (Generic JSONB structure or specific columns)
    hook.run("""
        CREATE TABLE IF NOT EXISTS cerebro.quarantine_data (
            id SERIAL PRIMARY KEY,
            source_file VARCHAR(50),
            error_reason VARCHAR(255),
            raw_data TEXT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    print("Tables created successfully.")
    print("Tables created successfully.")


def ingest_heroes_data(**context):
    heroes = read_csv_from_minio("heroes.csv", bucket=BUCKET_NAME)

    # Identify invalid rows
    invalid_mask = heroes['hero_id'].isnull()
    
    if invalid_mask.any():
        print(f"Found {invalid_mask.sum()} invalid heroes with null hero_id. Moving to quarantine.")
        postgres = PostgresHook(postgres_conn_id="postgres_default")
        
        # Prepare quarantine DataFrame
        invalid_rows = heroes[invalid_mask].copy()
        quarantine_df = pd.DataFrame({
            'source_file': 'heroes.csv',
            'error_reason': 'Null hero_id',
            'raw_data': invalid_rows.to_json(orient='records', lines=True).splitlines()
        })
        
        # Load to Quarantine Table
        engine = postgres.get_sqlalchemy_engine()
        quarantine_df.to_sql('quarantine_data', engine, if_exists='replace', index=False, schema='cerebro')
        
        # Filter out invalid rows from main flow
        heroes = heroes[~invalid_mask]

    heroes['name'] = heroes['name'].str.strip().str.title()

    heroes = heroes.drop_duplicates(subset=['hero_id'])

    postgres = PostgresHook(postgres_conn_id="postgres_default")
    
    # --- Load dim_heroes ---
    engine = postgres.get_sqlalchemy_engine()
    heroes.to_sql('stg_heroes', engine, if_exists='replace', index=False, schema='cerebro')
    
    postgres.run("""
        INSERT INTO cerebro.dim_heroes (hero_id, name, alter_ego, first_appearance)
        SELECT hero_id, name, alter_ego, first_appearance FROM cerebro.stg_heroes
        ON CONFLICT (hero_id) DO UPDATE SET
            name = EXCLUDED.name,
            alter_ego = EXCLUDED.alter_ego,
            first_appearance = EXCLUDED.first_appearance;
    """)
    print(f"Merged {len(heroes)} heroes.")

    postgres.run("DROP TABLE cerebro.stg_heroes")


def ingest_missions_data(**context):
    missions_data = read_json_from_minio("mission_logs.json", bucket=BUCKET_NAME)
    missions = pd.DataFrame(missions_data)

    if missions['mission_id'].isnull().any():
        raise ValueError("Null mission_id found in mission_logs.json")
    
    # Flatten damage_report dict
    def extract_damage(row):
        dr = row.get('damage_report')
        if isinstance(dr, dict):
            # We only store 'structural_damage' as 'damage' in table schema
            return dr.get('structural_damage')
        return None

    missions['damage'] = missions.apply(extract_damage, axis=1)
    
    # Explode hero_ids to create a row for each hero in the mission
    # 'hero_ids' is a list. explode will duplicate the rest of the row for each item in the list.
    missions = missions.explode('hero_ids')
    missions = missions.rename(columns={'hero_ids': 'hero_id'})
    
    # Parse timestamp to ensure correct SQL type
    missions['timestamp'] = pd.to_datetime(missions['timestamp'])

    # Drop complex columns (damage_report is already flattened, hero_id is now scalar string)
    cols_to_drop = ['damage_report']
    missions = missions.drop(columns=[c for c in cols_to_drop if c in missions.columns])

    # Deduplicate on the new composite key
    missions = missions.drop_duplicates(subset=['mission_id', 'hero_id'])

    postgres = PostgresHook(postgres_conn_id="postgres_default")
    
    # --- Load dim_missions ---
    engine = postgres.get_sqlalchemy_engine()
    missions.to_sql('stg_missions', engine, if_exists='replace', index=False, dtype={'timestamp': sqlalchemy.types.TIMESTAMP(timezone=True)}, schema='cerebro')
    
    postgres.run("""
        INSERT INTO cerebro.dim_missions (mission_id, hero_id, location, timestamp, outcome, damage)
        SELECT mission_id, hero_id, location, timestamp, outcome, damage FROM cerebro.stg_missions
        ON CONFLICT (mission_id, hero_id) DO UPDATE SET
            location = EXCLUDED.location,
            timestamp = EXCLUDED.timestamp,
            outcome = EXCLUDED.outcome,
            damage = EXCLUDED.damage;
    """)
    print(f"Merged {len(missions)} mission records (hero-mission grain).")
    postgres.run("DROP TABLE cerebro.stg_missions")


def ingest_biometrics_data(**context):
    biometrics = read_parquet_from_minio("biometrics.parquet", bucket=BUCKET_NAME)

    if biometrics['hero_id'].isnull().any():
        raise ValueError("Null hero_id found in biometrics.parquet")

    # Ensure timestamp is datetime for resampling
    biometrics['timestamp'] = pd.to_datetime(biometrics['timestamp'])

    biometrics = (
        biometrics
        .set_index("timestamp")
        .groupby("hero_id")
        .resample("1min")
        .mean()
        .reset_index()
    )

    # Deduplicate on composite key, not just hero_id
    biometrics = biometrics.drop_duplicates(subset=['timestamp', 'hero_id'])

    postgres = PostgresHook(postgres_conn_id="postgres_default")
    
    # --- Load dim_biometrics ---
    engine = postgres.get_sqlalchemy_engine()
    biometrics.to_sql('stg_biometrics', engine, if_exists='replace', index=False, schema='cerebro')
    
    postgres.run("""
        INSERT INTO cerebro.fact_biometrics (timestamp, hero_id, heart_rate, stress_level, energy_output)
        SELECT timestamp, hero_id, heart_rate, stress_level, energy_output FROM cerebro.stg_biometrics
        ON CONFLICT (timestamp, hero_id) DO UPDATE SET
            heart_rate = EXCLUDED.heart_rate,
            stress_level = EXCLUDED.stress_level,
            energy_output = EXCLUDED.energy_output;
    """)
    print(f"Merged {len(biometrics)} biometrics.")
    postgres.run("DROP TABLE cerebro.stg_biometrics")


with DAG(
    "cerebro_restore_pipeline",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["cerebro", "restore"],
) as dag:

    create_tables_task = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables
    )

    ingest_heroes_task = PythonOperator(
        task_id="ingest_heroes_data",
        python_callable=ingest_heroes_data
    )

    ingest_missions_task = PythonOperator(
        task_id="ingest_missions_data",
        python_callable=ingest_missions_data
    )

    ingest_biometrics_task = PythonOperator(
        task_id="ingest_biometrics_data",
        python_callable=ingest_biometrics_data
    )


create_tables_task >> ingest_heroes_task >> ingest_missions_task >> ingest_biometrics_task
