from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import pandas as pd
import numpy as np
import json
import os
import logging
from helpers.minio.connections import get_minio_client, ensure_bucket_exists
from config.settings import Config
import io

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


def generate_and_upload_heroes(**kwargs):
    """Generates heroes data and uploads to MinIO."""
    logger.info("Starting heroes data generation")
    
    try:
        unique_heroes = [
            ("C-1", "Captain America", "Steve Rogers", "1941-03-01"),
            ("I-2", "Iron Man", "Tony Stark", "1963-03-01"),
            ("T-3", "Thor", "Donald Blake", "1962-08-01"),
            ("H-4", "The Hulk", "Bruce Banner", "1962-05-01"),
            ("S-5", "Spider-Man", "Peter Parker", "1962-08-01"),
            ("W-6", "Black Widow", "Natasha Romanoff", "1964-04-01"),
            ("D-7", "Doctor Strange", "Stephen Strange", "1963-07-01"),
            ("V-8", "Vision", "Vision", "1968-10-01"),
            ("P-9", "Black Panther", "T'Challa", "1966-07-01"),
            ("A-10", "Ant-Man", "Scott Lang", "1979-03-01")
        ]
        
        data = []
        for hero_id, name, alter_ego, appearance in unique_heroes:
            data.append({'hero_id': hero_id, 'name': name, 'alter_ego': alter_ego, 'first_appearance': appearance})

        df = pd.DataFrame(data)
        logger.info(f"Generated {len(df)} unique heroes")

        # Duplicates and Messy Data
        df = pd.concat([df, df[df['hero_id'] == 'I-2'], df[df['hero_id'] == 'S-5'].copy()], ignore_index=True)
        df.loc[df['hero_id'] == 'S-5', 'name'] = df.loc[df['hero_id'] == 'S-5', 'name'].apply(
            lambda x: "spiderman" if np.random.rand() < 0.5 else "SPIDER-MAN" if np.random.rand() < 0.5 else x
        )
        df.loc[df['hero_id'] == 'H-4', 'name'] = df.loc[df['hero_id'] == 'H-4', 'name'].apply(
            lambda x: "the hulk" if np.random.rand() < 0.5 else "HULK"
        )
        df.loc[df.index[-1], 'hero_id'] = None
        df.loc[df.index[-2], 'name'] = None
        
        logger.info(f"Added data quality issues, total rows: {len(df)}")
        
        # Upload to MinIO
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        
        s3 = get_minio_client()
        s3.put_object(Bucket=Config.BUCKET_NAME, Key='heroes.csv', Body=csv_buffer.getvalue())
        logger.info(f"Uploaded heroes.csv to MinIO bucket {Config.BUCKET_NAME}")
        
        # Return valid hero IDs for next tasks
        valid_ids = df['hero_id'].dropna().unique().tolist()
        logger.info(f"Returning {len(valid_ids)} valid hero IDs")
        return valid_ids
        
    except Exception as e:
        logger.error(f"Failed to generate heroes data: {str(e)}", exc_info=True)
        raise

def generate_and_upload_missions(**kwargs):
    """Generates mission logs and uploads to MinIO."""
    ti = kwargs['ti']
    hero_ids = ti.xcom_pull(task_ids='generate_heroes')
    logger.info(f"Starting missions generation with {len(hero_ids)} hero IDs")
    
    try:
        logs = []
        start_time = datetime(2025, 1, 1, tzinfo=timezone.utc)
        
        for i in range(1, Config.NUM_MISSIONS + 1):
            mission_id = f"M-{i:03d}"
            num_heroes = np.random.randint(1, 5)
            participating_hero_ids = np.random.choice(hero_ids, num_heroes, replace=False).tolist()
            tz = timezone.utc if np.random.rand() < 0.7 else timezone(timedelta(hours=-5))
            
            log = {
                "mission_id": mission_id,
                "hero_ids": participating_hero_ids,
                "location": np.random.choice(["New York", "Wakanda", "Sokovia", "London", "Sanctuary"]),
                "timestamp": (start_time + timedelta(hours=i)).astimezone(tz).isoformat(),
                "outcome": np.random.choice(["Successful", "Failure", "Draw"]),
                "damage_report": {
                    "structural_damage": int(np.random.normal(loc=8000, scale=5000)),
                    "civilian_injuries": np.random.randint(0, 20)
                }
            }
            if np.random.rand() < 0.1: log.pop("damage_report")
            if np.random.rand() < 0.05: log["location"] = None
            logs.append(log)
        
        logger.info(f"Generated {len(logs)} mission logs")
        
        s3 = get_minio_client()
        s3.put_object(Bucket=Config.BUCKET_NAME, Key='mission_logs.json', Body=json.dumps(logs, indent=4))
        logger.info(f"Uploaded mission_logs.json to MinIO bucket {Config.BUCKET_NAME}")
        
    except Exception as e:
        logger.error(f"Failed to generate missions data: {str(e)}", exc_info=True)
        raise

def generate_and_upload_biometrics(**kwargs):
    """Generates biometrics and uploads to MinIO."""
    ti = kwargs['ti']
    hero_ids = ti.xcom_pull(task_ids='generate_heroes')
    logger.info(f"Starting biometrics generation with {len(hero_ids)} hero IDs")
    
    try:
        timestamps = pd.to_datetime(pd.date_range(
            start=datetime(2025, 1, 1, 0, 0, 0),
            periods=Config.NUM_BIOMETRIC_RECORDS,
            freq='1s',
            tz=timezone.utc
        ))
        
        df = pd.DataFrame({
            'timestamp': timestamps,
            'hero_id': np.random.choice(hero_ids, Config.NUM_BIOMETRIC_RECORDS, replace=True),
            'heart_rate': np.random.normal(loc=85, scale=15, size=Config.NUM_BIOMETRIC_RECORDS).clip(50, 150).astype(int),
            'stress_level': np.random.uniform(0, 10, size=Config.NUM_BIOMETRIC_RECORDS), 
            'energy_output': np.random.exponential(scale=500, size=Config.NUM_BIOMETRIC_RECORDS).clip(0, 5000).astype(int)
        })
        df.loc[df.sample(frac=0.01).index, 'heart_rate'] = None
        
        logger.info(f"Generated {len(df)} biometric records")
        
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        
        s3 = get_minio_client()
        s3.put_object(Bucket=Config.BUCKET_NAME, Key='biometrics.parquet', Body=parquet_buffer.getvalue())
        logger.info(f"Uploaded biometrics.parquet to MinIO bucket {Config.BUCKET_NAME}")
        
    except Exception as e:
        logger.error(f"Failed to generate biometrics data: {str(e)}", exc_info=True)
        raise

with DAG(
    'generate_and_ingest_data',
    default_args=default_args,
    description='Generates mock data and uploads to MinIO',
    schedule_interval=None, # Manually triggered
    catchup=False,
) as dag:

    t0 = PythonOperator(
        task_id='ensure_bucket_exists',
        python_callable=ensure_bucket_exists,
    )

    t1 = PythonOperator(
        task_id='generate_heroes',
        python_callable=generate_and_upload_heroes,
    )

    t2 = PythonOperator(
        task_id='generate_missions',
        python_callable=generate_and_upload_missions,
    )

    t3 = PythonOperator(
        task_id='generate_biometrics',
        python_callable=generate_and_upload_biometrics,
    )

    t0 >> t1 >> [t2, t3]
