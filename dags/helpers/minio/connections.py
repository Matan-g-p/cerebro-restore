from airflow.hooks.base import BaseHook
import boto3

def get_minio_client():
    conn = BaseHook.get_connection('aws_default')
    return boto3.client(
        's3',
        endpoint_url=conn.extra_dejson.get('endpoint_url'),
        aws_access_key_id=conn.login,
        aws_secret_access_key=conn.password,
        verify=False
    )

def ensure_bucket_exists():
    """Ensures the target bucket exists."""
    # Hardcoding bucket name here as it's app logic, not conn config
    BUCKET_NAME = "cerebro-data" 
    s3 = get_minio_client()
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
    except:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"Created bucket {BUCKET_NAME}")