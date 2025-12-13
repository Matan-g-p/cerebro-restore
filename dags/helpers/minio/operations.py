from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException
import pandas as pd
import io
import json
import logging

logger = logging.getLogger(__name__)


def read_csv_from_minio(key, bucket="raw-data"):
    """
    Read CSV file from MinIO with retry logic.
    
    Args:
        key: Object key in MinIO
        bucket: Bucket name
        
    Returns:
        pandas.DataFrame: Data from CSV file
        
    Raises:
        AirflowException: If file cannot be read after retries
    """
    try:
        logger.info(f"Reading CSV from MinIO: {bucket}/{key}")
        hook = S3Hook(aws_conn_id="aws_default")
        obj = hook.get_key(key, bucket_name=bucket)
        
        if obj is None:
            raise AirflowException(f"Object not found: {bucket}/{key}")
        
        data = pd.read_csv(io.BytesIO(obj.get()["Body"].read()))
        logger.info(f"Successfully read {len(data)} rows from {bucket}/{key}")
        return data
        
    except Exception as e:
        logger.error(f"Failed to read CSV from MinIO: {bucket}/{key}", exc_info=True)
        raise AirflowException(f"MinIO CSV read failed for {bucket}/{key}: {str(e)}")


def read_json_from_minio(key, bucket="raw-data"):
    """
    Read JSON file from MinIO with retry logic.
    
    Args:
        key: Object key in MinIO
        bucket: Bucket name
        
    Returns:
        dict or list: Parsed JSON data
        
    Raises:
        AirflowException: If file cannot be read after retries
    """
    try:
        logger.info(f"Reading JSON from MinIO: {bucket}/{key}")
        hook = S3Hook(aws_conn_id="aws_default")
        obj = hook.get_key(key, bucket_name=bucket)
        
        if obj is None:
            raise AirflowException(f"Object not found: {bucket}/{key}")
        
        data = json.loads(obj.get()["Body"].read())
        logger.info(f"Successfully read JSON from {bucket}/{key}")
        return data
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in {bucket}/{key}: {str(e)}")
        raise AirflowException(f"Invalid JSON format in {bucket}/{key}: {str(e)}")
    except Exception as e:
        logger.error(f"Failed to read JSON from MinIO: {bucket}/{key}", exc_info=True)
        raise AirflowException(f"MinIO JSON read failed for {bucket}/{key}: {str(e)}")


def read_parquet_from_minio(key, bucket="raw-data"):
    """
    Read Parquet file from MinIO with retry logic.
    
    Args:
        key: Object key in MinIO
        bucket: Bucket name
        
    Returns:
        pandas.DataFrame: Data from Parquet file
        
    Raises:
        AirflowException: If file cannot be read after retries
    """
    try:
        logger.info(f"Reading Parquet from MinIO: {bucket}/{key}")
        hook = S3Hook(aws_conn_id="aws_default")
        obj = hook.get_key(key, bucket_name=bucket)
        
        if obj is None:
            raise AirflowException(f"Object not found: {bucket}/{key}")
        
        data = pd.read_parquet(io.BytesIO(obj.get()["Body"].read()))
        logger.info(f"Successfully read {len(data)} rows from {bucket}/{key}")
        return data
        
    except Exception as e:
        logger.error(f"Failed to read Parquet from MinIO: {bucket}/{key}", exc_info=True)
        raise AirflowException(f"MinIO Parquet read failed for {bucket}/{key}: {str(e)}")