from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import io
import json

def read_csv_from_minio(key, bucket="raw-data"):
    hook = S3Hook(aws_conn_id="aws_default")
    obj = hook.get_key(key, bucket_name=bucket)
    return pd.read_csv(io.BytesIO(obj.get()["Body"].read()))


def read_json_from_minio(key, bucket="raw-data"):
    hook = S3Hook(aws_conn_id="aws_default")
    obj = hook.get_key(key, bucket_name=bucket)
    return json.loads(obj.get()["Body"].read())


def read_parquet_from_minio(key, bucket="raw-data"):
    hook = S3Hook(aws_conn_id="aws_default")
    obj = hook.get_key(key, bucket_name=bucket)
    return pd.read_parquet(io.BytesIO(obj.get()["Body"].read()))