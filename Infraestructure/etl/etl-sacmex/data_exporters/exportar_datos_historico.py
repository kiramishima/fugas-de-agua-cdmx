from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
import boto3
from polars import DataFrame
from os import path
from io import BytesIO

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


def write_parquet(client, df, bucket, key):
    parquet_io = BytesIO()
    df.write_parquet(parquet_io)
    return client.upload_fileobj(parquet_io, bucket, key)

@data_exporter
def export_data_to_s3(data, **kwargs) -> None:
    df, fname = data
    print(fname)
    # Conexion a S3
    aws_access_key = kwargs['ACCESS_KEY']
    aws_secret_key = kwargs['SECRET_KEY']
    client = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        endpoint_url="http://storage:9000",
    )
    print(client.list_buckets().get("Buckets"))
    bucket_name = 'cero-fugas'
    write_parquet(client, df, bucket_name, F"{fname}.parquet")
