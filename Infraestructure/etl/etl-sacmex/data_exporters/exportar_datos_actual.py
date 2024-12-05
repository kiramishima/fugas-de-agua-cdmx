from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
import boto3
from polars import DataFrame
from os import path
from io import BytesIO
#import s3fs
from pyarrow import Table, parquet as pq
from pyarrow import fs, csv, parquet

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


def write_parquet(client, df, bucket, key):
    parquet_io = BytesIO()
    df.write_parquet(parquet_io, compression='gzip')
    #out_buffer = BytesIO()
    #        input_datafame.to_parquet(out_buffer, index=False)
    #return #client.upload_fileobj(parquet_io, bucket, key)
    return client.put_object(Bucket=bucket, Key=key, Body=parquet_io.getvalue())

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

   #pyarrow.dataset.write_dataset(df.to_arrow(),
    #                          'data',
    #                          format='parquet')
    s3_path = f"s3://{bucket_name}/test.parquet"
    #df.write_parquet("s3://cero-fugas/test.parquet", storage_options=storage_options)
    #with fs.open(s3_path, mode="wb") as f:
    #    df.write_parquet(f)
    return s3_path
