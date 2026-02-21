from mine_adapter_minio.admin_adapter import MinioAdminAdapter
import boto3
from botocore.client import Config

from mine_adapter_minio.object_storage_adapter import MinioObjectStorageAdapter


def get_admin_client():
    import os

    minio_mc_alias = os.getenv('MINIO_MC_ALIAS')
    minio_endpoint = os.getenv('S3_ENDPOINT')
    minio_access_key = os.getenv('S3_ACCESS_KEY')
    minio_secret_key = os.getenv('S3_SECRET_KEY')
    minio_secure = (
        True if os.getenv('S3_SECURE', 'false').lower() == 'true' else False
    )

    return MinioAdminAdapter(
        minio_mc_alias,
        minio_endpoint,
        minio_access_key,
        minio_secret_key,
        minio_secure,
    )


def get_s3_client(sts_credentials: dict):
    import os

    minio_endpoint = os.getenv('S3_ENDPOINT')
    minio_region = os.getenv('S3_REGION')
    minio_secure = (
        True if os.getenv('S3_SECURE', 'false').lower() == 'true' else False
    )

    url = f"http{'s' if minio_secure else ''}://{minio_endpoint}"

    s3_client = boto3.client(
        's3',
        endpoint_url=url,
        region_name=minio_region,
        aws_access_key_id=sts_credentials['aws_access_key_id'],
        aws_secret_access_key=sts_credentials['aws_secret_access_key'],
        aws_session_token=sts_credentials.get('aws_session_token'),
        config=Config(signature_version='s3v4'),
    )

    return MinioObjectStorageAdapter(s3_client=s3_client)
