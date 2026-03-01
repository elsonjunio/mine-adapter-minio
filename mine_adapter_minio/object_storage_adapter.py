from typing import Optional
from botocore.exceptions import ClientError


from mine_spec.dto.object import (
    ObjectListResult,
    StorageObject,
    ObjectVersion,
    ObjectMetadata,
    BucketInfo,
    BucketUsage,
)


from mine_spec.exceptions.base import (
    AccessDenied,
    BucketAlreadyExists,
    BucketNotFound,
    ConflictError,
    InternalStorageError,
    ObjectNotFound,
)

from mine_spec.ports.object_storage import (
    ObjectStoragePort,
)

import json


def handle_exception(error: ClientError):
    code = error.response['Error']['Code']

    if code in ('NoSuchBucket',):
        raise BucketNotFound()

    if code in ('BucketAlreadyExists', 'BucketAlreadyOwnedByYou'):
        raise BucketAlreadyExists()

    if code in ('NoSuchKey',):
        raise ObjectNotFound()

    if code in ('AccessDenied',):
        raise AccessDenied()

    if code in ('OperationAborted',):
        raise ConflictError()

    raise InternalStorageError(str(error))


class MinioObjectStorageAdapter(ObjectStoragePort):
    def __init__(self, s3_client):
        self.s3 = s3_client

    def setup(self):
        return None

    def list_objects(
        self,
        bucket: str,
        prefix: Optional[str],
        limit: int,
        continuation_token: Optional[str],
    ) -> ObjectListResult:

        try:
            params = {
                'Bucket': bucket,
                'MaxKeys': limit,
            }

            if prefix:
                params['Prefix'] = prefix

            if continuation_token:
                params['ContinuationToken'] = continuation_token

            response = self.s3.list_objects_v2(**params)

            objects = [
                StorageObject(
                    key=obj['Key'],
                    size=obj['Size'],
                    last_modified=obj['LastModified'],
                    etag=obj['ETag'],
                    storage_class=obj.get('StorageClass'),
                )
                for obj in response.get('Contents', [])
            ]

            return ObjectListResult(
                objects=objects,
                is_truncated=response.get('IsTruncated', False),
                next_continuation_token=response.get('NextContinuationToken'),
            )

        except ClientError as e:
            return handle_exception(e)

    def delete_object(self, bucket: str, key: str) -> None:
        try:
            self.s3.delete_object(Bucket=bucket, Key=key)

        except ClientError as e:
            return handle_exception(e)

    def copy_object(
        self,
        source_bucket: str,
        source_key: str,
        dest_bucket: str,
        dest_key: str,
    ) -> None:
        try:
            self.s3.copy_object(
                CopySource={
                    'Bucket': source_bucket,
                    'Key': source_key,
                },
                Bucket=dest_bucket,
                Key=dest_key,
            )

        except ClientError as e:
            return handle_exception(e)

    def generate_upload_url(
        self,
        bucket: str,
        key: str,
        expires_in: int,
        content_type: str | None = None,
    ) -> str:

        try:
            params = {
                'Bucket': bucket,
                'Key': key,
            }

            if content_type:
                params['ContentType'] = content_type

            return self.s3.generate_presigned_url(
                'put_object',
                Params=params,
                ExpiresIn=expires_in,
            )

        except ClientError as e:
            return handle_exception(e)

    def generate_download_url(
        self,
        bucket: str,
        key: str,
        expires_in: int,
        response_content_type: str | None = None,
        response_content_disposition: str | None = None,
    ) -> str:

        try:
            params = {
                'Bucket': bucket,
                'Key': key,
            }

            if response_content_type:
                params['ResponseContentType'] = response_content_type

            if response_content_disposition:
                params[
                    'ResponseContentDisposition'
                ] = response_content_disposition

            return self.s3.generate_presigned_url(
                'get_object',
                Params=params,
                ExpiresIn=expires_in,
            )

        except ClientError as e:
            return handle_exception(e)

    def list_object_versions(
        self,
        bucket: str,
        key: str,
    ) -> list[ObjectVersion]:

        try:
            response = self.s3.list_object_versions(
                Bucket=bucket,
                Prefix=key,
            )

            versions: list[ObjectVersion] = []

            for v in response.get('Versions', []):
                if v['Key'] == key:
                    versions.append(
                        ObjectVersion(
                            version_id=v['VersionId'],
                            is_latest=v['IsLatest'],
                            last_modified=v['LastModified'],
                            size=v['Size'],
                        )
                    )

            return versions

        except ClientError as e:
            return handle_exception(e)

    def delete_object_version(
        self,
        bucket: str,
        key: str,
        version_id: str,
    ) -> None:

        try:
            self.s3.delete_object(
                Bucket=bucket,
                Key=key,
                VersionId=version_id,
            )

        except ClientError as e:
            return handle_exception(e)

    def restore_object_version(
        self,
        bucket: str,
        key: str,
        version_id: str,
    ) -> None:

        try:
            copy_source = {
                'Bucket': bucket,
                'Key': key,
                'VersionId': version_id,
            }

            self.s3.copy_object(
                Bucket=bucket,
                Key=key,
                CopySource=copy_source,
            )

        except ClientError as e:
            return handle_exception(e)

    def get_object_metadata(
        self,
        bucket: str,
        key: str,
    ) -> ObjectMetadata:

        try:
            response = self.s3.head_object(
                Bucket=bucket,
                Key=key,
            )

            return ObjectMetadata(
                size=response['ContentLength'],
                etag=response['ETag'],
                last_modified=response['LastModified'],
                content_type=response.get('ContentType'),
                metadata=response.get('Metadata', {}),
            )

        except ClientError as e:
            return handle_exception(e)

    def update_object_metadata(
        self,
        bucket: str,
        key: str,
        metadata: dict,
    ) -> None:

        try:
            # Primeiro pega metadata atual para manter ContentType
            head = self.s3.head_object(
                Bucket=bucket,
                Key=key,
            )

            content_type = head.get('ContentType')

            copy_source = {
                'Bucket': bucket,
                'Key': key,
            }

            self.s3.copy_object(
                Bucket=bucket,
                Key=key,
                CopySource=copy_source,
                Metadata=metadata,
                MetadataDirective='REPLACE',
                ContentType=content_type,
            )

        except ClientError as e:
            return handle_exception(e)

    def get_object_tags(
        self,
        bucket: str,
        key: str,
    ) -> dict[str, str]:

        try:
            response = self.s3.get_object_tagging(
                Bucket=bucket,
                Key=key,
            )

            tagset = response.get('TagSet', [])

            return {tag['Key']: tag['Value'] for tag in tagset}

        except ClientError as e:
            return handle_exception(e)

    def update_object_tags(
        self,
        bucket: str,
        key: str,
        tags: dict[str, str],
    ) -> None:

        try:
            tagset = [{'Key': k, 'Value': v} for k, v in tags.items()]

            self.s3.put_object_tagging(
                Bucket=bucket,
                Key=key,
                Tagging={'TagSet': tagset},
            )

        except ClientError as e:
            return handle_exception(e)

    def list_buckets(self) -> list[BucketInfo]:
        try:
            response = self.s3.list_buckets()

            return [
                BucketInfo(
                    name=bucket['Name'],
                    creation_date=bucket['CreationDate'],
                )
                for bucket in response.get('Buckets', [])
            ]

        except ClientError as e:
            return handle_exception(e)

    def create_bucket(self, name: str) -> None:
        try:
            self.s3.create_bucket(Bucket=name)
        except ClientError as e:
            return handle_exception(e)

    def delete_bucket(self, name: str) -> None:
        try:
            self.s3.delete_bucket(Bucket=name)
        except ClientError as e:
            return handle_exception(e)

    def set_bucket_versioning(
        self,
        name: str,
        enabled: bool,
    ) -> None:

        status_value = 'Enabled' if enabled else 'Suspended'

        try:
            self.s3.put_bucket_versioning(
                Bucket=name,
                VersioningConfiguration={'Status': status_value},
            )

        except ClientError as e:
            return handle_exception(e)

    def get_bucket_versioning_status(self, name: str) -> str | None:
        try:
            response = self.s3.get_bucket_versioning(Bucket=name)
            return response.get("Status")  # Enabled | Suspended | None
        except ClientError as e:
            return handle_exception(e)

    def get_bucket_usage(self, name: str) -> BucketUsage:

        try:
            total_size = 0
            total_objects = 0

            paginator = self.s3.get_paginator('list_objects_v2')

            for page in paginator.paginate(Bucket=name):
                for obj in page.get('Contents', []):
                    total_size += obj['Size']
                    total_objects += 1

            return BucketUsage(
                objects=total_objects,
                size_bytes=total_size,
            )

        except ClientError as e:
            return handle_exception(e)

    def get_bucket_policy(
        self,
        bucket: str,
    ) -> dict | None:

        try:
            response = self.s3.get_bucket_policy(Bucket=bucket)

            return json.loads(response['Policy'])

        except ClientError as e:
            error_code = e.response['Error']['Code']

            if error_code == 'NoSuchBucketPolicy':
                return None

            return handle_exception(e)

    def put_bucket_policy(
        self,
        bucket: str,
        policy: dict,
    ) -> None:

        try:
            self.s3.put_bucket_policy(
                Bucket=bucket,
                Policy=json.dumps(policy),
            )

        except ClientError as e:
            return handle_exception(e)

    def delete_bucket_policy(
        self,
        bucket: str,
    ) -> None:

        try:
            self.s3.delete_bucket_policy(Bucket=bucket)

        except ClientError as e:
            return handle_exception(e)

    def get_bucket_lifecycle(
        self,
        bucket: str,
    ) -> dict | None:

        try:
            response = self.s3.get_bucket_lifecycle_configuration(
                Bucket=bucket
            )

            return response

        except ClientError as e:
            error_code = e.response['Error']['Code']

            if error_code == 'NoSuchLifecycleConfiguration':
                return None

            return handle_exception(e)

    def put_bucket_lifecycle(
        self,
        bucket: str,
        lifecycle: dict,
    ) -> None:

        try:
            self.s3.put_bucket_lifecycle_configuration(
                Bucket=bucket,
                LifecycleConfiguration=lifecycle,
            )

        except ClientError as e:
            return handle_exception(e)

    def delete_bucket_lifecycle(
        self,
        bucket: str,
    ) -> None:

        try:
            self.s3.delete_bucket_lifecycle(Bucket=bucket)

        except ClientError as e:
            return handle_exception(e)

    def get_bucket_events(
        self,
        bucket: str,
    ) -> dict:

        try:
            response = self.s3.get_bucket_notification_configuration(
                Bucket=bucket
            )

            return response

        except ClientError as e:
            return handle_exception(e)

    def put_bucket_events(
        self,
        bucket: str,
        config: dict,
    ) -> None:

        try:
            self.s3.put_bucket_notification_configuration(
                Bucket=bucket,
                NotificationConfiguration=config,
            )

        except ClientError as e:
            return handle_exception(e)

    def delete_bucket_events(
        self,
        bucket: str,
    ) -> None:

        try:
            # No S3 remover = enviar config vazia
            self.s3.put_bucket_notification_configuration(
                Bucket=bucket,
                NotificationConfiguration={},
            )

        except ClientError as e:
            return handle_exception(e)
