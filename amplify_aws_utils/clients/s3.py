"""
Manage interaction with S3 API
"""
import hashlib
import logging
from typing import Dict, Any, Sequence, IO

from botocore.exceptions import ClientError

from mypy_boto3_s3.client import S3Client

from amplify_aws_utils.resource_helper import (
    get_boto3_paged_results,
    throttled_call,
    dict_to_boto3_tags,
    boto3_tags_to_dict,
)

logger = logging.getLogger(__name__)


class S3:
    """
    A simple class for managing interaction with the S3 API
    """

    def __init__(self, s3: S3Client):
        # pylint: disable=invalid-name
        self.s3 = s3

    def list_objects(
        self, bucket: str, prefix: str, **kwargs
    ) -> Sequence[Dict[str, Any]]:
        """
        Convenience function for listing objects in an S3 bucket with paging handled.
        :param bucket: Name of the bucket.
        :param prefix: Prefix of the objects to list.
        :param kwargs: Any additional arguments to pass to the underlying boto call.
        :return: A list of all of the objects.
        """
        results = get_boto3_paged_results(
            self.s3.list_objects_v2,
            results_key="Contents",
            Bucket=bucket,
            Prefix=prefix,
            **kwargs
        )

        return results

    def list_versions(
        self, bucket: str, prefix: str, **kwargs
    ) -> Sequence[Dict[str, Any]]:
        """
        Convenience function for listing all the versions in an S3 bucket with paging handled.
        :param bucket: Name of the bucket.
        :param prefix: Prefix of the objects to list.
        :param kwargs: Any additional arguments to pass to the underlying boto call.
        :return: A list of all of the object versions.
        """
        results = get_boto3_paged_results(
            self.s3.list_object_versions,
            results_key="Versions",
            next_token_key="NextVersionIdMarker",
            next_request_token_key="VersionIdMarker",
            Bucket=bucket,
            Prefix=prefix,
            **kwargs
        )

        return results

    def read_file(self, bucket: str, key: str, wait: bool = False, **kwargs) -> str:
        """
        Convenience function for reading an object out of S3.
        :param bucket: Name of the bucket.
        :param key: Name of the key.
        :param wait: If true, will wait up to 100 seconds for the object to exist before reading it.
        :param kwargs: Any additional arguments to pass to the underlying boto call.
        :return: The contents of the object, read and decoded as utf-8.
        """
        if wait:
            logger.info(
                "Waiting for s3://%s/%s to exist with additional params: %s",
                bucket,
                key,
                kwargs,
            )
            waiter = self.s3.get_waiter("object_exists")
            waiter.wait(Bucket=bucket, Key=key, **kwargs)

        result = (
            throttled_call(self.s3.get_object, Bucket=bucket, Key=key, **kwargs)["Body"]
            .read()
            .decode("utf-8")
        )

        return result

    def download_file(
        self, bucket: str, key: str, file_obj: IO, wait: bool = False, **kwargs
    ):
        """
        Convenience function for downloading an object out of S3.
        :param bucket: Name of the bucket.
        :param key: Name of the key.
        :param file_obj: File-like object to download the file to.
        :param wait: If true, will wait up to 100 seconds for the object to exist before reading it.
        :param kwargs: Any additional arguments to pass to the underlying boto call.
        """
        if wait:
            logger.info(
                "Waiting for s3://%s/%s to exist with additional params: %s",
                bucket,
                key,
                kwargs,
            )
            waiter = self.s3.get_waiter("object_exists")
            waiter.wait(Bucket=bucket, Key=key, **kwargs)

        throttled_call(
            self.s3.download_fileobj,
            Bucket=bucket,
            Key=key,
            Fileobj=file_obj,
            ExtraArgs=kwargs,
        )

    def write_file(self, bucket: str, key: str, body: str, **kwargs):
        """
        Convenience function for writing an object to S3.
        :param bucket: Name of the bucket.
        :param key: Name of the key.
        :param body: The contents of the object.
        :param kwargs: Any additional arguments to pass to the underlying boto call.
        """
        throttled_call(self.s3.put_object, Bucket=bucket, Key=key, Body=body, **kwargs)

    def delete_file(self, bucket: str, key: str, **kwargs):
        """
        Convenience function for deleting an object out of S3.
        :param bucket: Name of the bucket.
        :param key: Name of the key.
        :param kwargs: Any additional arguments to pass to the underlying boto call.
        """
        throttled_call(self.s3.delete_object, Bucket=bucket, Key=key, **kwargs)

    def put_bucket_tags(self, bucket: str, tags: Dict, merge: bool = False):
        """
        Convenience function for tagging a bucket in S3.
        :param bucket: Name of the bucket.
        :param tags: Dict of tags.
        :param merge: True if new tags should be merged with the existing tags on the bucket, otherwise the
        existing tags will be overwritten entirely.
        """
        if merge:
            existing_tags = self.get_bucket_tags(bucket=bucket)
            existing_tags.update(tags)
            tags = existing_tags

        throttled_call(
            self.s3.put_bucket_tagging,
            Bucket=bucket,
            Tagging={"TagSet": dict_to_boto3_tags(tags)},
        )

    def get_bucket_tags(self, bucket: str) -> Dict[str, str]:
        """
        Convenience function for getting tags of a bucket in S3.
        :param bucket: Name of the bucket
        :return: Dictionary representing the tags of the requested S3 bucket.
        """
        try:
            result = throttled_call(self.s3.get_bucket_tagging, Bucket=bucket)

            return boto3_tags_to_dict(result["TagSet"])
        except ClientError:
            logger.warning("Bucket %s has no tags", bucket)
            return {}

    def put_object_tags(self, bucket: str, key: str, tags: Dict, **kwargs):
        """
        Convenience function for adding tags to an object.
        :param bucket: Name of the bucket.
        :param key: Name of the key.
        :param tags: A dictionary of tags to set.
        :param kwargs: Any additional arguments to pass to the underlying boto call.
        """
        throttled_call(
            self.s3.put_object_tagging,
            Bucket=bucket,
            Key=key,
            Tagging={"TagSet": dict_to_boto3_tags(tags)},
            **kwargs
        )

    def get_object_tags(self, bucket: str, key: str, **kwargs):
        """
        Convenience function for getting tags on an object.
        :param bucket: Name of the bucket.
        :param key: Name of the key.
        :param kwargs: Any additional arguments to pass to the underlying boto call.
        :return: A dictionary representing the tags on the object.
        """
        boto_tags = throttled_call(
            self.s3.get_object_tagging, Bucket=bucket, Key=key, **kwargs
        )
        return boto3_tags_to_dict(boto_tags["TagSet"])

    def hash_file(self, bucket: str, key: str, **kwargs) -> str:
        """
        Convenience function for getting the hash of an object from S3
        :param bucket: Name of the bucket.
        :param key: Name of the key.
        :param kwargs: Any additional arguments to pass to the underlying boto call.
        :return: SHA256 hash of the object.
        """
        stream = throttled_call(self.s3.get_object, Bucket=bucket, Key=key, **kwargs)[
            "Body"
        ]

        # 5 megabytes
        block_size = 5242880

        hasher = hashlib.sha256()
        buffer = stream.read(block_size)

        while buffer:
            hasher.update(buffer)
            buffer = stream.read(block_size)

        return hasher.hexdigest()

    def copy_file(
        self,
        source_bucket: str,
        destination_bucket: str,
        source_key: str,
        destination_key: str,
        **kwargs
    ):
        """
        Convenience function for copying an S3 object from one bucket to another.
        :param source_bucket: Name of the source bucket.
        :param destination_bucket: Name of the destination bucket.
        :param source_key: Name of the source object.
        :param destination_key: Name of the destination object.
        :param kwargs: Any additional arguments to pass to the underlying boto call.
        """
        throttled_call(
            self.s3.copy_object,
            Bucket=destination_bucket,
            Key=destination_key,
            CopySource={"Bucket": source_bucket, "Key": source_key},
            **kwargs
        )

    def copy(
        self,
        source_bucket: str,
        destination_bucket: str,
        source_key: str,
        destination_key: str,
        **kwargs
    ):
        """
        Convenience function for copying an S3 object from one bucket to another with multipart uplaod.
        :param source_bucket: Name of the source bucket.
        :param destination_bucket: Name of the destination bucket.
        :param source_key: Name of the source object.
        :param destination_key: Name of the destination object.
        :param kwargs: Any additional arguments to pass to the underlying boto call.
        """
        throttled_call(
            self.s3.copy,
            Bucket=destination_bucket,
            Key=destination_key,
            CopySource={"Bucket": source_bucket, "Key": source_key},
            **kwargs
        )
