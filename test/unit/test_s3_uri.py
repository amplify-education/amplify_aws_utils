"""Module for testing S3 URI parser"""
from unittest import TestCase

from amplify_aws_utils.s3_uri import S3URI


class TestS3URI(TestCase):
    """Class for testing S3 URI parser"""

    def test_happy(self):
        """S3URI happy path"""
        bucket = "MOCK_BUCKET"
        key = "MOCK_KEY"
        uri = "s3://%s/%s" % (bucket, key)

        s3_uri = S3URI(uri=uri)

        self.assertEqual(
            bucket,
            s3_uri.bucket,
        )
        self.assertEqual(
            key,
            s3_uri.key,
        )
        self.assertEqual(
            uri,
            s3_uri.uri,
        )

    def test_with_query(self):
        """S3URI with query component"""
        bucket = "MOCK_BUCKET"
        key = "MOCK_KEY?MOCK_QUERY=BAR"
        uri = "s3://%s/%s" % (bucket, key)

        s3_uri = S3URI(uri=uri)

        self.assertEqual(
            bucket,
            s3_uri.bucket,
        )
        self.assertEqual(
            key,
            s3_uri.key,
        )
        self.assertEqual(
            uri,
            s3_uri.uri,
        )

    def test_with_hash(self):
        """S3URI with hash character"""
        bucket = "MOCK_BUCKET"
        key = "MOCK_KEY#MOCK_HASH"
        uri = "s3://%s/%s" % (bucket, key)

        s3_uri = S3URI(uri=uri)

        self.assertEqual(
            bucket,
            s3_uri.bucket,
        )
        self.assertEqual(
            key,
            s3_uri.key,
        )
        self.assertEqual(
            uri,
            s3_uri.uri,
        )
