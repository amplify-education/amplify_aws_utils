"""Contains test for S3 Helper"""
# pylint: disable=deprecated-module
import hashlib
import random
import string
from io import BytesIO
from typing import Dict, Set
from unittest import TestCase
from unittest.mock import MagicMock
from urllib.parse import urlencode

import boto3
import botostubs
import requests
from moto import mock_s3

from amplify_aws_utils.clients.s3 import S3
from amplify_aws_utils.resource_helper import boto3_tags_to_dict, dict_to_boto3_tags

TEST_BUCKET_NAME = "test-bucket-name"
TEST_OBJECT_PREFIX = "".join(random.choices(string.ascii_uppercase + string.digits, k=20))
TEST_OBJECT_BODY = "".join(random.choices(string.ascii_uppercase + string.digits, k=4000))
TEST_OBJECT_KEY_DUPLICATES = f"{TEST_OBJECT_PREFIX}/multiple_versions"
TEST_OBJECT_KEY_NO_DUPLICATES = f"{TEST_OBJECT_PREFIX}/single_version"
TEST_OBJECT_KEYS: Set[str] = set()
TEST_OBJECT_TAGS: Dict[str, str] = {"foo": "bar", "cat": "dog"}
TEST_BUCKET_TAGS: Dict[str, str] = {}


@mock_s3
class TestS3Helper(TestCase):
    """Class for testing S3 Helper"""

    def setUp(self):
        client: botostubs.S3 = boto3.client('s3')
        self.setup_environment(client)
        self.helper = S3(client)

    def tearDown(self):
        requests.post("http://motoapi.amazonaws.com/moto-api/reset")
        TEST_OBJECT_KEYS.clear()
        TEST_BUCKET_TAGS.clear()

    def setup_environment(self, client):
        """Convenience function for setting up the S3 environment"""
        TEST_OBJECT_KEYS.add(TEST_OBJECT_KEY_DUPLICATES)
        TEST_OBJECT_KEYS.add(TEST_OBJECT_KEY_NO_DUPLICATES)
        TEST_BUCKET_TAGS["foo"] = "bar"

        client.create_bucket(
            Bucket=TEST_BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": "us-moon-1"},
        )

        client.put_bucket_versioning(
            Bucket=TEST_BUCKET_NAME,
            VersioningConfiguration={
                'Status': 'Enabled'
            }

        )

        # pylint: disable=unused-variable
        for i in range(10):
            identifier = "".join(random.choices(string.ascii_uppercase + string.digits, k=10))
            key = f"{TEST_OBJECT_PREFIX}/{identifier}"
            client.put_object(
                Bucket=TEST_BUCKET_NAME,
                Key=key,
                Body=TEST_OBJECT_BODY
            )
            TEST_OBJECT_KEYS.add(key)

        # pylint: disable=unused-variable
        for i in range(10):
            client.put_object(
                Bucket=TEST_BUCKET_NAME,
                Key=TEST_OBJECT_KEY_DUPLICATES,
                Body=TEST_OBJECT_BODY
            )

        client.put_object(
            Bucket=TEST_BUCKET_NAME,
            Key=TEST_OBJECT_KEY_NO_DUPLICATES,
            Body=TEST_OBJECT_BODY,
            Tagging=urlencode(TEST_OBJECT_TAGS)
        )

        client.put_bucket_tagging(
            Bucket=TEST_BUCKET_NAME,
            Tagging={
                'TagSet': dict_to_boto3_tags(TEST_BUCKET_TAGS)
            }
        )

    def test_list_objects(self):
        """Test that we can list objects"""
        items = self.helper.list_objects(
            bucket=TEST_BUCKET_NAME,
            prefix=TEST_OBJECT_PREFIX
        )

        self.assertEqual(TEST_OBJECT_KEYS, {item["Key"] for item in items})

    def test_list_versions(self):
        """Test that we can list versions of objects"""
        versions = self.helper.list_versions(
            bucket=TEST_BUCKET_NAME,
            prefix=TEST_OBJECT_PREFIX
        )

        self.assertEqual(TEST_OBJECT_KEYS, {item["Key"] for item in versions})
        self.assertEqual(len(TEST_OBJECT_KEYS) + 9, len(versions))

    def test_list_versions_no_duplicates(self):
        """Test that we can list versions of an object with only one version"""
        versions = self.helper.list_versions(
            bucket=TEST_BUCKET_NAME,
            prefix=TEST_OBJECT_KEY_NO_DUPLICATES
        )

        self.assertEqual({TEST_OBJECT_KEY_NO_DUPLICATES}, {item["Key"] for item in versions})
        self.assertEqual(1, len(versions))

    def test_list_versions_with_duplicates(self):
        """Test that we can list versions of an object with multiple versions"""
        versions = self.helper.list_versions(
            bucket=TEST_BUCKET_NAME,
            prefix=TEST_OBJECT_KEY_DUPLICATES
        )

        self.assertEqual({TEST_OBJECT_KEY_DUPLICATES}, {item["Key"] for item in versions})
        self.assertEqual(10, len(versions))

    def test_read_file(self):
        """Test that we can read a file"""
        key = random.choice(tuple(TEST_OBJECT_KEYS))

        contents = self.helper.read_file(
            bucket=TEST_BUCKET_NAME,
            key=key
        )

        self.assertEqual(TEST_OBJECT_BODY, contents)

    def test_download_file(self):
        """Test that we can download a file"""
        mock_s3_client = MagicMock()
        self.helper.s3 = mock_s3_client
        key = random.choice(tuple(TEST_OBJECT_KEYS))
        file_obj = BytesIO()

        self.helper.download_file(
            bucket=TEST_BUCKET_NAME,
            key=key,
            file_obj=file_obj,
        )

        mock_s3_client.download_fileobj.assert_called_once_with(
            Bucket=TEST_BUCKET_NAME,
            Key=key,
            Fileobj=file_obj,
            ExtraArgs={},
        )

    def test_write_file(self):
        """Test that we can write a file"""
        key = "".join(random.choices(string.ascii_uppercase + string.digits, k=20))
        body = "".join(random.choices(string.ascii_uppercase + string.digits, k=80))

        self.helper.write_file(
            bucket=TEST_BUCKET_NAME,
            key=key,
            body=body
        )

        contents = self.helper.read_file(
            bucket=TEST_BUCKET_NAME,
            key=key
        )

        self.assertEqual(body, contents)

    def test_tag_bucket(self):
        """Test that we can tag a bucket"""
        artifact = "".join(random.choices(string.ascii_uppercase + string.digits, k=10))
        execution = "".join(random.choices(string.ascii_uppercase + string.digits, k=10))
        tags = {
            'artifactId': artifact,
            'executionId': execution
        }

        self.helper.put_bucket_tags(
            bucket=TEST_BUCKET_NAME,
            tags=tags
        )

        client = boto3.client("s3")
        response = client.get_bucket_tagging(
            Bucket=TEST_BUCKET_NAME
        )
        test_tags = boto3_tags_to_dict(response['TagSet'])

        self.assertEqual(tags, test_tags)

    def test_tag_bucket_merge(self):
        """Test that we can tag a bucket and merge with existing tags"""
        artifact = "fake-arti"
        execution = "fake-exec"
        tags = {
            'artifactId': artifact,
            'executionId': execution
        }

        self.helper.put_bucket_tags(
            bucket=TEST_BUCKET_NAME,
            tags=tags,
            merge=True
        )

        client = boto3.client("s3")
        response = client.get_bucket_tagging(
            Bucket=TEST_BUCKET_NAME
        )
        test_tags = boto3_tags_to_dict(response['TagSet'])
        tags.update(TEST_BUCKET_TAGS)

        self.assertEqual(tags, test_tags)

    def test_hash_file(self):
        """Test that we can hash a file"""
        expected_hash_value = hashlib.sha256(TEST_OBJECT_BODY.encode("utf-8")).hexdigest()

        actual_hash_value = self.helper.hash_file(
            bucket=TEST_BUCKET_NAME,
            key=random.choice(tuple(TEST_OBJECT_KEYS))
        )

        self.assertEqual(expected_hash_value, actual_hash_value)

    def test_copy_file(self):
        """Test that we can copy a file"""
        source_key = "COPY_SOURCE"
        destination_key = "COPY_DESTINATION"
        expected_body = "COPY_TEST_BODY"

        self.helper.write_file(
            bucket=TEST_BUCKET_NAME,
            key=source_key,
            body=expected_body
        )

        self.helper.copy_file(
            source_bucket=TEST_BUCKET_NAME,
            destination_bucket=TEST_BUCKET_NAME,
            source_key=source_key,
            destination_key=destination_key
        )

        actual_body = self.helper.read_file(
            bucket=TEST_BUCKET_NAME,
            key=destination_key
        )

        self.assertEqual(expected_body, actual_body)

    def test_get_object_tags(self):
        """Test that we can get an object's tags"""
        actual_tags = self.helper.get_object_tags(
            bucket=TEST_BUCKET_NAME,
            key=TEST_OBJECT_KEY_NO_DUPLICATES
        )

        self.assertEqual(TEST_OBJECT_TAGS, actual_tags)

    def test_put_object_tags(self):
        """Test that we can set an object's tags"""
        before_tags = self.helper.get_object_tags(
            bucket=TEST_BUCKET_NAME,
            key=TEST_OBJECT_KEY_DUPLICATES,
        )

        self.assertEqual({}, before_tags)

        self.helper.put_object_tags(
            bucket=TEST_BUCKET_NAME,
            key=TEST_OBJECT_KEY_DUPLICATES,
            tags=TEST_OBJECT_TAGS,
        )

        after_tags = self.helper.get_object_tags(
            bucket=TEST_BUCKET_NAME,
            key=TEST_OBJECT_KEY_DUPLICATES,
        )

        self.assertEqual(TEST_OBJECT_TAGS, after_tags)

    def test_get_bucket_tags(self):
        """Test that we can get a bucket's tags"""
        actual_tags = self.helper.get_bucket_tags(
            bucket=TEST_BUCKET_NAME
        )

        self.assertEqual(TEST_BUCKET_TAGS, actual_tags)
