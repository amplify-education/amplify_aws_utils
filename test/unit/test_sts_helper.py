"""Test STS helper"""
from unittest import TestCase

from mock import MagicMock, patch

from amplify_aws_utils.clients.sts import STS


class TestSTSHelper(TestCase):
    """Test STS helper"""
    def setUp(self):
        self.sts_client = MagicMock()
        self.sts_helper = STS(self.sts_client)

    def test_assume_role(self):
        """test getting credentials by assuming a role in a account"""
        self.sts_helper.assume_role("1234", "fake-role")

        self.sts_client.assume_role.assert_called_once_with(
            RoleArn='arn:aws:iam::1234:role/fake-role',
            RoleSessionName='AssumedRole'
        )

    def test_assume_role_with_session_name(self):
        """test getting credentials by assuming a role in a account with a session name"""
        self.sts_helper.assume_role("1234", "fake-role", "fake-session-name")

        self.sts_client.assume_role.assert_called_once_with(
            RoleArn='arn:aws:iam::1234:role/fake-role',
            RoleSessionName='fake-session-name',
        )

    def test_get_boto3_client(self):
        """test getting a boto3 client with an assumed role"""
        self.sts_client.assume_role.return_value = {
            'Credentials': {
                'AccessKeyId': 'foo',
                'SecretAccessKey': 'bar',
                'SessionToken': 'baz'
            }
        }
        with(patch('amplify_aws_utils.clients.sts.boto3.client')) as boto3_mock:
            self.sts_helper.get_boto3_client_for_account("1234", "fake-role", "s3", region_name="us-moon-1")

            self.sts_client.assume_role.assert_called_once_with(
                RoleArn='arn:aws:iam::1234:role/fake-role',
                RoleSessionName='AssumedRole'
            )

            boto3_mock.assert_called_once_with(
                's3',
                aws_access_key_id='foo',
                aws_secret_access_key='bar',
                aws_session_token='baz',
                region_name='us-moon-1'
            )
