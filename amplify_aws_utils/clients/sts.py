"""Utilities for assuming AWS roles"""
from typing import Dict, Any

import boto3
import botostubs


class STS:
    """Utilities for assuming AWS roles"""
    def __init__(self, sts_client: botostubs.STS):
        self.sts_client = sts_client

    def assume_role(self, account_id: str, role_name: str) -> Dict[str, str]:
        """Return credentials of assumed role"""
        assumed_role_object = self.sts_client.assume_role(
            RoleArn='arn:aws:iam::%s:role/%s' % (account_id, role_name),
            RoleSessionName='AssumedRole'
        )

        return assumed_role_object['Credentials']

    def get_boto3_client_for_account(self, account_id: str, role_name: str, client_name: str, **kwargs) \
            -> Any:
        """Return a boto3 client instance using an assumed role for an account id"""
        credentials = self.assume_role(account_id, role_name)
        return boto3.client(
            client_name,
            aws_access_key_id=credentials['AccessKeyId'],
            aws_secret_access_key=credentials['SecretAccessKey'],
            aws_session_token=credentials['SessionToken'],
            **kwargs
        )
