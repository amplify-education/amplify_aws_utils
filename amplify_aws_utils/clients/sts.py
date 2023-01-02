"""Utilities for assuming AWS roles"""
from typing import Any, Optional

import boto3
from mypy_boto3_sts.client import STSClient
from mypy_boto3_sts.type_defs import CredentialsTypeDef
from mypy_boto3_sts.literals import ServiceName


class STS:
    """Utilities for assuming AWS roles"""

    def __init__(self, sts_client: STSClient):
        self.sts_client = sts_client

    def assume_role(
        self, account_id: str, role_name: str, role_session_name: Optional[str] = None
    ) -> CredentialsTypeDef:
        """
        Assumes a role and returns its credentials.
        :param account_id: The id of the account to assume the role in.
        :param role_name: The name of the role to assume.
        :param role_session_name: The name of the role session.
        :return: A dictionary of the credentials.
        """
        assumed_role_object = self.sts_client.assume_role(
            RoleArn=f"arn:aws:iam::{account_id}:role/{role_name}",
            RoleSessionName=role_session_name or "AssumedRole",
        )

        return assumed_role_object["Credentials"]

    def get_boto3_client_for_account(
        self,
        account_id: str,
        role_name: str,
        client_name: ServiceName,
        role_session_name: Optional[str] = None,
        **kwargs,
    ) -> Any:
        """
        Convenience function for assuming a role into and then returning a boto3 client with that role.
        :param account_id: The id of the account to assume the role in.
        :param role_name: The name of the role to assume.
        :param client_name: The name of the boto3 client to create.
        :param role_session_name: The name of the role session.
        :param kwargs: Any additional key word arguments to pass to the client's constructor.
        :return: A boto3 client with credentials from the requested role.
        """
        credentials = self.assume_role(
            account_id=account_id,
            role_name=role_name,
            role_session_name=role_session_name,
        )
        return boto3.client(
            client_name,
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            **kwargs,
        )
