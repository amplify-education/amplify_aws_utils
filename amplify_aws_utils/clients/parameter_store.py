"""Class for Parameter Store"""
import botostubs

from amplify_aws_utils.resource_helper import throttled_call


class ParameterStore:
    """
    A simple class to manage parameter store
    """

    def __init__(self, ssm: botostubs.SSM):
        self.ssm = ssm

    def get_parameter(self, name: str) -> str:
        """
        Gets the paramater
        :param name: name that needs to be retrieved
        :return: retrieved name
        """
        return throttled_call(
            self.ssm.get_parameter,
            Name=name,
            WithDecryption=True
        )['Parameter']['Value']
