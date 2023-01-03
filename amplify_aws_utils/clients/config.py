"""Class for wrapping common Config API calls"""
import logging
from typing import List, Dict

from mypy_boto3_config.client import ConfigServiceClient

from amplify_aws_utils.resource_helper import throttled_call, chunker


logger = logging.getLogger(__name__)


class Config:
    """Class for wrapping common Config API calls"""

    def __init__(self, config_client: ConfigServiceClient):
        self.config_client = config_client

    def put_evaluations(
        self, result_token: str, evaluations: List[Dict[str, str]]
    ) -> None:
        """
        Convenience function for submitting evaluations to the AWS Config service in chunks of 100.
        :param result_token: The result token that yielded these evaluations.
        :param evaluations: A list of dictionaries representing the evaluations.
        """
        for evaluation_group in chunker(evaluations, 100):
            logger.info(
                "Submitting evaluations for: %s",
                [
                    (evaluation["ComplianceResourceId"], evaluation["ComplianceType"])
                    for evaluation in evaluation_group
                ],
            )
            throttled_call(
                self.config_client.put_evaluations,
                ResultToken=result_token,
                Evaluations=evaluation_group,
            )
