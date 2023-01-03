"""Class for testing the Config client wrapper"""
from unittest import TestCase

from mock import MagicMock, call

from amplify_aws_utils.clients.config import Config


MOCK_RESULT_TOKEN = "MOCK_RESULT_TOKEN"


class TestAwsConfigClient(TestCase):
    """Class for testing the Config client wrapper"""

    def setUp(self):
        """Setup for tests"""
        self.boto_client = MagicMock()

        self.config = Config(
            config_client=self.boto_client,
        )

    def test_put_evaluations(self):
        """Tests Config client can send evaluations"""
        evaluations = [
            {
                "ComplianceResourceId": str(i),
                "ComplianceType": str(i),
            }
            for i in range(0, 101)
        ]

        self.config.put_evaluations(
            result_token=MOCK_RESULT_TOKEN, evaluations=evaluations
        )

        self.boto_client.put_evaluations.assert_has_calls(
            calls=(
                call(ResultToken=MOCK_RESULT_TOKEN, Evaluations=evaluations[:100]),
                call(ResultToken=MOCK_RESULT_TOKEN, Evaluations=evaluations[100:]),
            ),
            any_order=False,
        )
