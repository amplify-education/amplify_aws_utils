"""Class for testing the EC2 client wrapper"""
from datetime import datetime, timedelta
from unittest import TestCase

from mock import MagicMock

from amplify_aws_utils.clients.ec2 import EC2, DATETIME_FORMAT

MOCK_INITIAL_AMIS = {"ami-foo", "ami-bar"}
MOCK_DERIVED_AMIS = {"ami-foo2", "ami-bar2"}
MOCK_BAD_INSTANCES = {"i-foo", "i-bar"}
MOCK_GOOD_INSTANCES = {"i-foo2", "i-bar2"}
MOCK_ENVIRONMENT = "MOCK_ENVIRONMENT"
MOCK_INSTANCE_STATE = "MOCK_INSTANCE_STATE"


class TestAwsEc2Client(TestCase):
    """Class for testing the EC2 client wrapper"""

    def setUp(self):
        """Setup for tests"""
        self.boto_client = MagicMock()

        self.ec2 = EC2(
            ec2_client=self.boto_client,
        )

    def test_find_amis(self):
        """Tests EC2 client can find AMIs"""
        self.boto_client.describe_images.return_value = {
            "Images": [{"ImageId": image_id} for image_id in MOCK_DERIVED_AMIS]
        }

        actual_amis = self.ec2.find_amis()

        self.boto_client.describe_images.assert_called_once_with(
            Filters=[], Owners=["self"]
        )

        self.assertEqual(MOCK_DERIVED_AMIS, actual_amis)

    def test_find_amis_by_age(self):
        """Tests EC2 client can find AMIs relative to a datetime"""
        self.boto_client.describe_images.return_value = {
            "Images": [
                {
                    "ImageId": image_id,
                    "CreationDate": (datetime.now() + timedelta(days=1)).strftime(
                        DATETIME_FORMAT
                    ),
                }
                for image_id in MOCK_DERIVED_AMIS
            ]
        }

        actual_amis = self.ec2.find_amis(newer_than=datetime.now())

        self.boto_client.describe_images.assert_called_once_with(
            Filters=[], Owners=["self"]
        )

        self.assertEqual(MOCK_DERIVED_AMIS, actual_amis)

    def test_find_amis_by_source_ami(self):
        """Tests EC2 client can find descendant AMIs"""
        self.boto_client.describe_images.return_value = {
            "Images": [{"ImageId": image_id} for image_id in MOCK_DERIVED_AMIS]
        }

        actual_amis = self.ec2.find_amis(source_amis=MOCK_INITIAL_AMIS)

        self.boto_client.describe_images.assert_called_once_with(
            Filters=[{"Name": "tag:source_ami", "Values": list(MOCK_INITIAL_AMIS)}],
            Owners=["self"],
        )

        self.assertEqual(MOCK_DERIVED_AMIS, actual_amis)

    def test_find_instances(self):
        """Tests EC2 client can find instances"""
        self.boto_client.describe_instances.return_value = {
            "Reservations": [
                {
                    "Instances": [
                        {"InstanceId": instance_id}
                        for instance_id in MOCK_BAD_INSTANCES
                    ]
                }
            ]
        }

        actual_instance_ids = self.ec2.find_instances()

        self.boto_client.describe_instances.assert_called_once_with(Filters=[])

        self.assertEqual(
            [{"InstanceId": instance_id} for instance_id in MOCK_BAD_INSTANCES],
            actual_instance_ids,
        )

    def test_find_instances_by_ami(self):
        """Tests EC2 client can find instances by AMI"""
        self.boto_client.describe_instances.return_value = {
            "Reservations": [
                {
                    "Instances": [
                        {"InstanceId": instance_id, "ImageId": image_id}
                        for instance_id, image_id in zip(
                            list(MOCK_BAD_INSTANCES) + list(MOCK_GOOD_INSTANCES),
                            list(MOCK_INITIAL_AMIS) + list(MOCK_DERIVED_AMIS),
                        )
                    ]
                }
            ]
        }

        actual_instance_ids = self.ec2.find_instances(amis=MOCK_DERIVED_AMIS)

        self.boto_client.describe_instances.assert_called_once_with(Filters=[])

        self.assertEqual(
            [
                {"InstanceId": instance_id, "ImageId": image_id}
                for instance_id, image_id in zip(
                    list(MOCK_GOOD_INSTANCES), list(MOCK_DERIVED_AMIS)
                )
            ],
            actual_instance_ids,
        )

    def test_find_instances_by_environment(self):
        """Tests EC2 client can find instances by environment"""
        self.boto_client.describe_instances.return_value = {
            "Reservations": [{"Instances": []}]
        }

        self.ec2.find_instances(environment=MOCK_ENVIRONMENT)

        self.boto_client.describe_instances.assert_called_once_with(
            Filters=[{"Name": "tag:environment", "Values": [MOCK_ENVIRONMENT]}]
        )
