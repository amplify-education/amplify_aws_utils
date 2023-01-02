"""Class for wrapping common EC2 API calls"""
import logging
from typing import List, Dict, Any, Set
from datetime import datetime

from mypy_boto3_ec2.client import EC2Client

from amplify_aws_utils.resource_helper import (
    get_boto3_paged_results,
    throttled_call,
    create_filters,
)

DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
logger = logging.getLogger(__name__)


class EC2:
    """Class for wrapping common EC2 API calls"""

    def __init__(self, ec2_client: EC2Client):
        self.ec2_client = ec2_client

    def find_instances(
        self, amis: Set[str] = None, environment: str = None, instance_state: str = None
    ) -> List[Dict[str, Any]]:
        """
        Function for finding instances, with optional AMI filtering.
        :param amis: A set of AMIs to filter by, optionally.
        :param environment:
        :param instance_state:
        :return: A set of instance-ids in the account, optionally filtered by AMI.
        """
        filters = {}
        if environment:
            filters["tag:environment"] = [environment]

        if instance_state:
            filters["instance-state-name"] = [instance_state]

        reservations = get_boto3_paged_results(
            func=self.ec2_client.describe_instances,
            results_key="Reservations",
            Filters=create_filters(filters),
        )

        instances = [
            instance
            for reservation in reservations
            for instance in reservation["Instances"]
            if not amis or instance["ImageId"] in amis
        ]

        logger.info(
            "Discovered additional instances=%s",
            {instance["InstanceId"] for instance in instances},
        )
        return instances

    def find_amis(
        self, source_amis: Set[str] = None, newer_than: datetime = None
    ) -> Set[str]:
        """
        Function for finding AMIs that are children of a given set of AMIs.
        :param source_amis: A set of parent ami-ids to find children of.
        :param newer_than: Only AMIs newer than this datetime object will be returned.
        :return: A set of AMI ids.
        """
        filters = {}
        if source_amis:
            filters["tag:source_ami"] = list(source_amis)

        images = throttled_call(
            self.ec2_client.describe_images,
            Filters=create_filters(filters),
            Owners=["self"],
        )["Images"]

        image_ids = {
            image["ImageId"]
            for image in images
            if not newer_than
            or datetime.strptime(image["CreationDate"], DATETIME_FORMAT) > newer_than
        }
        logger.info("Found additional images=%s", image_ids)
        return image_ids
