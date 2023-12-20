"""Contains SpotinstClient class for talking to the Spotinst REST API"""
import logging
from typing import Dict, Any, List, Callable, Optional

import requests

# pylint: disable=redefined-builtin
from requests.exceptions import Timeout, ConnectionError

from amplify_aws_utils.jitter import Jitter

SPOTINST_API_HOST = "https://api.spotinst.io"
logger = logging.getLogger(__name__)


class SpotinstClient:
    """A client for the Spotinst REST API"""

    def __init__(self, token: str, account_id: str):
        self.token = token
        self.account_id = account_id

    def create_group(self, group_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new Elastigroup
        :param dict group_config: Config parameters for Elastigroup
        :return: Elastigroup
        :rtype: dict
        """
        response = self._make_throttled_request(
            path="aws/ec2/group", data=group_config, method="post"
        )
        return response["response"]["items"][0]

    def update_group(self, group_id: str, group_config: Dict[str, Any]):
        """
        Update an existing Elastigroup
        :param str group_id: Id of group to update
        :param dict group_config: New group config
        """
        self._make_throttled_request(
            path=f"aws/ec2/group/{group_id}", data=group_config, method="put"
        )

    def get_group(self, group_id: str) -> Dict[str, Any]:
        """
        Get group status
        :param str group_id: Id of Elastigroup to get status info for
        :return: List of instances in a Elastigroup
        :rtype: list[dict]
        """
        response = self._make_throttled_request(
            path=f"aws/ec2/group/{group_id}", method="get"
        )
        return response["response"]["items"][0]

    def get_instances_in_group(self, group_id: str):
        """
        Get group status
        :param str group_id: Id of Elastigroup to get status info for
        :return: List of instances in a Elastigroup
        :rtype: list[dict]
        """
        response = self._make_throttled_request(
            path=f"aws/ec2/group/{group_id}" + "/status", method="get"
        )
        return response["response"]["items"]

    def get_groups(self) -> List[Dict[str, Any]]:
        """
        Get a list of all Elastigroup
        :return: Lst of Elastigroups
        :rtype: list[dict]
        """
        return self._make_throttled_request(path="aws/ec2/group", method="get")[
            "response"
        ]["items"]

    def delete_group(self, group_id: str):
        """
        Delete an Elastigroup
        :param str group_id: Id of group to delete
        """
        self._make_throttled_request(path=f"aws/ec2/group/{group_id}", method="delete")

    def roll_group(
        self,
        group_id: str,
        batch_percentage: int,
        grace_period: int,
        health_check_type: str,
    ):
        """
        Spin up a new set of instances and then shutdown the old instances
        :param str group_id: Id of Elastigroup to perform roll operation on
        :param int batch_percentage: Percentage of the group to roll at a time
        :param int grace_period: Amount of time in seconds to wait for instances to pass health checks
        :param str health_check_type: Type of health check to use. Available options are ELB, TARGET_GROUP,
                                      MLB, HCS, EC2, NONE
        """
        request = {
            "batchSizePercentage": batch_percentage,
            "gracePeriod": grace_period,
            "healthCheckType": health_check_type,
            "strategy": {"action": "REPLACE_SERVER"},
        }
        self._make_throttled_request(
            path=f"aws/ec2/group/{group_id}/roll", data=request, method="put"
        )

    def get_deployments(self, group_id: str) -> List[Dict[str, Any]]:
        """
        Get list of current and past deployments for a Elastigroup
        :param str group_id:
        :return list[dict]:
        """
        response = self._make_throttled_request(
            path=f"aws/ec2/group/{group_id}/roll", method="get"
        )
        deploys = response["response"]["items"]
        return sorted(deploys, key=lambda deploy: deploy["createdAt"])

    def get_roll_status(self, group_id: str, deploy_id: str) -> Dict[str, Any]:
        """
        Get info about a specific deployment (also called a roll) for a group
        :param str group_id:
        :param str deploy_id:
        :return dict:
        """
        response = self._make_throttled_request(
            path=f"aws/ec2/group/{group_id}/roll/{deploy_id}", method="get"
        )
        return response["response"]["items"][0]

    def get_group_instances_health(self, group_id: str) -> List[Dict[str, Any]]:
        """
        Get info about a specific deployment (also called a roll) for a group
        :param str group_id:
        :return dict:
        """
        response = self._make_throttled_request(
            path=f"aws/ec2/group/{group_id}/instanceHealthiness", method="get"
        )
        return response["response"]["items"]

    def _make_throttled_request(
        self,
        method: str,
        path: str,
        params: Dict[str, str] = None,
        data: Dict[str, Any] = None,
    ) -> Dict[str, Any]:
        return self._throttle_spotinst_call(
            self._make_request, method, path, params, data
        )

    def _make_request(
        self,
        method: str,
        path: str,
        params: Dict[str, str] = None,
        data: Dict[str, Any] = None,
    ):
        """
        Convenience function for making requests to the Spotinst API.

        :param str method: What HTTP method to use.
        :param str path: The API endpoint to call. IE: aws/ec2/group
        :param dict params: Dictionary of query parameters.
        :param dict data: Body data.
        :return: The response from the Spotinst API.
        :rtype: dict
        """
        try:
            params = params or {}
            params["accountId"] = self.account_id

            response = requests.request(
                method=method,
                url=f"{SPOTINST_API_HOST}/{path}",
                params=params,
                json=data,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.token}",
                },
                timeout=60,
            )
        except (ConnectionError, Timeout) as err:
            raise SpotinstRateExceededException(
                f"Rate exceeded while calling {method} {path}: {err}"
            ) from err

        if response.status_code == 401:
            raise SpotinstApiException("Provided Spotinst API token is not valid")

        if response.status_code == 429:
            raise SpotinstRateExceededException(
                f"Rate exceeded while calling {method} {path}"
            )

        try:
            ret = response.json()
        except ValueError as err:
            raise SpotinstApiException(
                f"Spotinst API did not return JSON response: {response.text}"
            ) from err

        if response.status_code != 200:
            status = ret["response"]["status"]
            req_id = ret["request"]["id"]
            errors = ret["response"].get("errors") or []

            for error in errors:
                if error.get("code") in ("Throttling", "RequestLimitExceeded"):
                    raise SpotinstRateExceededException(
                        f"Rate exceeded while calling {method} {path}"
                    )

            raise SpotinstApiException(
                f"Unknown Spotinst API error encountered: {status} {errors}. RequestId {req_id}"
            )

        return ret

    def _throttle_spotinst_call(self, fun: Callable, *args, **kwargs):
        max_time = 5 * 60
        jitter = Jitter(
            min_wait=60
        )  # wait at least 60 seconds because our rate limit resets then
        time_passed = 0

        while True:
            try:
                return fun(*args, **kwargs)
            except SpotinstRateExceededException:
                if logging.getLogger().level == logging.DEBUG:
                    logger.exception("Failed to run %s.", fun)

                if time_passed > max_time:
                    raise

                time_passed = jitter.backoff()


def get_tag_for_spotinst_group(group: Dict[str, Any], key: str) -> Optional[str]:
    """
    Get a tag for a Spotinst group by key
    :param group: Spotinst group dict
    :param key: Tag key
    :return:
    """
    return spotinst_tags_to_dict(
        group["compute"]["launchSpecification"].get("tags", {})
    ).get(key)


def spotinst_tags_to_dict(tags: List[Dict[str, str]]) -> Dict[str, str]:
    """
    Converts spotinst tage to dictionaries
    :param tags: spotinst tages
    :return: Dictionary that contains spotinst tags
    """
    return {tag["tagKey"]: tag["tagValue"] for tag in tags}


def dict_to_spotinst_tags(tag_dict: Dict[str, str]) -> List[Dict[str, str]]:
    """
    Converts dictionaries to spotinst tags
    :param tag_dict: Dictionary that needs to converted
    :return: Spotinst tags
    """
    return [{"tagKey": key, "tagValue": value} for key, value in tag_dict.items()]


class SpotinstApiException(Exception):
    """Raised if Spotinst API problem encountered"""


class SpotinstRateExceededException(Exception):
    """Raised if Spotinst API throttled a request"""
