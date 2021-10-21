"""
This module has utility functions for working with aws resources
"""
import logging
from typing import Dict, List, Sequence, Callable

import boto3
from boto.exception import EC2ResponseError, BotoServerError
from botocore.exceptions import ClientError, WaiterError, ReadTimeoutError

from amplify_aws_utils.jitter import Jitter
from .exceptions import (
    TimeoutError,
    ExpectedTimeoutError,
    S3WritingError
)

logger = logging.getLogger(__name__)

STATE_POLL_INTERVAL = 2  # seconds
INSTANCE_SSHABLE_POLL_INTERVAL = 15  # seconds


def create_filters(filter_dict):
    """
    Converts a dict to a list of boto3 filters. The keys and value of the dict represent
    the Name and Values of a filter, respectively.
    """
    filters = []
    for key in filter_dict.keys():
        filters.append({'Name': key, 'Values': filter_dict[key]})

    return filters


def key_values_to_tags(dicts):
    """
    Converts the list of key:value strings (example ["mykey:myValue", ...])
    into a list of AWS tag dicts (example: [{'Key': 'mykey', 'Value': 'myValue'}, ...]
    """
    return [{'Key': tag_key_value[0], 'Value': tag_key_value[1]}
            for tag_key_value in [key_value_option.split(":", 1) for key_value_option in dicts]]


def find_or_create(find, create):
    """Given a find and a create function, create a resource if it doesn't exist"""
    result = find()
    return result if result else create()


def keep_trying(max_time, fun, *args, **kwargs):
    """
    Execute function fun with args and kwargs until it does
    not throw exception or max time has passed.

    After each failed attempt a delay is introduced using Jitter.backoff() function.

    Note: If you are only concerned about throttling use throttled_call
    instead. Any irrecoverable exception within a keep_trying will
    cause a max_time delay.
    """

    jitter = Jitter()
    time_passed = 0
    while True:
        try:
            return fun(*args, **kwargs)
        except Exception:
            if logging.getLogger().level == logging.DEBUG:
                logger.exception("Failed to run %s.", fun)
            if time_passed > max_time:
                raise
            time_passed = jitter.backoff()


def throttled_call(fun, *args, **kwargs):
    """
    Execute function fun with args and kwargs until it does
    not throw a throttled exception or 5 minutes have passed.

    After each failed attempt a delay is introduced using Jitter.backoff() function.
    """
    max_time = 5 * 60
    jitter = Jitter()
    time_passed = 0

    while True:
        try:
            return fun(*args, **kwargs)
        except ClientError as err:
            if logging.getLogger().level == logging.DEBUG:
                logger.exception("Failed to run %s.", fun)

            error_code = err.response['Error'].get('Code', 'Unknown')
            is_throttle_exception = any(
                key_word in error_code
                for key_word in ("Throttling", "RequestLimitExceeded", "TooManyRequestsException")
            )

            if not is_throttle_exception or time_passed > max_time:
                raise

            time_passed = jitter.backoff()
        except (ReadTimeoutError, WaiterError):
            if time_passed > max_time:
                raise

            time_passed = jitter.backoff()


# pylint: disable=no-else-return
def wait_for_state(resource, state, timeout=15 * 60, state_attr='state'):
    """Wait for an AWS resource to reach a specified state"""
    jitter = Jitter()
    time_passed = 0

    while True:
        try:
            resource.update()
            current_state = getattr(resource, state_attr)
            if current_state == state:
                return
            elif current_state in ('failed', 'terminated'):
                raise ExpectedTimeoutError(
                    f"{resource} entered state {current_state} after {time_passed}s waiting for state {state}"
                )
        except (EC2ResponseError, BotoServerError):
            pass  # These are most likely transient, we will timeout if they are not

        if time_passed >= timeout:
            raise TimeoutError(
                f"Timed out waiting for {resource} to change state to {state} after {time_passed}s."
            )

        time_passed = jitter.backoff()


def wait_for_state_boto3(describe_func, params_dict, resources_name,
                         expected_state, state_attr='state', timeout=15 * 60):
    """Wait for an AWS resource to reach a specified state using the boto3 library"""
    jitter = Jitter()
    time_passed = 0
    while True:
        try:
            resources = describe_func(**params_dict)[resources_name]
            if not isinstance(resources, list):
                resources = [resources]

            all_good = True
            failure = False
            for resource in resources:
                if resource[state_attr] in ('failed', 'terminated'):
                    failure = True
                    all_good = False
                elif resource[state_attr] != expected_state:
                    all_good = False

            if all_good:
                return
            elif failure:
                raise ExpectedTimeoutError(
                    "At least some resources who meet the following description "
                    "entered either 'failed' or 'terminated' state "
                    f"after {time_passed}s waiting for state {expected_state}:\n{params_dict}"
                )
        except (EC2ResponseError, ClientError):
            pass  # These are most likely transient, we will timeout if they are not

        if time_passed >= timeout:
            raise TimeoutError(
                "Timed out waiting for resources who meet the following description to change "
                f"state to {expected_state} after {time_passed}s:\n{params_dict}"
            )

        time_passed = jitter.backoff()


def wait_for_sshable(remotecmd, instance, timeout=15 * 60, quiet=False):
    """
    Returns True when host is up and sshable
    returns False on timeout
    """
    jitter = Jitter()
    time_passed = 0

    if not quiet:
        logger.info("Waiting for instance %s to be fully provisioned.", instance.id)
    wait_for_state(instance, 'running', timeout)
    if not quiet:
        logger.info("Instance %s running (booting up).", instance.id)

    while True:
        logger.debug("Waiting for %s to become sshable.", instance.id)
        if remotecmd(instance, ['true'], nothrow=True)[0] == 0:
            logger.info("Instance %s now SSHable.", instance.id)
            logger.debug("Waited %s seconds for instance to boot", time_passed)
            return
        if time_passed >= timeout:
            break
        time_passed = jitter.backoff()

    raise TimeoutError(f"Timed out waiting for instance {instance} to become sshable after {timeout}s.")


def get_boto3_paged_results(
        func: Callable,
        results_key: str,
        next_token_key: str = 'NextToken',
        next_request_token_key: str = 'NextToken',
        *args,
        **kwargs
) -> List:
    """
    Helper method for automatically making multiple boto3 requests for their listing functions
    :param func: Boto3 function to call
    :param results_key: Key of response dict that contains list items
    :param next_token_key: Key of the response dict that contains the paging token
    :param next_request_token_key: Name of the request parameter to pass the token key as.
    :return list:
    """
    response = throttled_call(func, *args, **kwargs)
    response_items = response.get(results_key, [])
    if not response_items:
        logger.warning("No items found in response=%s", response)

    next_token = response.get(next_token_key)
    prev_token = None

    # Apparently sometimes the next token can be repeated back to you, and that means stop making requests
    while next_token and next_token != prev_token:
        kwargs[next_request_token_key] = next_token
        response = throttled_call(func, *args, **kwargs)
        response_items += response[results_key]
        prev_token = next_token
        next_token = response.get(next_token_key)

    return response_items


def check_written_s3(object_name, expected_written_length, written_length):
    """
    Check S3 object is written by checking the bytes_written from key.set_contents_from_* method
    Raise error if any problem happens so we can diagnose the causes
    """
    if expected_written_length != written_length:
        raise S3WritingError(f"{object_name} is not written correctly to S3 bucket")


def get_ssm_parameters(names: Sequence[str]) -> Dict[str, str]:
    """
    Convenience function for getting multiple SSM parameters
    :param names: List of names of parameters to get.
    :return: A dictionary of the name of the parameter to its value.
    """
    client = boto3.client('ssm')

    results = throttled_call(
        client.get_parameters,
        Names=names,
        WithDecryption=True
    )

    return {param['Name']: param['Value'] for param in results['Parameters']}


def get_ssm_parameter(name: str) -> str:
    """
    Convenience function for getting a single SSM parameter
    :param name: Name of the parameter to get.
    :return: SSM parameter value.
    """
    client = boto3.client('ssm')

    results = throttled_call(
        client.get_parameter,
        Name=name,
        WithDecryption=True
    )

    return results["Parameter"]['Value']


# DEPRACATED
def tag2dict(tags):
    """
    tag2dict is deprecated, its replaced by boto3_tags_to_dict
    :param tags:
    :return:
    """
    boto3_tags_to_dict(tags)


def boto3_tags_to_dict(boto3_tags):
    """
    Convenience function for converting boto3 tags to a dictionary
    :param boto3_tags: List of boto3 tags.
    :return: Simple dictionary of tags
    """
    if not boto3_tags:
        return {}

    # boto3 is not consistent with the tag dict it returns
    # depending on the resource, the tag name will either be under a 'Key' or 'Name' key or even in lowercase
    # check all possibilities to figure out what key names are being used
    possible_keys = ['Key', 'key', 'Name', 'name']
    actual_keys = (key for key in boto3_tags[0].keys() if key in possible_keys)
    key_name = next(actual_keys, None)

    possible_values = ['Value', 'value']
    actual_values = (key for key in boto3_tags[0].keys() if key in possible_values)
    value_name = next(actual_values, None)

    if not key_name or not value_name:
        raise RuntimeError('Unable to identify tag key names in dict')

    return {
        tag[key_name]: tag[value_name]
        for tag in boto3_tags
    }


def dict_to_boto3_tags(tag_dict):
    """
    Convenience function for converting a dictionary to boto3 tags
    :param tag_dict: A dictionary of str to str.
    :return: A list of boto3 tags.
    """
    return [
        {"Key": key, "Value": value}
        for key, value in tag_dict.items()
    ]


def chunker(sequence, size):
    """
    Creates a generator that yields chunks of sequence in the given size
    for group in chunker(range(0, 20), 5):
        print group
    # [0, 1, 2, 3, 4]
    # [5, 6, 7, 8, 9]
    # [10, 11, 12, 13, 14]
    # [15, 16, 17, 18, 19]
    """
    return (sequence[position:position + size] for position in range(0, len(sequence), size))


def dynamodb_record_to_dict(record: Dict[str, Dict[str, str]]) -> Dict[str, str]:
    """
    Converts a DynamoDB Record into a normal Python dictionary.
    :param record: A DynamoDB Record that looks like:
    {
        "foo": {
            "S": "bar"
        },
        "baz": {
            "N": "100"
        }
    }
    :return: A dictionary that looks like:
    {
        "foo": "bar",
        "baz": "100",
    }
    """
    return {
        key: list(value.values())[0]
        for key, value in record.items()
    }
