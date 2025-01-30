"""
This module has utility functions for working with aws resources
"""
import logging
import traceback
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

import boto3
from aws_lambda_powertools.middleware_factory import lambda_handler_decorator
from aws_lambda_powertools.utilities.typing import LambdaContext
from botocore.exceptions import ClientError, WaiterError, ReadTimeoutError

from amplify_aws_utils.jitter import Jitter

# pylint: disable=redefined-builtin
from .exceptions import (
    CatchAllExceptionError,
    TimeoutError,
    ExpectedTimeoutError,
    S3WritingError,
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
        filters.append({"Name": key, "Values": filter_dict[key]})

    return filters


def key_values_to_tags(dicts):
    """
    Converts the list of key:value strings (example ["mykey:myValue", ...])
    into a list of AWS tag dicts (example: [{'Key': 'mykey', 'Value': 'myValue'}, ...]
    """
    return [
        {"Key": tag_key_value[0], "Value": tag_key_value[1]}
        for tag_key_value in [
            key_value_option.split(":", 1) for key_value_option in dicts
        ]
    ]


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

            error_code = err.response["Error"].get("Code", "Unknown")
            is_throttle_exception = any(
                key_word in error_code
                for key_word in (
                    "Throttling",
                    "RequestLimitExceeded",
                    "TooManyRequestsException",
                    "ServiceUnavailable",
                    "DatabaseResumingException",
                )
            )

            if not is_throttle_exception or time_passed > max_time:
                raise

            time_passed = jitter.backoff()
        except (ReadTimeoutError, WaiterError):
            if time_passed > max_time:
                raise

            time_passed = jitter.backoff()


def wait_for_state_boto3(
    describe_func,
    params_dict,
    resources_name,
    expected_state,
    state_attr="state",
    timeout=15 * 60,
):
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
                if resource[state_attr] in ("failed", "terminated"):
                    failure = True
                    all_good = False
                elif resource[state_attr] != expected_state:
                    all_good = False

            if all_good:
                return
            if failure:
                raise ExpectedTimeoutError(
                    "At least some resources who meet the following description "
                    "entered either 'failed' or 'terminated' state "
                    f"after {time_passed}s waiting for state {expected_state}:\n{params_dict}"
                )
        except ClientError:
            pass  # These are most likely transient, we will timeout if they are not

        if time_passed >= timeout:
            raise TimeoutError(
                "Timed out waiting for resources who meet the following description to change "
                f"state to {expected_state} after {time_passed}s:\n{params_dict}"
            )

        time_passed = jitter.backoff()


# pylint: disable=keyword-arg-before-vararg
def get_boto3_paged_results(
    func: Callable,
    results_key: str,
    next_token_key: str = "NextToken",
    next_request_token_key: str = "NextToken",
    *args,
    **kwargs,
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
        logger.debug("No items found in response=%s", response)

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
    client = boto3.client("ssm")

    results = throttled_call(client.get_parameters, Names=names, WithDecryption=True)

    return {param["Name"]: param["Value"] for param in results["Parameters"]}


def get_ssm_parameter(name: str) -> str:
    """
    Convenience function for getting a single SSM parameter
    :param name: Name of the parameter to get.
    :return: SSM parameter value.
    """
    client = boto3.client("ssm")

    results = throttled_call(client.get_parameter, Name=name, WithDecryption=True)

    return results["Parameter"]["Value"]


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
    possible_keys = ["Key", "key", "Name", "name"]
    actual_keys = (key for key in boto3_tags[0].keys() if key in possible_keys)
    key_name = next(actual_keys, None)

    possible_values = ["Value", "value"]
    actual_values = (key for key in boto3_tags[0].keys() if key in possible_values)
    value_name = next(actual_values, None)

    if not key_name or not value_name:
        raise RuntimeError("Unable to identify tag key names in dict")

    return {tag[key_name]: tag[value_name] for tag in boto3_tags}


def dict_to_boto3_tags(tag_dict):
    """
    Convenience function for converting a dictionary to boto3 tags
    :param tag_dict: A dictionary of str to str.
    :return: A list of boto3 tags.
    """
    return [{"Key": key, "Value": value} for key, value in tag_dict.items()]


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
    return (
        sequence[position : position + size]
        for position in range(0, len(sequence), size)
    )


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
    return {key: list(value.values())[0] for key, value in record.items()}


# pylint: disable=invalid-name
@lambda_handler_decorator
def catchall_exception_lambda_handler_decorator(
    handler: Callable,
    event: Dict[str, Any],
    context: LambdaContext,
    log_exception: Optional[bool] = True,
    raise_exception: Optional[bool] = True,
) -> Union[Dict[str, Any], None]:
    """
    Decorator to handle uncaught exceptions for a lambda handler.

    The lambda handler function being decorated must have the signature `handler(event, context)`.

    This decorator will catch all uncaught exceptions from the decorated lambda handler function,
    then manage it. There will be a particular focus on the exception chaining info of the
    exception because by default it will be lost forever when the exception propagates out of the
    lambda function. The exception can be handled as follows:

        - optionally log the caught exception, including the exception chaining info, before the
          exception propagates out of the lambda function.

        - optionally raise new `CatchAllExceptionError` exception instance, with the error message
          portion set with the caught exception's exception chaining info.

    Note that setting both *log_exception* and *raise_exception* to True, the default, will
    probably result in the exception being logged twice by your log service. It will first be
    explicitly logged by this decorator when *log_exception* is True. Then, when *raise_exception*
    is True, and the exception propagates out of the lambda function, your log service will
    probably log it.

    Example usage:

        @catchall_exception_lambda_handler_decorator
        def lambda_handler(event, context):
            return True

        @catchall_exception_lambda_handler_decorator(log_exception=False)
        def lambda_handler(event, context):
            return True

        @catchall_exception_lambda_handler_decorator(raise_exception=False)
        def lambda_handler(event, context):
            return True

        @catchall_exception_lambda_handler_decorator(log_exception=False, raise_exception=False)
        def lambda_handler(event, context):
            return True

    :param handler: The lambda function's main handler
    :param event: The incoming lambda event
    :param context: The incoming lambda context
    :param log_exception: Defaults to True. If True, will log the caught exception, including the
    exception chaining.
    :param raise_exception: Defaults to True. If True, will raise a new CatchAllExceptionError with
    the error message portion set with the exception chaining info from the caught exception.
    :return: The response of the lambda handler
    """
    try:
        return handler(event, context)
    except Exception as exc:
        if log_exception:
            # log stack trace info, including exception chaining
            logger.exception("Catchall exception logging")

        if raise_exception:
            err_mssg = repr(exc)
            stack_trace = traceback.format_exc()

            # a bit kludgy to put the stack trace info into the error message portion of the
            # exception, but necessary to retain the exception chaining info b/c exception
            # chaining will be lost once an exception propagates out of a lambda function
            raise CatchAllExceptionError(
                f"Catchall exception. err_mssg: {err_mssg}, stack_trace: {stack_trace}"
            ) from exc

    return None


def to_bool(value: Any) -> bool:
    """
    Convert `value` to the lower string and compare with the list ["yes", "y", "true", "t", "on", "1"]
    If in the list then `true` else `false`
    """
    if str(value).lower() in ["yes", "y", "true", "t", "on", "1"]:
        return True

    return False
