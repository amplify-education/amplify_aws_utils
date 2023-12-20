"""
Tests for resource Helper
"""
from unittest import TestCase
from unittest.mock import patch, MagicMock

from botocore.exceptions import ClientError, WaiterError

# pylint: disable=redefined-builtin
from amplify_aws_utils.exceptions import (
    CatchAllExceptionError,
    ExpectedTimeoutError,
    TimeoutError,
)
from amplify_aws_utils.resource_helper import (
    Jitter,
    catchall_exception_lambda_handler_decorator,
    dynamodb_record_to_dict,
    keep_trying,
    throttled_call,
    wait_for_state_boto3,
    to_bool,
)


class MockError(Exception):
    """Exception for testing"""


# time.sleep is being patched but not referenced.
# pylint: disable=unused-argument
class ResourceHelperTests(TestCase):
    """Test Resource Helper"""

    @patch("time.sleep", return_value=None)
    def test_jitter(self, mock_sleep):
        """Test Jitter backoff"""
        min_wait = 3
        jitter = Jitter(min_wait=min_wait)
        previous_time_passed = 0
        while True:
            time_passed = jitter.backoff()
            wait_time = time_passed - previous_time_passed
            self.assertTrue(wait_time >= min_wait)
            self.assertTrue(wait_time <= max(min_wait, previous_time_passed * 3))
            self.assertTrue(wait_time <= Jitter.MAX_POLL_INTERVAL)
            previous_time_passed = time_passed
            if time_passed > 1000:
                break

    @patch("time.sleep", return_value=None)
    def test_keep_trying_noerr(self, mock_sleep):
        """Test keep_trying with no error"""
        mock_func = MagicMock()
        mock_func.side_effect = [Exception, Exception, True]
        keep_trying(10, mock_func)
        self.assertEqual(3, mock_func.call_count)

    @patch("time.sleep", return_value=None)
    def test_keep_trying_timeout(self, mock_sleep):
        """Test keep_trying with timeout"""
        mock_func = MagicMock()
        mock_func.side_effect = Exception
        self.assertRaises(Exception, keep_trying, 10, mock_func)

    @patch("time.sleep", return_value=None)
    def test_throttled_call_clienterror_noerr(self, mock_sleep):
        """Test throttle_call with no error"""
        mock_func = MagicMock()
        error_response = {"Error": {"Code": "Throttling"}}
        client_error = ClientError(error_response, "test")
        mock_func.side_effect = [client_error, client_error, True]
        throttled_call(mock_func)
        self.assertEqual(3, mock_func.call_count)

    @patch("time.sleep", return_value=None)
    def test_throttled_call_clienterror_timeout(self, mock_sleep):
        """Test throttle_call with ClientError timeout"""
        mock_func = MagicMock()
        error_response = {"Error": {"Code": "Throttling"}}
        client_error = ClientError(error_response, "test")
        mock_func.side_effect = client_error
        self.assertRaises(ClientError, throttled_call, mock_func)

    @patch("time.sleep", return_value=None)
    def test_throttled_call_waitererror_timeout(self, mock_sleep):
        """Test throttle_call with WaiterError timeout"""
        mock_func = MagicMock()
        last_response = {"Error": {"Code": "Throttling"}}
        waiter_error = WaiterError("Timeout", "test", last_response)
        mock_func.side_effect = waiter_error
        self.assertRaises(WaiterError, throttled_call, mock_func)

    @patch("time.sleep", return_value=None)
    def test_throttled_call_clienterror_error(self, mock_sleep):
        """Test throttle_call with error"""
        mock_func = MagicMock()
        error_response = {"Error": {"Code": "MyError"}}
        client_error = ClientError(error_response, "test")
        mock_func.side_effect = client_error
        self.assertRaises(ClientError, throttled_call, mock_func)
        self.assertEqual(1, mock_func.call_count)

    @patch("time.sleep", return_value=None)
    def test_wait_for_state_boto3_noerr(self, mock_sleep):
        """Test wait_for_state_boto3 with no error"""
        mock_describe_func = MagicMock(
            return_value={"myresource": {"status": "available"}}
        )
        wait_for_state_boto3(
            mock_describe_func,
            {"param1": "p1"},
            "myresource",
            "available",
            state_attr="status",
            timeout=30,
        )
        self.assertEqual(1, mock_describe_func.call_count)

    @patch("time.sleep", return_value=None)
    def test_wait_for_state_boto3_timeout(self, mock_sleep):
        """Test wait_for_state_boto3 with timeout"""
        mock_describe_func = MagicMock(
            return_value={"myresource": {"status": "mystatus"}}
        )
        self.assertRaises(
            TimeoutError,
            wait_for_state_boto3,
            mock_describe_func,
            {"param1": "p1"},
            "myresource",
            "available",
            state_attr="status",
            timeout=30,
        )

    @patch("time.sleep", return_value=None)
    def test_wait_for_state_boto3_exp_timeout(self, mock_sleep):
        """Test wait_for_state_boto3 with ExpectedTimeout"""
        mock_describe_func = MagicMock(
            return_value={"myresource": {"status": "failed"}}
        )
        self.assertRaises(
            ExpectedTimeoutError,
            wait_for_state_boto3,
            mock_describe_func,
            {"param1": "p1"},
            "myresource",
            "available",
            state_attr="status",
            timeout=30,
        )
        self.assertEqual(1, mock_describe_func.call_count)

        mock_describe_func = MagicMock(
            return_value={"myresource": {"status": "terminated"}}
        )
        self.assertRaises(
            ExpectedTimeoutError,
            wait_for_state_boto3,
            mock_describe_func,
            {"param1": "p1"},
            "myresource",
            "available",
            state_attr="status",
            timeout=30,
        )
        self.assertEqual(1, mock_describe_func.call_count)

    @patch("time.sleep", return_value=None)
    def test_wait_for_state_boto3_clienterror(self, mock_sleep):
        """Test wait_for_state_boto3 with ClientError and returned Timeout"""
        mock_describe_func = MagicMock()
        error_response = {"Error": {"Code": "MyError"}}
        mock_describe_func.side_effect = ClientError(error_response, "test")
        self.assertRaises(
            TimeoutError,
            wait_for_state_boto3,
            mock_describe_func,
            {"param1": "p1"},
            "myresource",
            "available",
            state_attr="status",
            timeout=30,
        )

    @patch("time.sleep", return_value=None)
    def test_wait_for_state_boto3_error(self, mock_sleep):
        """Test wait_for_state_boto3 with RuntimeError and returned RuntimeError"""
        mock_describe_func = MagicMock()
        mock_describe_func.side_effect = RuntimeError
        self.assertRaises(
            RuntimeError,
            wait_for_state_boto3,
            mock_describe_func,
            {"param1": "p1"},
            "myresource",
            "available",
            state_attr="status",
            timeout=30,
        )
        self.assertEqual(1, mock_describe_func.call_count)

    def test_dynamodb_record_to_dict(self):
        """Test dynamodb_record_to_dict happy"""
        mock_record = {
            "foo": {
                "S": "bar",
            },
            "baz": {
                "N": "100",
            },
        }

        expected = {
            "foo": "bar",
            "baz": "100",
        }

        actual = dynamodb_record_to_dict(
            record=mock_record,
        )

        self.assertEqual(
            expected,
            actual,
        )

    def test_to_bool(self):
        """Test to_bool happy"""
        # true values
        self.assertEqual(to_bool("t"), True)
        self.assertEqual(to_bool("true"), True)
        self.assertEqual(to_bool("yes"), True)
        self.assertEqual(to_bool("y"), True)
        self.assertEqual(to_bool("on"), True)
        self.assertEqual(to_bool("1"), True)
        self.assertEqual(to_bool(1), True)
        # false values
        self.assertEqual(to_bool("0"), False)
        self.assertEqual(to_bool(0), False)
        self.assertEqual(to_bool(None), False)
        self.assertEqual(to_bool("any"), False)
        self.assertEqual(to_bool("n"), False)
        self.assertEqual(to_bool(False), False)
        self.assertEqual(to_bool("False"), False)


# aws_lambda_powertools.middleware_factory.factory.logger is being patched but not referenced.
# pylint: disable=unused-argument,no-value-for-parameter
class CatchallExceptionLambdaHandlerDecoratorTests(TestCase):
    """Test amplify_aws_utils.resource_helper.catchall_exception_lambda_handler_decorator()"""

    # pylint: disable=assignment-from-no-return
    @patch("aws_lambda_powertools.middleware_factory.factory.logger")
    @patch("amplify_aws_utils.resource_helper.logger")
    def test_no_raise_exception(self, mock_logger, mock_aws_lambda_powertools_logger):
        """Tests catchall_exception_lambda_handler_decorator, doesn't raise exception, does log exception"""

        @catchall_exception_lambda_handler_decorator(raise_exception=False)
        def _lambda_handler(*_):
            raise MockError("some exception")

        actual_response = _lambda_handler({}, MagicMock)

        mock_logger.exception.assert_called_once_with("Catchall exception logging")
        self.assertEqual(actual_response, None)

    @patch("aws_lambda_powertools.middleware_factory.factory.logger")
    @patch("amplify_aws_utils.resource_helper.logger")
    def test_no_log_exception(self, mock_logger, mock_aws_lambda_powertools_logger):
        """Tests catchall_exception_lambda_handler_decorator, doesn't log exception, does raise exception"""

        @catchall_exception_lambda_handler_decorator(log_exception=False)
        def _lambda_handler(*_):
            raise MockError("some exception")

        with self.assertRaises(CatchAllExceptionError) as context:
            _lambda_handler({}, MagicMock)

        self.assertRegex(str(context.exception), "some exception")
        mock_logger.exception.assert_not_called()

    @patch("aws_lambda_powertools.middleware_factory.factory.logger")
    @patch("amplify_aws_utils.resource_helper.logger")
    def test_no_raise_no_log_exception(
        self, mock_logger, mock_aws_lambda_powertools_logger
    ):
        """
        Tests catchall_exception_lambda_handler_decorator, doesn't raise exception, doesn't log exception
        """

        @catchall_exception_lambda_handler_decorator(
            log_exception=False, raise_exception=False
        )
        def _lambda_handler(*_):
            raise MockError("some exception")

        actual_response = _lambda_handler({}, MagicMock)

        mock_logger.exception.assert_not_called()
        self.assertEqual(actual_response, None)

    @patch("aws_lambda_powertools.middleware_factory.factory.logger")
    @patch("amplify_aws_utils.resource_helper.logger")
    def test_exception_chaining_in_err_mssg(
        self, mock_logger, mock_aws_lambda_powertools_logger
    ):
        """
        Tests catchall_exception_lambda_handler_decorator correctly retains exception chaining
        in exception error message
        """

        def _some_func():
            raise MockError("some exception 1")

        @catchall_exception_lambda_handler_decorator
        def _lambda_handler(*_):
            try:
                _some_func()
            except Exception as exc:
                raise MockError("some exception 2") from exc

        with self.assertRaises(CatchAllExceptionError) as context:
            _lambda_handler({}, MagicMock)

        self.assertRegex(str(context.exception), "some exception 1")
        self.assertRegex(str(context.exception), "some exception 2")

        mock_logger.exception.assert_called_once_with("Catchall exception logging")

    @patch("amplify_aws_utils.resource_helper.logger")
    def test_wrap_response(self, mock_logger):
        """Tests catchall_exception_lambda_handler_decorator correctly wraps the response"""
        mock_response = {
            "foo": "bar",
            1: 2,
        }

        @catchall_exception_lambda_handler_decorator
        def _lambda_handler(*_):
            return mock_response

        actual_response = _lambda_handler({}, MagicMock())

        self.assertEqual(actual_response, mock_response)

        mock_logger.exception.assert_not_called()
