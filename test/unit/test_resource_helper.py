"""
Tests for resource Helper
"""
import random
from unittest import TestCase
from unittest.mock import patch, MagicMock, create_autospec

from boto.exception import EC2ResponseError
import boto.ec2.instance
from botocore.exceptions import ClientError, WaiterError

from amplify_aws_utils.exceptions import ExpectedTimeoutError
from amplify_aws_utils.exceptions import TimeoutError
from amplify_aws_utils.resource_helper import (
    Jitter,
    keep_trying,
    throttled_call,
    throttled_call_on_exception,
    wait_for_state,
    wait_for_state_boto3,
    wait_for_sshable,
    dynamodb_record_to_dict,
)


class SupportedThrottleError(BaseException):
    """Simple error for unit test of supported Exceptions"""


class UnSupportedThrottleError(BaseException):
    """Simple error for unit test of unsupported Exceptions"""


# time.sleep is being patched but not referenced.
# pylint: disable=W0613, invalid-name, line-too-long
class ResourceHelperTests(TestCase):
    """Test Resource Helper"""

    def setUp(self):
        self.call_count = 0

    def mock_instance(self):
        """Create a mock Instance"""
        inst = create_autospec(boto.ec2.instance.Instance)
        inst.id = 'i-' + ''.join(random.choice("0123456789abcdef") for _ in range(8))
        inst.instance_id = inst.id
        return inst

    @patch('time.sleep', return_value=None)
    def test_jitter(self, mock_sleep):
        """Test Jitter backoff """
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

    @patch('time.sleep', return_value=None)
    def test_keep_trying_noerr(self, mock_sleep):
        """Test keep_trying with no error"""
        mock_func = MagicMock()
        mock_func.side_effect = [Exception, Exception, True]
        keep_trying(10, mock_func)
        self.assertEqual(3, mock_func.call_count)

    @patch('time.sleep', return_value=None)
    def test_keep_trying_timeout(self, mock_sleep):
        """Test keep_trying with timeout"""
        mock_func = MagicMock()
        mock_func.side_effect = Exception
        self.assertRaises(Exception, keep_trying, 10, mock_func)

    @patch('time.sleep', return_value=None)
    def test_throttled_call_clienterror_noerr(self, mock_sleep):
        """Test throttle_call with no error"""
        mock_func = MagicMock()
        error_response = {"Error": {"Code": "Throttling"}}
        client_error = ClientError(error_response, "test")
        mock_func.side_effect = [client_error, client_error, True]
        throttled_call(mock_func)
        self.assertEqual(3, mock_func.call_count)

    @patch('time.sleep', return_value=None)
    def test_throttled_call_clienterror_timeout(self, mock_sleep):
        """Test throttle_call with ClientError timeout"""
        mock_func = MagicMock()
        error_response = {"Error": {"Code": "Throttling"}}
        client_error = ClientError(error_response, "test")
        mock_func.side_effect = client_error
        self.assertRaises(ClientError, throttled_call, mock_func)

    @patch('time.sleep', return_value=None)
    def test_throttled_call_waitererror_timeout(self, mock_sleep):
        """Test throttle_call with WaiterError timeout"""
        mock_func = MagicMock()
        last_response = {"Error": {"Code": "Throttling"}}
        waiter_error = WaiterError('Timeout', 'test', last_response)
        mock_func.side_effect = waiter_error
        self.assertRaises(WaiterError, throttled_call, mock_func)

    @patch('time.sleep', return_value=None)
    def test_throttled_call_clienterror_error(self, mock_sleep):
        """Test throttle_call with error"""
        mock_func = MagicMock()
        error_response = {"Error": {"Code": "MyError"}}
        client_error = ClientError(error_response, "test")
        mock_func.side_effect = client_error
        self.assertRaises(ClientError, throttled_call, mock_func)
        self.assertEqual(1, mock_func.call_count)

    @patch('time.sleep', return_value=None)
    def test_throttled_call_on_exception_dec_none_param_func_no_exc(self, mock_sleep):
        """Test throttled_call_on_exception, when decorator has None param, func has no exception."""

        expected_call_count = 1

        def _reset():
            self.call_count = 0
            mock_sleep.reset_mock()

        def _test_basic(func):
            func()
            self.assertEqual(expected_call_count, self.call_count)
            mock_sleep.assert_not_called()

        # test default call to decorator
        _reset()

        @throttled_call_on_exception()
        def test_func1():
            self.call_count += 1

        _test_basic(test_func1)

        # test default call to decorator with no parentheses
        _reset()

        @throttled_call_on_exception
        def test_func2():
            self.call_count += 1

        _test_basic(test_func2)

        # test call to decorator with positional arg of None (same as default)
        _reset()

        @throttled_call_on_exception(None)
        def test_func3():
            self.call_count += 1

        _test_basic(test_func3)

        # test call to decorator with kwarg of None (same as default)
        _reset()

        @throttled_call_on_exception(is_throttled_exception=None)
        def test_func4():
            self.call_count += 1

        _test_basic(test_func4)

        # test default call to decorator with args to function
        _reset()
        expected_return_value = 12

        @throttled_call_on_exception()
        def test_func5(arg1, arg2, arg3):
            self.call_count += 1
            return arg1 + arg2 + arg3

        actual_return_value = test_func5(7, arg3=2, arg2=3)
        self.assertEqual(expected_return_value, actual_return_value)
        self.assertEqual(expected_call_count, self.call_count)
        mock_sleep.assert_not_called()

    @patch('time.sleep', return_value=None)
    def test_throttled_call_on_exception_dec_none_param_func_exc_throttles(self, mock_sleep):
        """
        Test throttled_call_on_exception, when decorator has None param, func has exception, throttles
        and executes
        """

        expected_call_count = 3

        self.call_count = 0
        mock_sleep.reset_mock()

        @throttled_call_on_exception
        def test_func():
            self.call_count += 1
            if self.call_count < 3:
                raise SupportedThrottleError("this is some bogus error message for testing")

        test_func()
        self.assertEqual(expected_call_count, self.call_count)
        self.assertEqual(expected_call_count - 1, mock_sleep.call_count)

    @patch('time.sleep', return_value=None)
    def test_throttled_call_on_exception_dec_none_param_func_exc_timeout(self, mock_sleep):
        """
        Test throttled_call_on_exception, when decorator has None param, func has exception, times out
        and raise exception
        """

        self.call_count = 0
        mock_sleep.reset_mock()

        @throttled_call_on_exception
        def test_func():
            self.call_count += 1
            raise SupportedThrottleError("exception type doesn't matter b/c everything is throttled")

        self.assertRaises(SupportedThrottleError, test_func)
        # it's impossible to predict the exact number of call counts (throttles) before a timeout
        # b/c of the random nature of Jitter.backoff() in throttled_call_on_exceptions.
        # But it's safe to assume, based on current Jitter default configuration and usage, that even
        # in the most extreme case the call count (throttle) will be at least 5 before a timeout.
        self.assertGreaterEqual(self.call_count, 5)
        self.assertGreaterEqual(mock_sleep.call_count, 4)
        self.assertEqual(self.call_count - 1, mock_sleep.call_count)

    @patch('time.sleep', return_value=None)
    def test_throttled_call_on_exception_dec_param_func_no_exc(self, mock_sleep):
        """Test throttled_call_on_exception, when decorator has param, func has no exception"""

        expected_call_count = 1

        self.call_count = 0
        mock_sleep.reset_mock()

        def dont_throttle_exception(err):
            return False

        @throttled_call_on_exception(is_throttled_exception=dont_throttle_exception)
        def test_func():
            self.call_count += 1

        test_func()
        self.assertEqual(expected_call_count, self.call_count)
        mock_sleep.assert_not_called()

    @patch('time.sleep', return_value=None)
    def test_throttled_call_on_exception_dec_param_func_exc_throttles(self, mock_sleep):
        """
        Test throttled_call_on_exception, when decorator has param, func has exception, throttles
        and executes
        """

        expected_call_count = 3

        self.call_count = 0
        mock_sleep.reset_mock()

        def throttle_some_exception(err):
            if issubclass(err.__class__, SupportedThrottleError) and "some bogus error" in str(err):
                return True
            return False

        @throttled_call_on_exception(is_throttled_exception=throttle_some_exception)
        def test_func():
            self.call_count += 1
            if self.call_count < 3:
                raise SupportedThrottleError("this is some bogus error message for testing")

        test_func()
        self.assertEqual(expected_call_count, self.call_count)
        self.assertEqual(expected_call_count - 1, mock_sleep.call_count)

    @patch('time.sleep', return_value=None)
    def test_throttled_call_on_exception_dec_param_func_exc_timeout(self, mock_sleep):
        """
        Test throttled_call_on_exception, when decorator has param, func has exception, times out
        and raise exception
        """

        self.call_count = 0
        mock_sleep.reset_mock()

        def throttle_some_exception(err):
            if issubclass(err.__class__, SupportedThrottleError) and "some bogus error" in str(err):
                return True
            return False

        @throttled_call_on_exception(is_throttled_exception=throttle_some_exception)
        def test_func():
            self.call_count += 1
            raise SupportedThrottleError("this is some bogus error message for testing")

        self.assertRaises(SupportedThrottleError, test_func)
        # it's impossible to predict the exact number of call counts (throttles) before a timeout
        # b/c of the random nature of Jitter.backoff() in throttled_call_on_exceptions.
        # But it's safe to assume, based on current Jitter default configuration and usage, that even
        # in the most extreme case the call count (throttle) will be at least 5 before a timeout.
        self.assertGreaterEqual(self.call_count, 5)
        self.assertGreaterEqual(mock_sleep.call_count, 4)
        self.assertEqual(self.call_count - 1, mock_sleep.call_count)

    @patch('time.sleep', return_value=None)
    def test_throttled_call_on_exception_dec_param_func_exc_raises(self, mock_sleep):
        """
        Test throttled_call_on_exception, when decorator has param, func has exception, immediately raises
        b/c exception doesn't meet throttle criteria
        """

        expected_call_count = 1

        self.call_count = 0
        mock_sleep.reset_mock()

        def throttle_some_exception(err):
            return issubclass(err.__class__, SupportedThrottleError)

        @throttled_call_on_exception(is_throttled_exception=throttle_some_exception)
        def test_func():
            self.call_count += 1
            raise UnSupportedThrottleError("an unsupported exception for testing")

        self.assertRaises(UnSupportedThrottleError, test_func)
        self.assertEqual(expected_call_count, self.call_count)
        mock_sleep.assert_not_called()

    @patch('boto3.resource')
    @patch('time.sleep', return_value=None)
    def test_wait_for_state_noerr(self, mock_sleep, mock_resource):
        """Test wait_for_state with no error"""
        setattr(mock_resource, 'status', 'available')
        wait_for_state(mock_resource, 'available', state_attr='status', timeout=30)
        self.assertEqual(1, mock_resource.update.call_count)

    @patch('boto3.resource')
    @patch('time.sleep', return_value=None)
    def test_wait_for_state_timeout(self, mock_sleep, mock_resource):
        """Test wait_for_state with timeout"""
        setattr(mock_resource, 'status', 'mystatus')
        self.assertRaises(TimeoutError, wait_for_state, mock_resource, 'available',
                          state_attr='status', timeout=30)

    @patch('boto3.resource')
    @patch('time.sleep', return_value=None)
    def test_wait_for_state_expected_timeout(self, mock_sleep, mock_resource):
        """Test wait_for_state with expected timeout"""
        setattr(mock_resource, 'status', 'failed')
        self.assertRaises(ExpectedTimeoutError, wait_for_state, mock_resource, 'available',
                          state_attr='status', timeout=30)
        self.assertEqual(1, mock_resource.update.call_count)

        setattr(mock_resource, 'status', 'terminated')
        self.assertRaises(ExpectedTimeoutError, wait_for_state, mock_resource, 'available',
                          state_attr='status', timeout=30)
        self.assertEqual(2, mock_resource.update.call_count)

    @patch('boto3.resource')
    @patch('time.sleep', return_value=None)
    def test_wait_for_state_ec2error(self, mock_sleep, mock_resource):
        """Test wait_for_state using EC2ResponseError and timeout"""
        setattr(mock_resource, 'status', 'mystatus')
        mock_resource.update.side_effect = EC2ResponseError("mystatus", "test")
        self.assertRaises(TimeoutError, wait_for_state, mock_resource, 'available', state_attr='status',
                          timeout=30)

    @patch('boto3.resource')
    @patch('time.sleep', return_value=None)
    def test_wait_for_state_error(self, mock_sleep, mock_resource):
        """Test wait_for_state using RuntimeError and returned Exception"""
        setattr(mock_resource, 'status', 'mystatus')
        mock_resource.update.side_effect = RuntimeError
        self.assertRaises(RuntimeError, wait_for_state, mock_resource, 'available', state_attr='status',
                          timeout=30)
        self.assertEqual(1, mock_resource.update.call_count)

    @patch('time.sleep', return_value=None)
    def test_wait_for_state_boto3_noerr(self, mock_sleep):
        """Test wait_for_state_boto3 with no error"""
        mock_describe_func = MagicMock(return_value={"myresource": {"status": "available"}})
        wait_for_state_boto3(mock_describe_func, {"param1": "p1"}, "myresource", 'available',
                             state_attr='status', timeout=30)
        self.assertEqual(1, mock_describe_func.call_count)

    @patch('time.sleep', return_value=None)
    def test_wait_for_state_boto3_timeout(self, mock_sleep):
        """Test wait_for_state_boto3 with timeout"""
        mock_describe_func = MagicMock(return_value={"myresource": {"status": "mystatus"}})
        self.assertRaises(TimeoutError, wait_for_state_boto3, mock_describe_func, {"param1": "p1"},
                          "myresource", 'available', state_attr='status', timeout=30)

    @patch('time.sleep', return_value=None)
    def test_wait_for_state_boto3_exp_timeout(self, mock_sleep):
        """Test wait_for_state_boto3 with ExpectedTimeout"""
        mock_describe_func = MagicMock(return_value={"myresource": {"status": "failed"}})
        self.assertRaises(ExpectedTimeoutError, wait_for_state_boto3, mock_describe_func, {"param1": "p1"},
                          "myresource", 'available', state_attr='status', timeout=30)
        self.assertEqual(1, mock_describe_func.call_count)

        mock_describe_func = MagicMock(return_value={"myresource": {"status": "terminated"}})
        self.assertRaises(ExpectedTimeoutError, wait_for_state_boto3, mock_describe_func, {"param1": "p1"},
                          "myresource", 'available', state_attr='status', timeout=30)
        self.assertEqual(1, mock_describe_func.call_count)

    @patch('time.sleep', return_value=None)
    def test_wait_for_state_boto3_clienterror(self, mock_sleep):
        """Test wait_for_state_boto3 with ClientError and returned Timeout"""
        mock_describe_func = MagicMock()
        error_response = {"Error": {"Code": "MyError"}}
        mock_describe_func.side_effect = ClientError(error_response, "test")
        self.assertRaises(TimeoutError, wait_for_state_boto3, mock_describe_func, {"param1": "p1"},
                          "myresource", 'available', state_attr='status', timeout=30)

    @patch('time.sleep', return_value=None)
    def test_wait_for_state_boto3_ec2error(self, mock_sleep):
        """Test wait_for_state_boto3 with EC2ResponseError and returned Timeout"""
        mock_describe_func = MagicMock()
        mock_describe_func.side_effect = EC2ResponseError("mystatus", "test")
        self.assertRaises(TimeoutError, wait_for_state_boto3, mock_describe_func, {"param1": "p1"},
                          "myresource", 'available', state_attr='status', timeout=30)

    @patch('time.sleep', return_value=None)
    def test_wait_for_state_boto3_error(self, mock_sleep):
        """Test wait_for_state_boto3 with RuntimeError and returned RuntimeError"""
        mock_describe_func = MagicMock()
        mock_describe_func.side_effect = RuntimeError
        self.assertRaises(RuntimeError, wait_for_state_boto3, mock_describe_func, {"param1": "p1"},
                          "myresource", 'available', state_attr='status', timeout=30)
        self.assertEqual(1, mock_describe_func.call_count)

    @patch('amplify_aws_utils.resource_helper.wait_for_state')
    @patch('time.sleep', return_value=None)
    def test_wait_for_sshable_noerr(self, mock_sleep, mock_wait_for_state):
        """Test wait_for_sshable with no error"""
        mock_remote_cmd = MagicMock(return_value=[0])
        wait_for_sshable(mock_remote_cmd, self.mock_instance(), 30)
        self.assertEqual(1, mock_remote_cmd.call_count)

    @patch('amplify_aws_utils.resource_helper.wait_for_state')
    @patch('time.sleep', return_value=None)
    def test_wait_for_sshable_timeout(self, mock_sleep, mock_wait_for_state):
        """Test wait_for_sshable with timeout"""
        mock_remote_cmd = MagicMock(return_value=[1])
        self.assertRaises(TimeoutError, wait_for_sshable, mock_remote_cmd, self.mock_instance(), 30)

    def test_dynamodb_record_to_dict(self):
        """Test dynamodb_record_to_dict happy"""
        mock_record = {
            "foo": {
                "S": "bar",
            },
            "baz": {
                "N": "100",
            }
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
