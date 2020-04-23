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
    ThrottledException,
    throttled_call,
    throttled_call_with_exceptions,
    wait_for_state,
    wait_for_state_boto3,
    wait_for_sshable,
    dynamodb_record_to_dict,
)


class TestThrottleError(RuntimeError):
    """Exception for testing throttled_call_with_exceptions"""
    pass


DEFAULT_TEST_THROTTLE_ERROR = TestThrottleError("some test Exception message")
DEFAULT_RUNTIME_ERROR = RuntimeError("some test Exception message")

DEFAULT_THROTTLED_EXCEPTION_WITH_CORRECT_REGEX = ThrottledException(
    exception_class=TestThrottleError,
    error_message_regexes=["test Exception"]
)
DEFAULT_THROTTLED_EXCEPTION_WITH_WRONG_REGEX = ThrottledException(
    exception_class=TestThrottleError,
    error_message_regexes=["wrong Exception message"]
)
DEFAULT_THROTTLED_EXCEPTION_WITH_MIXED_REGEX = ThrottledException(
    exception_class=TestThrottleError,
    error_message_regexes=["wrong Exception message", "test Exception"]
)
DEFAULT_THROTTLED_EXCEPTION_WITH_WRONG_EXC = ThrottledException(
    exception_class=TypeError,
    error_message_regexes=["test Exception"]
)


# time.sleep is being patched but not referenced.
# pylint: disable=W0613
class ResourceHelperTests(TestCase):
    """Test Resource Helper"""

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

    def test_throttled_exception_init_good_param(self):
        """Test ThrottledException, handles good params to __init__"""

        # test valid val for param exception_class, doesn't raise Exception
        ThrottledException(exception_class=RuntimeError)
        ThrottledException(exception_class=TestThrottleError)

        # test valid val for param error_message_regexes, doesn't raise Exception
        ThrottledException(exception_class=RuntimeError,
                           error_message_regexes=None)
        ThrottledException(exception_class=RuntimeError,
                           error_message_regexes=[])
        ThrottledException(exception_class=RuntimeError,
                           error_message_regexes=["valid val"])
        ThrottledException(exception_class=RuntimeError,
                           error_message_regexes=("valid val", "another valid val"))
        ThrottledException(exception_class=RuntimeError,
                           error_message_regexes=set(["valid val", "another valid val"]))

    def test_throttled_exception_init_bad_param(self):
        """Test ThrottledException, handles bad params to __init__"""

        # test invalid val for param exception_class
        self.assertRaises(TypeError, ThrottledException, exception_class=str)
        self.assertRaises(TypeError, ThrottledException, exception_class=None)

        # test invalid val for param error_message_regexes
        self.assertRaises(TypeError, ThrottledException, exception_class=RuntimeError,
                          error_message_regexes="invalid val")
        self.assertRaises(TypeError, ThrottledException, exception_class=RuntimeError,
                          error_message_regexes=RuntimeError("another invalid val"))
        self.assertRaises(TypeError, ThrottledException, exception_class=RuntimeError,
                          error_message_regexes=[RuntimeError("another invalid val")])
        self.assertRaises(TypeError, ThrottledException, exception_class=RuntimeError,
                          error_message_regexes=["valid str", RuntimeError("another invalid val")])

    def test_throttled_exception_matches_success(self):
        """Test ThrottledException, handles successful call to matches"""

        # test match when there's no error_message_regexes
        te = ThrottledException(exception_class=RuntimeError)
        self.assertTrue(te.matches(DEFAULT_TEST_THROTTLE_ERROR))
        self.assertTrue(te.matches(DEFAULT_RUNTIME_ERROR))

        te = ThrottledException(exception_class=RuntimeError,
                                error_message_regexes=[])
        self.assertTrue(te.matches(DEFAULT_TEST_THROTTLE_ERROR))
        self.assertTrue(te.matches(DEFAULT_RUNTIME_ERROR))

        te = ThrottledException(exception_class=RuntimeError,
                                error_message_regexes=None)
        self.assertTrue(te.matches(DEFAULT_TEST_THROTTLE_ERROR))
        self.assertTrue(te.matches(DEFAULT_RUNTIME_ERROR))

        # test match when there's error_message_regexes
        te = ThrottledException(exception_class=RuntimeError,
                                error_message_regexes=["some val"])
        self.assertTrue(te.matches(RuntimeError("has 'some val' in message")))

        te = ThrottledException(exception_class=RuntimeError,
                                error_message_regexes=["some val", "another val", "yet a final val"])
        self.assertTrue(te.matches(RuntimeError("has 'some val' in message")))
        self.assertTrue(te.matches(RuntimeError("has another val in message")))
        self.assertTrue(te.matches(RuntimeError("has yet a final val in message")))
        self.assertTrue(te.matches(TestThrottleError("this is a subclass of RuntimeError and has some val in it")))

        te = ThrottledException(exception_class=RuntimeError,
                                error_message_regexes=["some .* message"])
        self.assertTrue(te.matches(RuntimeError("has 'some val' in message")))

        te = ThrottledException(exception_class=RuntimeError,
                                error_message_regexes=["some .* message", "another .* message", "yet a final .* message"])
        self.assertTrue(te.matches(RuntimeError("has 'some val' in message")))
        self.assertTrue(te.matches(RuntimeError("has another val in message")))
        self.assertTrue(te.matches(RuntimeError("has yet a final val in message")))
        self.assertTrue(te.matches(TestThrottleError("this is a subclass of RuntimeError and has some val in message")))

    def test_throttled_exception_matches_fail(self):
        """Test ThrottledException, handles failed call to matches"""

        # test there's no match when there's no error_message_regexes
        te = ThrottledException(exception_class=RuntimeError)
        # some bogus inputs
        self.assertFalse(te.matches(None))
        self.assertFalse(te.matches(""))
        self.assertFalse(te.matches(1))
        # not a subclass of RuntimeError
        self.assertFalse(te.matches(SystemError("has some val for err message")))

        # test there's no match when there's error_message_regexes
        te = ThrottledException(exception_class=RuntimeError,
                                error_message_regexes=["some val"])
        # not a subclass of RuntimeError
        self.assertFalse(te.matches(SystemError("has some val for err message")))
        # no matching error message
        self.assertFalse(te.matches(RuntimeError("has no matching err message")))

        te = ThrottledException(exception_class=RuntimeError,
                                error_message_regexes=["some .* message"])
        self.assertFalse(te.matches(RuntimeError("has 'wrong val' in message that doesn't match regex")))

        te = ThrottledException(exception_class=RuntimeError,
                                error_message_regexes=["some .* message", "another .* message", "yet a final .* message"])
        self.assertFalse(te.matches(RuntimeError("has 'wrong val' in message")))
        self.assertFalse(te.matches(RuntimeError("has another val in mssg")))
        self.assertFalse(te.matches(RuntimeError("has yet a wrong final val in message")))
        self.assertFalse(te.matches(TestThrottleError("this is a subclass of RuntimeError and has wrong val in message")))

    @patch('time.sleep', return_value=None)
    def test_throttled_call_with_exceptions_handled_exception(self, mock_sleep):
        """Test throttled_call_with_exceptions, throttles and doesn't raise Exception, when function raises Exception"""

        te_with_correct_regex = DEFAULT_THROTTLED_EXCEPTION_WITH_CORRECT_REGEX
        te_with_wrong_regex = DEFAULT_THROTTLED_EXCEPTION_WITH_WRONG_REGEX
        te_with_mixed_regex = DEFAULT_THROTTLED_EXCEPTION_WITH_MIXED_REGEX
        te_with_wrong_exc = DEFAULT_THROTTLED_EXCEPTION_WITH_WRONG_EXC

        throttled_excs_test_cases = [
            [te_with_correct_regex],
            [te_with_mixed_regex],
            [te_with_mixed_regex, te_with_wrong_regex],
            [te_with_wrong_regex, te_with_correct_regex],
            [te_with_wrong_exc, te_with_correct_regex],
            [te_with_wrong_exc, te_with_mixed_regex],
            [te_with_wrong_exc, te_with_mixed_regex, te_with_wrong_exc, te_with_correct_regex]
        ]

        for throttled_excs in throttled_excs_test_cases:
            mock_func = MagicMock()
            mock_error = DEFAULT_TEST_THROTTLE_ERROR
            mock_func.side_effect = [mock_error, mock_error, True]

            throttled_call_with_exceptions(fun=mock_func, throttled_exceptions=throttled_excs)
            self.assertEqual(3, mock_func.call_count)

    @patch('time.sleep', return_value=None)
    def test_throttled_call_with_exceptions_handled_exception_with_timeout(self, mock_sleep):
        """Test throttled_call_with_exceptions, throttles and eventually raises Exception, when function raises handled Exception"""

        te_with_correct_regex = DEFAULT_THROTTLED_EXCEPTION_WITH_CORRECT_REGEX
        te_with_wrong_regex = DEFAULT_THROTTLED_EXCEPTION_WITH_WRONG_REGEX
        te_with_mixed_regex = DEFAULT_THROTTLED_EXCEPTION_WITH_MIXED_REGEX
        te_with_wrong_exc = DEFAULT_THROTTLED_EXCEPTION_WITH_WRONG_EXC

        throttled_excs_test_cases = [
            [te_with_correct_regex],
            [te_with_mixed_regex],
            [te_with_mixed_regex, te_with_wrong_regex],
            [te_with_wrong_regex, te_with_correct_regex],
            [te_with_wrong_exc, te_with_correct_regex],
            [te_with_wrong_exc, te_with_mixed_regex],
            [te_with_wrong_exc, te_with_mixed_regex, te_with_wrong_exc, te_with_correct_regex]
        ]

        for throttled_excs in throttled_excs_test_cases:
            mock_func = MagicMock()
            mock_error = DEFAULT_TEST_THROTTLE_ERROR
            mock_func.side_effect = mock_error

            self.assertRaises(TestThrottleError, throttled_call_with_exceptions, mock_func, throttled_excs)

            # it's impossible to predict the exact number of call counts (throttles) before a timeout
            # b/c of the random nature of Jitter.backoff() in throttled_call_with_exceptions.
            # But it's safe to assume, based on current Jitter default configuration and usage, that even
            # in the most extreme case the call count (throttle) will be at least 5 before a timeout.
            self.assertTrue(mock_func.call_count >= 5)

    @patch('time.sleep', return_value=None)
    def test_throttled_call_with_exceptions_unhandled_exception_with_raise(self, mock_sleep):
        """Test throttled_call_with_exceptions, doesn't throttle and immediately raises Exception, when function raises unhandled Exception"""

        te_with_wrong_regex = DEFAULT_THROTTLED_EXCEPTION_WITH_WRONG_REGEX
        te_with_wrong_exc = DEFAULT_THROTTLED_EXCEPTION_WITH_WRONG_EXC

        throttled_excs_test_cases = [
            None,
            [],
            [te_with_wrong_regex],
            [te_with_wrong_exc],
            [te_with_wrong_regex, te_with_wrong_regex],
            [te_with_wrong_exc, te_with_wrong_regex]
        ]

        for throttled_excs in throttled_excs_test_cases:
            mock_func = MagicMock()
            mock_error = DEFAULT_TEST_THROTTLE_ERROR
            mock_func.side_effect = mock_error

            self.assertRaises(TestThrottleError, throttled_call_with_exceptions, mock_func, throttled_excs)
            self.assertEqual(1, mock_func.call_count)

    @patch('time.sleep', return_value=None)
    def test_throttled_call_with_exceptions_no_exception(self, mock_sleep):
        """Test throttled_call_with_exceptions, doesn't throttle, when function doesn't raise Exception"""

        te_with_correct_regex = DEFAULT_THROTTLED_EXCEPTION_WITH_CORRECT_REGEX
        te_with_wrong_regex = DEFAULT_THROTTLED_EXCEPTION_WITH_WRONG_REGEX
        te_with_mixed_regex = DEFAULT_THROTTLED_EXCEPTION_WITH_MIXED_REGEX
        te_with_wrong_exc = DEFAULT_THROTTLED_EXCEPTION_WITH_WRONG_EXC

        throttled_excs_test_cases = [
            None,
            [],
            [te_with_wrong_regex],
            [te_with_wrong_exc],
            [te_with_correct_regex],
            [te_with_mixed_regex],
            [te_with_mixed_regex, te_with_wrong_regex],
            [te_with_wrong_regex, te_with_correct_regex],
            [te_with_wrong_exc, te_with_correct_regex],
            [te_with_wrong_exc, te_with_mixed_regex],
            [te_with_wrong_exc, te_with_mixed_regex, te_with_wrong_exc, te_with_correct_regex]
        ]

        for throttled_excs in throttled_excs_test_cases:
            mock_func = MagicMock()

            throttled_call_with_exceptions(fun=mock_func, throttled_exceptions=throttled_excs)
            self.assertEqual(1, mock_func.call_count)

    @patch('time.sleep', return_value=None)
    def test_throttled_call_with_exceptions_bad_param(self, mock_sleep):
        """Test throttled_call_with_exceptions, handles bad param"""

        valid_te = DEFAULT_THROTTLED_EXCEPTION_WITH_CORRECT_REGEX

        throttled_excs_test_cases = [
            RuntimeError("invalid type b/c it needs to be an iterable"),
            123,
            "another invalid type b/c it needs to be an iterable",
            valid_te,
            [""],
            [None],
            [-1],
            [0],
            [123],
            [valid_te, ""],
            [valid_te, RuntimeError("invalid obj")],
            [2, valid_te],
            [valid_te, 3, valid_te],
            [valid_te, valid_te, None],
            [valid_te, 4, valid_te, None, valid_te, "yet another invalid obj"]
        ]

        for throttled_excs in throttled_excs_test_cases:
            # test when function doesn't raise Exception
            mock_func = MagicMock()

            self.assertRaises(TypeError, throttled_call_with_exceptions, mock_func, throttled_excs)
            self.assertEqual(0, mock_func.call_count)

            # test when function raises Exception
            mock_func = MagicMock()
            mock_error = DEFAULT_TEST_THROTTLE_ERROR
            mock_func.side_effect = mock_error

            self.assertRaises(TypeError, throttled_call_with_exceptions, mock_func, throttled_excs)
            self.assertEqual(0, mock_func.call_count)

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
