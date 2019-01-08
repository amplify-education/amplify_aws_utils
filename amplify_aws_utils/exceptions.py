"""
Container for amplify_aws_utils exceptions
"""


class TimeoutError(RuntimeError):
    """Error raised on timeout"""


class ExpectedTimeoutError(TimeoutError):
    """
    Error raised in situations where we decide to terminate
    the keep-trying loop early because we have learned that
    the chance of success is 0%
    """


class S3WritingError(RuntimeError):
    """S3 object is not written correctly"""
