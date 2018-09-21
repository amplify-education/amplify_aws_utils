"""Contains Jitter class"""
import time
from random import randint


class Jitter:
    """
    This class implements the logic to run an AWS command using Backoff with Decorrelated Jitter.
    The logic is based on the following article:
    https://www.awsarchitectureblog.com/2015/03/backoff.html
    """
    MAX_POLL_INTERVAL = 60  # seconds

    def __init__(self, min_wait=3):
        self._time_passed = 0
        self._min_wait = min_wait
        self._previous_interval = 0

    def backoff(self):
        """
        Uses a slightly modified version of the Decorrelated Jitter function as described in the AWS blog

        The main change is:
            A random value is chosen from 0 and min(max_poll_interval, prev_value * 3)
            This is different than min(max_poll_interval, rand(0, prev_value * 3) as defined in the blog
            We chose to do this to make sure we continue to get random backoff values instead of
            constantly returning the max value once enough time has passed
        """
        new_interval = randint(0, min(Jitter.MAX_POLL_INTERVAL, self._previous_interval * 3))
        new_interval = max(self._min_wait, new_interval)

        time.sleep(new_interval)
        self._time_passed += new_interval
        self._previous_interval = new_interval
        return self._time_passed
