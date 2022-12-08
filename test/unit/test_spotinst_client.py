"""Module for testing our Spotinst client"""

from unittest import TestCase

from mock import MagicMock, patch
import requests_mock
# pylint: disable=redefined-builtin
from requests.exceptions import ReadTimeout, ConnectTimeout, ConnectionError

from amplify_aws_utils.clients.spotinst import SpotinstClient, SpotinstRateExceededException


class TestSpotinstClient(TestCase):
    """Class for testing Spotinst Client"""

    def setUp(self):
        """Pretest setup"""
        self.session = MagicMock()
        self.spotinst_client = SpotinstClient("", "")

    @requests_mock.mock()
    def test_create_group(self, requests):
        """Test sending create group request"""
        requests.post('https://api.spotinst.io/aws/ec2/group', json={
            'response': {
                'items': [{
                    'name': 'foo'
                }]
            }
        })

        result = self.spotinst_client.create_group({
            'group': {
                'name': 'foo'
            }
        })

        self.assertEqual(len(requests.request_history), 1)
        self.assertEqual(result, {'name': 'foo'})

    @requests_mock.mock()
    def test_update_group(self, requests):
        """Test sending update group request"""
        requests.put('https://api.spotinst.io/aws/ec2/group/sig-5af12785', json={
            'response': {
                'items': [{
                    'group': {
                        'name:': 'foo'
                    }
                }]
            }
        })

        self.spotinst_client.update_group('sig-5af12785', {
            'group': {
                'name': 'foo'
            }
        })

        self.assertEqual(len(requests.request_history), 1)

    @requests_mock.mock()
    def test_get_groups(self, requests):
        """Test sending group list request"""
        requests.get('https://api.spotinst.io/aws/ec2/group', json={
            'response': {
                'items': [{
                    'instanceId': 'i-abcd1234'
                }]
            }
        })

        self.spotinst_client.get_groups()

        self.assertEqual(len(requests.request_history), 1)

    @requests_mock.mock()
    def test_delete_group(self, requests):
        """Test sending delete group request"""
        requests.delete('https://api.spotinst.io/aws/ec2/group/sig-5af12785', json={
            "request": {
                "id": "4a0d5084-0b41-4255-82e5-d64a8232d7cc",
                "url": "/aws/ec2/group/sig-5af12785",
                "method": "DELETE",
                "time": "2015-06-28T15:52:45.772Z"
            },
            "response": {
                "status": {
                    "code": 200,
                    "message": "OK"
                }
            }
        })

        self.spotinst_client.delete_group('sig-5af12785')

        self.assertEqual(len(requests.request_history), 1)

    @requests_mock.mock()
    def test_roll_group(self, requests):
        """Test sending roll group request"""
        requests.put('https://api.spotinst.io/aws/ec2/group/sig-5af12785/roll', json={
            "request": {
                "id": "3213e42e-455e-4901-a185-cc3eb65fac5f",
                "url": "/aws/ec2/group/sig-5af12785/roll",
                "method": "PUT",
                "time": "2016-02-10T15:49:11.911Z"
            },
            "response": {
                "status": {
                    "code": 200,
                    "message": "OK"
                },
                "kind": "spotinst:aws:ec2:group:roll",
            }
        })

        self.spotinst_client.roll_group('sig-5af12785', 100, 100, health_check_type='EC2')

        self.assertEqual(len(requests.request_history), 1)

    @requests_mock.mock()
    def test_get_deployments(self, requests):
        """Test getting a list of deployments for a group"""
        requests.get('https://api.spotinst.io/aws/ec2/group/sig-5af12785/roll', json={
            "request": {
                "id": "3213e42e-455e-4901-a185-cc3eb65fac5f",
                "url": "/aws/ec2/group/sig-5af12785/roll",
                "method": "PUT",
                "time": "2016-02-10T15:49:11.911Z"
            },
            "response": {
                "items": [
                    {
                        "id": "sbgd-c47a527a",
                        "status": "finished",
                        "progress": {
                            "unit": "percent",
                            "value": 100
                        },
                        "createdAt": "2017-05-24T12:12:39.000+0000",
                        "updatedAt": "2017-05-24T12:19:17.000+0000"
                    },
                    {
                        "id": "sbgd-f789ec37",
                        "status": "in_progress",
                        "progress": {
                            "unit": "percent",
                            "value": 0
                        },
                        "createdAt": "2017-05-24T20:13:37.000+0000",
                        "updatedAt": "2017-05-24T20:15:17.000+0000"
                    }],
                "count": 2
            }
        })

        deployments = self.spotinst_client.get_deployments('sig-5af12785')

        self.assertEqual(len(requests.request_history), 1)
        self.assertEqual(len(deployments), 2)

    @requests_mock.mock()
    def test_get_roll_status(self, requests):
        """Test getting the status of a deployment"""
        requests.get('https://api.spotinst.io/aws/ec2/group/sig-5af12785/roll/sbgd-c47a527a', json={
            "request": {
                "id": "3213e42e-455e-4901-a185-cc3eb65fac5f",
                "url": "/aws/ec2/group/sig-5af12785/roll",
                "method": "PUT",
                "time": "2016-02-10T15:49:11.911Z"
            },
            "response": {
                "items": [
                    {
                        "id": "sbgd-c47a527a",
                        "status": "finished",
                        "progress": {
                            "unit": "percent",
                            "value": 100
                        },
                        "createdAt": "2017-05-24T12:12:39.000+0000",
                        "updatedAt": "2017-05-24T12:19:17.000+0000"
                    }],
                "count": 1
            }
        })

        status = self.spotinst_client.get_roll_status('sig-5af12785', 'sbgd-c47a527a')

        self.assertEqual(len(requests.request_history), 1)
        self.assertEqual(status['status'], 'finished')

    # pylint: disable=unused-argument
    @requests_mock.mock()
    @patch("time.sleep", return_value=None)
    def test_throttle_error(self, requests, sleep_mock):
        """Test handling spotinst throttling"""
        requests.get('https://api.spotinst.io/aws/ec2/group', status_code=429)

        self.assertRaises(SpotinstRateExceededException, self.spotinst_client.get_groups)

    # pylint: disable=unused-argument
    @requests_mock.mock()
    @patch("time.sleep", return_value=None)
    def test_timeout_error(self, requests, sleep_mock):
        """Test handling a request timeout"""
        requests.get('https://api.spotinst.io/aws/ec2/group', exc=ReadTimeout)

        self.assertRaises(SpotinstRateExceededException, self.spotinst_client.get_groups)

    # pylint: disable=unused-argument
    @requests_mock.mock()
    @patch("time.sleep", return_value=None)
    def test_retry(self, requests, sleep_mock):
        """Test request keeps retrying until successful"""
        responses = [
            {'status_code': 429},
            {'exc': ReadTimeout},
            {
                'status_code': 400,
                'json': {
                    'request': {
                        'id': 'b4415046-bb2d-4338-9b8a-73a405a6fe0c'
                    },
                    'response': {
                        'status': '',
                        'errors': [{
                            'message': 'Cant validate AMI',
                            'code': 'CANT_VALIDATE_IMAGE'
                        }, {
                            'message': 'Request limit exceeded',
                            'code': 'RequestLimitExceeded'
                        }]
                    }
                }
            },
            {'exc': ConnectTimeout},
            {'exc': ConnectionError},
            {'json': {
                'response': {
                    'items': [{
                        'name': 'foo'
                    }]
                }
            }}
        ]
        requests.get('https://api.spotinst.io/aws/ec2/group', responses)

        groups = self.spotinst_client.get_groups()

        self.assertEqual([{'name': 'foo'}], groups)
