import base64
import json
import unittest

from unittest import mock
from unittest.mock import patch

from main import post_processing, post_process_address, get_geo_address


class TestPostProcessing(unittest.TestCase):
    """
    Test suite for the cloud function template
    """

    @patch("google.cloud.firestore.Client")
    def test_post_processing(self, mock_firestore_client):
        mock_context = mock.Mock()
        mock_context.event_id = "617187464464135194"
        mock_context.timestamp = "2022-07-15T22:09:03.761Z"
        mock_context.resource = {
            'name': 'projects/my-project/topics/my-topic',
            'service': 'pubsub.googleapis.com',
            'type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage',
        }

        json_obj = {
            "file_name": "58e1c48b_2022-07-28T13:07:54.307169.pdf",
        }

        mock_event = {'data': base64.b64encode(json.dumps(json_obj).encode("utf-8"))}
        post_processing(mock_event, mock_context)

    @patch("requests.request")
    def test_get_geo_address(self, mock_request=None):
        mock_request.return_value.status_code.return_value = 200
        mock_request.return_value.json.return_value = {
            'results': [{'formatted_address': 'Auf den Häfen 11, 28203 Bremen, Germany'}]}

        address = get_geo_address("auf den Häfen 11")
        assert address == "Auf den Häfen 11, 28203 Bremen, Germany"

    @patch("main.get_geo_address")
    def test_post_process_address(self, mock_get_geo_address):
        address_list = [{"value": "auf den Häfen 11"}]
        mock_get_geo_address.return_value = 'Auf den Häfen 11, 28203 Bremen, Germany'
        result = post_process_address(address_list)
        assert result == ['Auf den Häfen 11, 28203 Bremen, Germany']
