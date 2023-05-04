import base64
import json
import unittest

from unittest import mock
from unittest.mock import patch, call

from main import document_initializer


mock_document = {"entities": [
    {"textAnchor": {"textSegments": [{"startIndex": "772", "endIndex": "779"}], "content": "Hans"},
     "type": "vorname", "mentionText": "Hans", "confidence": 0.94856465,
     "pageAnchor": {"pageRefs": [{
         "boundingPoly": {
             "normalizedVertices": [
                 {
                     "x": 0.7483641,
                     "y": 0.3425809},
                 {
                     "x": 0.8816181,
                     "y": 0.3425809},
                 {
                     "x": 0.8816181,
                     "y": 0.36906263},
                 {
                     "x": 0.7483641,
                     "y": 0.36906263}],
             "vertices": []},
         "page": "0",
         "layoutType": 0,
         "layoutId": "",
         "confidence": 0.0}]},
     "id": "0", "mentionId": "", "properties": [], "redacted": False}
]}


@patch("google.cloud.firestore.Client")
class TestDocumentInitializer(unittest.TestCase):
    """
    Document extractor test suite
    """

    @patch("google.cloud.pubsub.PublisherClient")
    @patch("google.cloud.storage.Client")
    def test_document_initializer(self, mock_storage_client, mock_pubsub_document, mock_firestore_client):
        mock_context = mock.Mock()
        mock_context.event_id = "617187464464135194"
        mock_context.timestamp = "2022-07-15T22:09:03.761Z"
        mock_context.resource = {
            'name': 'projects/my-project/topics/my-topic',
            'service': 'pubsub.googleapis.com',
            'type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage',
        }

        json_obj = {
            "file_name": "file123.pdf",
        }

        mock_event = {'data': base64.b64encode(json.dumps(json_obj).encode("utf-8"))}

        document_initializer(mock_context, mock_event)



