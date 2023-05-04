import base64
import json
import unittest

from unittest import mock
from unittest.mock import patch, call
from google.cloud import documentai_v1 as documentai

from main import document_extractor, process_document

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
@patch("google.cloud.pubsub.PublisherClient")
@patch("google.cloud.storage.Client")
class TestDocumentExtractor(unittest.TestCase):
    """
    Document extractor test suite
    """

    @patch("main.process_document")
    def test_document_extractor(self, mock_process_document, mock_storage_client, mock_pubsub_document,
                                mock_firestore_client):
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

        mock_response = documentai.Document.from_json(json.dumps(mock_document))

        mock_process_document.return_value = (
            mock_response.entities, documentai.HumanReviewStatus.State.VALIDATION_PASSED)
        document_extractor(mock_event, mock_context)

        bucket = mock_storage_client().get_bucket
        bucket.assert_has_calls([call('gasag-input')])
        bucket.assert_has_calls([call('gasag-archive')])

        blob = bucket().blob
        blob.assert_has_calls([call(json_obj["file_name"]), call().download_as_bytes()])

    @patch("google.cloud.documentai_v1.DocumentProcessorServiceClient")
    def test_process_document(self, mock_docai_client, mock_storage_client, mock_pubsub_document,
                              mock_firestore_client):
        mock_pdf = str.encode("test")

        mock_process_response = {
            "human_review_status": {
                "state": "VALIDATION_PASSED"
            },
            "document": mock_document
        }

        mock_docai_client().process_document.return_value = documentai.ProcessResponse.from_json(
            json.dumps(mock_process_response))

        entities, human_review_status = process_document(mock_pdf)

        assert entities[0].type == "vorname"
        assert entities[0].mention_text == "Hans"
        assert human_review_status == documentai.HumanReviewStatus.State.VALIDATION_PASSED
