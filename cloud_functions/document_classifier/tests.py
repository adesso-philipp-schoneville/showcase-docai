import base64
import json
import unittest

from unittest import mock
from unittest.mock import patch, call
from google.cloud import documentai_v1 as documentai

from main import document_classifier, process_document


@patch("google.cloud.firestore.Client")
@patch("google.cloud.pubsub.PublisherClient")
@patch("google.cloud.storage.Client")
class TestDocumentClassifier(unittest.TestCase):
    """
    Document classifier test suite
    """

    @patch("main.process_document")
    def test_document_classifier(self, mock_process_document, mock_storage_client, mock_pubsub_document,
                                 mock_firestore_client):
        json_obj = {
            "file_name": "file123.pdf",
        }

        mock_context = mock.Mock()
        mock_context.event_id = '617187464135194'
        mock_context.timestamp = '2019-07-15T22:09:03.761Z'
        mock_context.resource = {
            'name': 'projects/my-project/topics/my-topic',
            'service': 'pubsub.googleapis.com',
            'type': 'type.googleapis.com/google.pubsub.v1.PubsubMessage',
        }
        mock_event = {'data': base64.b64encode(json.dumps(json_obj).encode("utf-8"))}

        mock_entity = documentai.Document.Entity
        mock_entity.type_ = "Zaehlerstand_fuer_Einzuege"
        mock_entity.confidence = 0.7364181923866272

        mock_process_document.return_value = ([mock_entity], documentai.HumanReviewStatus.State.VALIDATION_PASSED)
        document_classifier(mock_event, mock_context)

        bucket = mock_storage_client().get_bucket
        bucket.assert_has_calls([call("gasag-input")])
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
            "document": {
                "entities": [
                    {
                        "confidence": 0.7364181923866272,
                        "type_": "Zaehlerstand_fuer_Einzuege"
                    },
                    {
                        "confidence": 0.26358180761,
                        "type_": "Zaehlerstand_fuer_Auszuege"
                    }
                ]
            }
        }

        mock_docai_client().process_document.return_value = documentai.ProcessResponse.from_json(
            json.dumps(mock_process_response))

        entities, human_review_status = process_document(mock_pdf)

        assert entities[0].type_ == "Zaehlerstand_fuer_Einzuege"
        assert human_review_status == documentai.HumanReviewStatus.State.VALIDATION_PASSED
