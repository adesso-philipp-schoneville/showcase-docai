import base64
import json
import os

from google.cloud import documentai_v1 as documentai
from google.cloud import storage, pubsub_v1 as pubsub, firestore
from utils.logging import logger

project_id = os.environ["GCP_PROJECT"]
input_bucket = os.environ["INPUT_BUCKET"]
topic_name = os.environ["TOPIC_NAME"]
location = os.environ["LOCATION"]
processor_id_broad = os.environ["CDC_BROAD"]
processor_id_widerruf = os.environ["CDC_WIDERRUF"]
processor_id_zaehlerstand = os.environ["CDC_ZAEHLERSTAND"]


def process_document(
    image_content: bytes, processor_id: str
) -> (list, documentai.HumanReviewStatus.State):
    """
    Function that sends image to custom document processor endpoint.

    :param image_content: Pdf that needs to be classified
    :param processor_id: Processor ID in the form 'projects/{project_id}/locations/{location}/processors/{processor_id}'
    :return: List of entities and HumanReviewStatus
    """
    opts = {}
    if location == "eu":
        opts = {"api_endpoint": "eu-documentai.googleapis.com"}

    client = documentai.DocumentProcessorServiceClient(client_options=opts)

    document = {"content": image_content, "mime_type": "application/pdf"}

    # Configure the process request
    request = {"name": processor_id, "raw_document": document}
    result = client.process_document(request=request)
    human_review_status = result.human_review_status.state
    entities = result.document.entities

    return entities, human_review_status


def document_classifier(event: dict = None, context: dict = None):
    """
    Cloud Function to be triggered by Pub/Sub.

    :param event:   The dictionary with data specific to this type of
                    event. The `@type` field maps to
                    type.googleapis.com/google.pubsub.v1.PubsubMessage`.
                    The `data` field maps to the PubsubMessage data
                    in a base64-encoded string. The `attributes` field maps
                    to the PubsubMessage attributes if any is present
    :param context: Metadata of triggering event
                    including `event_id` which maps to the PubsubMessage
                    messageId, `timestamp` which maps to the PubsubMessage
                    publishTime, `event_type` which maps to
                    `google.pubsub.topic.publish`, and `resource` which is
                    a dictionary that describes the service API endpoint
                    pubsub.googleapis.com, the triggering topic's name, and
                    the triggering event type`type.googleapis.com/google.pubsub.v1.PubsubMessage`.
    :return: None. The output is written to Cloud Logging.
    """

    logger.info(
        """Document classifier was triggered by messageId {} published at {} to {}
    """.format(
            context.event_id, context.timestamp, context.resource["name"]
        )
    )

    if "data" in event:
        data = json.loads(base64.b64decode(event["data"]).decode("utf-8"))
        file_name = data["file_name"]
    else:
        raise Exception("No data in event")

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(input_bucket)

    blob = bucket.blob(file_name)
    file = blob.download_as_bytes()

    document_entities, human_review_status = process_document(
        image_content=file, processor_id=processor_id_broad
    )

    # classification_dict = {
    #     "human_review_status": str(human_review_status).split(".")[-1]
    # }

    # Get intent with max confidence
    intent = max(document_entities, key=lambda entity: entity.confidence).type_
    confidence = max(document_entities, key=lambda entity: entity.confidence).confidence

    logger.info(f"Document classified as: {intent} with {confidence}.")

    classification_dict = {"intent": {"value": intent, "confidence": confidence}}

    if intent == "Zaehlerstand":
        document_entities, human_review_status = process_document(
            image_content=file, processor_id=processor_id_zaehlerstand
        )
        # Get intent with max confidence
        sub_intent = max(document_entities, key=lambda entity: entity.confidence).type_
        sub_confidence = max(
            document_entities, key=lambda entity: entity.confidence
        ).confidence

    elif intent == "Widerruf":
        document_entities, human_review_status = process_document(
            image_content=file, processor_id=processor_id_widerruf
        )
        # Get intent with max confidence
        sub_intent = max(document_entities, key=lambda entity: entity.confidence).type_
        sub_confidence = max(
            document_entities, key=lambda entity: entity.confidence
        ).confidence

    elif intent == "Sonstige":
        sub_intent = None
        sub_confidence = None

    else:
        raise Exception("Unknown intent")

    logger.info(f"Document classified as: {sub_intent} with {sub_confidence}.")

    classification_dict["sub_intent"] = {
        "value": sub_intent,
        "confidence": sub_confidence,
    }

    firestore_client = firestore.Client()
    firestore_document = firestore_client.collection("metadata").document(
        file_name[:-4]
    )
    firestore_document.update({"classification_result": classification_dict})
    logger.info(
        f"wrote classification metadata for document {file_name} into firestore"
    )

    # Send pub/sub message to trigger entity extraction
    publisher = pubsub.PublisherClient()
    message = {"file_name": file_name}
    data = json.dumps(message, indent=2).encode("utf-8")

    # Construct the Pub/Sub topic path and publish
    topic_path = publisher.topic_path(project_id, topic_name)
    future = publisher.publish(topic_path, data=data)
