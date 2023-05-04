import base64
import json
import os
from typing import List

from google.cloud import (
    storage,
    firestore,
    documentai_v1 as documentai,
    pubsub_v1 as pubsub,
)

from utils.logging import logger


project_id = os.environ["GCP_PROJECT"]
input_bucket = os.environ["INPUT_BUCKET"]
location = os.environ["LOCATION"]
archive_bucket = os.environ["ARCHIVE_BUCKET"]
processor_id_cde_allgemein = os.environ["CDE_ALLGEMEIN"]
processor_id_cde_umzugsmitteilung_formular = os.environ["CDE_UMZUG"]
processor_id_cde_widerruf_formular = os.environ["CDE_WIDERRUF"]
topic_name = os.environ["TOPIC_NAME"]


# def get_dummy_entities():
#     return {
#         'Vorname': 'Max',
#         'Nachname': 'Mustermann',
#         'Adresse': {
#             'Straße': 'Musterstraße',
#             'Hausnummer': '1',
#             'PLZ': '12345',
#             'Ort': 'Musterstadt'
#         },
#         'IBAN': 'DE1234567890123456789',
#         'Vertragskontonummer': '123456789',
#         'Zaehlernummer': '123456789',
#         'Zaehlerstand': '123456789',
#         'Datum_der_Ablesung': '01.01.2020',
#     }


def process_document(
    image_content: bytes, processor_id: str
) -> (List[documentai.Document.Entity], documentai.HumanReviewStatus.State):
    """
     Function that sends image to custom document processor endpoint

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


def get_entity_name(text_anchor: documentai.Document.Entity) -> str:
    return text_anchor.type_


def get_entity_confidence(text_anchor: documentai.Document.Entity) -> str:
    return text_anchor.confidence


def get_entity_value(text_anchor: documentai.Document.Entity) -> str:
    return text_anchor.mention_text


def document_extractor(event: dict = None, context: dict = None):
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
        """Document extractor was triggered by messageId {} published at {} to {}
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
    source_bucket = storage_client.get_bucket(input_bucket)

    blob_event = source_bucket.blob(file_name)
    source_blob = blob_event.download_as_bytes()

    firestore_client = firestore.Client()
    firestore_document = firestore_client.collection("metadata").document(
        file_name[:-4]
    )
    classification_dict = firestore_document.get().to_dict()

    intent = classification_dict["classification_result"]["intent"]["value"]
    sub_intent = classification_dict["classification_result"]["sub_intent"]["value"]

    if intent == "Zaehlerstand":
        if sub_intent == "Umzugsmitteilung_Formular":
            document_entities, human_review_status = process_document(
                image_content=source_blob,
                processor_id=processor_id_cde_umzugsmitteilung_formular,
            )
        else:
            document_entities, human_review_status = process_document(
                image_content=source_blob, processor_id=processor_id_cde_allgemein
            )
    elif intent == "Widerruf":
        if sub_intent == "Widerrufsformular":
            document_entities, human_review_status = process_document(
                image_content=source_blob,
                processor_id=processor_id_cde_widerruf_formular,
            )
        elif sub_intent == "Widerrufsschreiben":
            document_entities, human_review_status = process_document(
                image_content=source_blob, processor_id=processor_id_cde_allgemein
            )
    elif intent == "Sonstige":
        document_entities, human_review_status = process_document(
            image_content=source_blob, processor_id=processor_id_cde_allgemein
        )
    else:
        raise Exception("Unknown intent")

    # extraction_dict = {"file_name": file_name, "human_review_status": str(human_review_status).split(".")[-1]}
    extraction_dict = {}

    # extracted labels
    labels = [
        "vorname",
        "nachname",
        "adresse",
        "email",
        "telefon",
        "iban",
        "vertragskontonummer",
        "zaehlernummer",
        "zaehlerstand",
        "ablesedatum",
        "abnahmestelle",
    ]

    # initialize extraction dict with empty arrays for each label
    for label in labels:
        extraction_dict[label] = []

    # iterater over all entities to get name, confidence, value and add them to extraction dict
    for text_anchor in document_entities:
        entity_name = get_entity_name(text_anchor)
        entity_value = get_entity_value(text_anchor)
        entity_confidence = get_entity_confidence(text_anchor)

        # iterate over all labels to check if the entity_name is in the label list
        for label in labels:
            if entity_name.strip().lower() == label:
                extraction_dict[label].append(
                    {"value": entity_value, "confidence": entity_confidence}
                )

    # debug/initial testing: write dummy entities to firestore
    # extraction_dict = get_dummy_entities()

    # update firestore metadata
    firestore_document.update({"entity_extraction_result": extraction_dict})
    logger.info(
        f"wrote entity extraction metadata for document {file_name} into firestore"
    )

    # Send pub/sub message to trigger entity extraction
    publisher = pubsub.PublisherClient()
    message = {"file_name": file_name}
    data = json.dumps(message, indent=2).encode("utf-8")

    # Construct the Pub/Sub topic path and publish
    topic_path = publisher.topic_path(project_id, topic_name)
    future = publisher.publish(topic_path, data=data)
