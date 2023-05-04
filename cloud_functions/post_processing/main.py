import base64
import json
import re
import os

import requests

from utils.logging import logger
from google.cloud import firestore, storage

input_bucket = os.environ["INPUT_BUCKET"]
archive_bucket = os.environ["ARCHIVE_BUCKET"]
output_bucket = os.environ["OUTPUT_BUCKET"]

# Google Maps API Key
MAPS_API_KEY = os.environ["MAPS_API_KEY"]


def post_processing(event: dict = None, context: dict = None):
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

    if "data" in event:
        data = json.loads(base64.b64decode(event["data"]).decode("utf-8"))
        file_name = data["file_name"]
    else:
        raise Exception("No data in event")

    firestore_client = firestore.Client()
    firestore_document_ref = firestore_client.collection("metadata").document(
        file_name[:-4]
    )
    firestore_document = firestore_document_ref.get()
    if firestore_document.exists:
        firestore_document_dict = firestore_document.to_dict()
    else:
        raise Exception("Document does not exist")

    # Firestore document update with postprocessing results
    firestore_document_ref.update(
        {
            "entity_extraction_result.vorname": post_process_names(
                firestore_document_dict["entity_extraction_result"]["vorname"]
            ),
            "entity_extraction_result.nachname": post_process_names(
                firestore_document_dict["entity_extraction_result"]["nachname"]
            ),
            "entity_extraction_result.abnahmestelle": post_process_address(
                firestore_document_dict["entity_extraction_result"]["abnahmestelle"]
            ),
            "entity_extraction_result.adresse": post_process_address(
                firestore_document_dict["entity_extraction_result"]["adresse"]
            ),
            "entity_extraction_result.zaehlerstand": post_process_zaehlerstand(
                firestore_document_dict["entity_extraction_result"]["zaehlerstand"]
            ),
        }
    )

    # Firestore export
    firestore_document = firestore_document_ref.get()
    firestore_document_dict = firestore_document.to_dict()

    # json export: firestore -> gasag-output
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(output_bucket)
    blob = bucket.blob(file_name[:-4] + ".json")
    blob.upload_from_string(
        json.dumps(firestore_document_dict, indent=2).encode("utf-8")
    )

    # pdf export: gasag-input -> gasag-archive
    storage_client = storage.Client()
    source_bucket = storage_client.bucket(input_bucket)
    source_blob = source_bucket.blob(file_name)
    destination_bucket = storage_client.bucket(archive_bucket)

    blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, file_name)
    source_blob.delete()

    logger.info(
        "Blob {} in bucket {} moved to blob_event {} in bucket {}.".format(
            source_blob.name,
            source_bucket.name,
            blob_copy.name,
            destination_bucket.name,
        )
    )


def post_process_zaehlerstand(zaehlerstand_list: list) -> list:
    """
    Post processes the zaehlerstand.
    """
    formatted_zaehlerstand = []

    for zaehlerstand in zaehlerstand_list:
        formatted_zaehlerstand.append(
            {
                "value": re.sub("[^0-9,]", "", zaehlerstand["value"]),
                "confidence": zaehlerstand["confidence"],
            }
        )

    return formatted_zaehlerstand


def post_process_address(address_list: list) -> list:
    """
    Post processes the address, send request to Google Maps API and return the result, if any found.
    If no result found, return the initial extracted value for the address entity.
    """
    formatted_addresses = []
    for address in address_list:
        geo_address = get_geo_address(address["value"])
        if geo_address:
            formatted_addresses.append(
                {
                    "value": get_geo_address(address["value"])
                    .replace("straße", "str.")
                    .replace("ß", "ss"),
                    "confidence": address["confidence"],
                    "raw_value": address["value"],
                }
            )
        else:
            formatted_addresses.append(
                {"value": address["value"], "confidence": address["confidence"]}
            )
    return formatted_addresses


def post_process_names(names_list: list) -> list:
    """
    Post processes the names, filter redundant names and keep only the occurrence with the highest confidence.

    :param names_list:
    :return:
    """
    result = []
    for i, name in enumerate(names_list):
        index = _check_name_in_list(name["value"].lower(), result)
        if index is False:
            result.append(
                {"value": name["value"].lower(), "confidence": name["confidence"]}
            )
        elif name["confidence"] > result[index]["confidence"]:
            result[index]["confidence"] = name["confidence"]
    return result


def _check_name_in_list(name: str, names_list: list) -> int or bool:
    """
    Helper function for function "post_process_names".

    Checks if a name is already in the result list, which has the follwing structure:
    names_list = [{
        "value": "susanne",
        "confidence": 94.2
    },
    {...},
    ...]

    :param name:
    :param names_list:
    :return:
    """
    for index, name_ in enumerate(names_list):
        if name == name_["value"].lower():
            return index
    return False


def get_geo_address(address: str) -> str or None:
    """
    Function that calls the Google Maps Places Api to get formatted address
    :param address: unformatted address
    :return: formatted address or None
    """
    query = address
    url = f"https://maps.googleapis.com/maps/api/place/textsearch/json?query={query}&key={MAPS_API_KEY}"

    payload, headers = {}, {}

    response = requests.request("GET", url, headers=headers, data=payload)
    results = response.json()["results"]
    if results:
        new_address = results[0]["formatted_address"]
        logger.info(f"Google Maps API found address: '{address}' -> '{new_address}'.")
        return new_address
    else:
        logger.info(f"No address found by Google Maps API for '{address}'.")
        return None
