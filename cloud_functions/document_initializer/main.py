from google.cloud import firestore, storage, pubsub_v1 as pubsub
from datetime import datetime
import hashlib
import json
import os

from utils.logging import logger

project_id = os.environ["GCP_PROJECT"]
input_bucket = os.environ["INPUT_BUCKET"]
topic_name = os.environ["TOPIC_NAME"]
processing_bucket = os.environ["PROCESSING_BUCKET"]


def initialize_firestore(firestore_client: firestore.Client, id: str, filename: str):
    """
    Initialize firestore with metadata

    :param firestore_client:
    :param id:
    :param filename:
    :return:
    """
    metadata = {"unique_filename": f"{id}.pdf", "initial_input_filename": filename}

    # Add a new doc in collection
    data = firestore_client.collection("metadata").document(id)
    data.set(metadata, merge=False)

    logger.info(f"created metadata for document {filename}")


def document_initializer(data=None, context: dict = None):
    """
    Cloud Function to initialize documents in the cloud storage.
    - creates a new unique id for each document with an 8-character hash of the filename and the current timestamp
    - renames the file in the cloud storage to the unique id
    - creates a new document in the metadata collection with the name and id of the file

    """
    logger.info(f"document_initializer was called with {data}")

    firestore_client = firestore.Client()
    storage_client = storage.Client()
    bucket_input = storage_client.get_bucket(input_bucket)
    bucket_processing = storage_client.get_bucket(processing_bucket)

    # check file format being PDF and whether file has format of already processed/renamed files
    if (filename := data["name"]).lower().endswith(".pdf"):
        try:
            datetime.fromisoformat(filename[9:-4])
        except ValueError:
            pass
        else:
            already_processed_error = f"{filename} was already processed."
            logger.info(already_processed_error)
            return already_processed_error
    else:
        pdf_error_message = (
            f"{filename} does not have the type PDF and won't be processed."
        )
        logger.warning(pdf_error_message)
        return pdf_error_message

    # create unique hash id
    id = f'{hashlib.sha256(filename.encode("utf-8")).hexdigest()[:8]}_{datetime.now().isoformat()}'

    # write metadata to firestore
    initialize_firestore(firestore_client=firestore_client, id=id, filename=filename)

    # move blob to processing bucket
    new_file_name = f"{id}.pdf"
    source_blob = bucket_input.get_blob(filename)
    blob_copy = bucket_input.copy_blob(
        blob=source_blob, destination_bucket=bucket_processing, new_name=new_file_name
    )
    source_blob.delete()

    # # # Send pub/sub message to trigger classifier
    publisher = pubsub.PublisherClient()

    message = {"file_name": new_file_name, "firestore_collection_name": "metadata"}

    data = json.dumps(message, indent=2)
    logger.info(f"document_initializer serialized message: {data}")

    # Construct the Pub/Sub topic path and publish
    topic_path = publisher.topic_path(project_id, topic_name)
    future = publisher.publish(topic_path, data=data.encode("utf-8"))

    logger.info(f"document_initializer published {data} to {topic_path}")

    return "Success"
