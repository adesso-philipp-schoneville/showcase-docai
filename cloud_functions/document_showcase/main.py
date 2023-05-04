import datetime
import hashlib
import os
from typing import List

from google.cloud import documentai_v1 as documentai
from google.cloud import firestore, storage
from utils.logging import logger

PROCESSOR_ID_CDS = os.environ["CDS_ID"]
LOCATION = os.environ["LOCATION"]
FIRESTORE_COLLECTION = os.environ["FIRESTORE_COLLECTION"]


def initialize_firestore(
    firestore_client: firestore.Client, id: str, filename: str
) -> firestore.DocumentReference:
    """
    Initialize firestore with metadata

    :param firestore_client:
    :param id:
    :param filename:
    :return:
    """
    metadata = {
        "filename": filename,
        "created": datetime.datetime.now().isoformat(),
        "status": "initialized",
    }

    # Add a new doc in collection
    doc_ref = firestore_client.collection(FIRESTORE_COLLECTION).document(id)
    doc_ref.set(metadata, merge=False)

    logger.info(f"created metadata for document {filename}")
    return doc_ref


def process_document(image_content: bytes, processor_id: str) -> List[str]:
    """
    Function that sends image to custom document processor endpoint.

    :param image_content: Pdf that needs to be classified
    :param processor_id: Processor ID in the form 'projects/{project_id}/locations/{location}/processors/{processor_id}'
    :return: List of entities and HumanReviewStatus
    """
    opts = {}
    if LOCATION == "eu":
        opts = {"api_endpoint": "eu-documentai.googleapis.com"}

    client = documentai.DocumentProcessorServiceClient(client_options=opts)

    document = {"content": image_content, "mime_type": "application/pdf"}

    # Configure the process request
    request = {"name": processor_id, "raw_document": document}
    result = client.process_document(request=request)
    entities = result.document.entities

    return entities


def document_showcase(data=None, context: dict = None) -> str:
    """
    Cloud Function to initialize documents in the cloud storage.
    - creates a new unique id for each document with an 8-character hash of the filename
    and the current timestamp
    - renames the file in the cloud storage to the unique id
    - creates a new document in the metadata collection with the name and id of the file

    """
    logger.info(f"document_showcase was called with {data}")

    firestore_client = firestore.Client()
    storage_client = storage.Client()
    bucket_input = storage_client.get_bucket(data["bucket"])

    # check file format being PDF
    if not (filename := data["name"]).lower().endswith(".pdf"):
        pdf_error_message = (
            f"{filename} does not have the type PDF and won't be processed."
        )
        logger.warning(pdf_error_message)
        return pdf_error_message

    # create unique hash id
    id = hashlib.sha256(
        datetime.datetime.now(datetime.timezone.utc).isoformat().encode()
    ).hexdigest()

    # write metadata to firestore
    doc_ref = initialize_firestore(
        firestore_client=firestore_client, id=id, filename=filename
    )

    # download document
    source_blob = bucket_input.get_blob(filename)
    document = source_blob.download_as_bytes()

    # get split and class with CDS
    document_entities = process_document(
        image_content=document, processor_id=PROCESSOR_ID_CDS
    )

    logger.info(document_entities)
    doc_ref.update({"status": "cds_processed"})

    # extract values for each page

    # remove file
    source_blob.delete()

    return "Success"
