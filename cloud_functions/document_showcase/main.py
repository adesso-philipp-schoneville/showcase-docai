import datetime
import hashlib
import json
import os
import io
from collections import defaultdict
from pathlib import Path
from typing import List

import pikepdf
from google.cloud import documentai_v1 as documentai
from google.cloud import firestore, storage
from google.protobuf.json_format import MessageToDict
from tqdm import tqdm
from utils.logging import logger

PROCESSOR_ID_CDS = os.environ["CDS_ID"]
LOCATION = os.environ["LOCATION"]
FIRESTORE_COLLECTION = os.environ["FIRESTORE_COLLECTION"]
CDE_PROCESSORS = {
    "Anschreiben": os.environ["CDE_ANSCHREIBEN"],
    "KFZ_Formular": os.environ["CDE_KFZ_FORMULAR"],
}


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


def save_entities_to_firestore(
    cde_entities: list, doc_ref: firestore.DocumentReference, result: dict
) -> None:
    """
    Save entities to Firestore document.

    :param cde_entities: List of entities from CDE response
    :param doc_ref: Reference to document in Firestore
    """

    entity_data = defaultdict(dict)

    for entity in cde_entities:
        entity_type = entity.type_
        entity_confidence = entity.confidence
        entity_mention_text = entity.mention_text

        # Get the occurrence count for the entity type
        occurrence_count = len(entity_data[entity_type])

        # Save occurrence data under the entity type
        entity_data[entity_type][occurrence_count] = {
            "Confidence": entity_confidence,
            "Value": entity_mention_text,
        }

    # Create the file name dictionary
    extracted_data = dict(entity_data)

    # Print the dictionary structure
    logger.info(f"{extracted_data}")

    # Update the Firestore document with the file data
    result["extracted_data"] = extracted_data
    doc_ref.update(result)


def process_document_cds(
    image_content: bytes, processor_id: str, mock: bool = True
) -> List[str]:
    # If CDS does not work, it has to be mocked
    # load the sample CDS response from the file
    if mock:
        json_file = "sample_cds_response.json"
        with open(json_file) as f:
            data = json.load(f)
        return data

    response = process_document_cds(
        image_content=image_content, processor_id=processor_id
    )
    return [MessageToDict(entity._pb) for entity in response]


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
    logger.info(f"Sending request {request}.")
    result = client.process_document(request=request)
    entities = result.document.entities

    return entities


def extract_with_cde(
    cds_response: list,
    pdf_file: str,
    output_folder: str,
    doc_ref: firestore.DocumentReference,
) -> None:
    """
    Split the PDF file according to the entities specified in the dict and request CDE.

    Args:
        cds_response (dict): CDS response which specifies classes for each page.
        pdf_file (str): Path to the PDF file.
        output_folder (str): Path to the output folder where split PDFs will be saved.
    """
    # Create the output folder if it doesn't exist
    Path(output_folder).mkdir(parents=True, exist_ok=True)

    # Iterate over each entity in the JSON data
    for entity in tqdm(cds_response, desc="Splitting PDF"):
        entity_type = entity["type"]
        page_refs = entity["pageAnchor"]["pageRefs"]

        # Get the start and end page numbers for the entity
        # The first page (0) does not have the page attribute
        try:
            start_page = int(page_refs[0]["page"])
        except KeyError:
            start_page = 0

        try:
            end_page = int(page_refs[-1]["page"])
        except KeyError:
            end_page = 0

        # Split the PDF and save the entity-specific pages
        with pikepdf.Pdf.open(pdf_file) as pdf:
            output_pdf = pikepdf.Pdf.new()

            for page_num in range(start_page, end_page + 1):
                print(f"{entity_type} - page {page_num}")
                output_pdf.pages.append(pdf.pages[page_num])

            entity_name_pages = f"{entity_type}_{start_page}-{end_page}"
            entity_desc = f"{pdf_file}: {entity_type} (pages {start_page}-{end_page})"

            # If there is a CDE processor for this entity type, extract
            # content using the CDE processor
            result = {
                entity_name_pages: {
                    "class": entity_type,
                    "start_page": start_page,
                    "end_page": end_page,
                }
            }
            try:
                processor_id = CDE_PROCESSORS[entity_type]
            except KeyError:
                logger.info(f"{entity_desc} does not have a CDE processor.")
                result["extracted_data"] = {}
                doc_ref.update(result)
            else:
                pdf_bytes = io.BytesIO()
                output_pdf.save(pdf_bytes)
                pdf_bytes.seek(0)

                logger.info(f"Sending request to {processor_id} for {entity_desc}")
                cde_entities = process_document(pdf_bytes.read(), processor_id)

                save_entities_to_firestore(
                    cde_entities=cde_entities, doc_ref=doc_ref, result=result
                )

            doc_ref.update({"status": "cde_processed"})


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
    document_entities = process_document_cds(
        image_content=document, processor_id=PROCESSOR_ID_CDS
    )

    logger.info(str(document_entities).replace("\n", ""))

    doc_ref.update({"status": "cds_processed"})

    # extract values for each page

    # remove file
    source_blob.delete()

    return "Success"
