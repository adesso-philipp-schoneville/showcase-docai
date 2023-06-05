import datetime
import json
import os
import pathlib

from google.cloud import firestore
from utils.logging import logger

LOCATION = os.environ["LOCATION"]
FIRESTORE_COLLECTION = os.environ["FIRESTORE_COLLECTION"]


def add_document(firestore_client: firestore.Client, filename: pathlib.Path):
    """
    Initialize firestore with metadata
    :param firestore_client:
    :param id:
    :param filename:
    :return:
    """
    id = filename.stem

    logger.info(f"Loading file {filename}.")
    with open(filename) as json_file:
        data = json.load(json_file)

    metadata = {
        "filename": filename.name,
        "created": datetime.datetime.now().isoformat(),
        "status": "imported",
    }
    data.update(metadata)

    # Log data
    json_data = json.dumps(data)
    logger.info(json_data)

    # Add a new doc in collection
    doc_ref = firestore_client.collection(FIRESTORE_COLLECTION).document(id)
    doc_ref.set(data, merge=False)
    logger.info(f"Saved data to Firestore / {id}.")


def showcase_data_ingestion(data=None, context: dict = None) -> str:
    """
    Cloud Function to import JSON files with sample data for the showcase into Firestore
    """
    logger.info("showcase_data_ingestion was called.")
    firestore_client = firestore.Client()

    # Folder path containing the files
    folder_path = pathlib.Path("./showcase_data")

    # Iterate over files in the folder and process JSON files
    for file_path in folder_path.iterdir():
        if file_path.is_file() and file_path.suffix == ".json":
            add_document(firestore_client, file_path)

    return "Success"
