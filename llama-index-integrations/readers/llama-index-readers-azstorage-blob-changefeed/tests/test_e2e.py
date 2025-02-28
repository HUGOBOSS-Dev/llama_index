import os
from datetime import datetime
from llama_index.readers.azstorage_blob_changefeed.base import BlobChangeFeedReader


def test_process_event():
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    reader = BlobChangeFeedReader(connection_string=connection_string, container_name="sap")

    documents = reader.load_data(date=datetime(2025, 2, 1))
    token = reader.continuation_token


test_process_event()
