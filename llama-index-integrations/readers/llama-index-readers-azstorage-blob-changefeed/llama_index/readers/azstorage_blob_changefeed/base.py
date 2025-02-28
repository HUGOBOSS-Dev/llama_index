from dataclasses import Field
import logging
from datetime import datetime
import re
from typing import Any, Dict, Optional, List, Union
import tempfile
import os
from llama_index.core.readers import SimpleDirectoryReader, FileSystemReaderMixin
from llama_index.core.readers.base import BaseReader, BasePydanticReader, ResourcesReaderMixin
from llama_index.core.schema import Document
from azure.storage.blob import BlobServiceClient, BlobClient  # new import

logger = logging.getLogger(__name__)

class BlobChangeFeedReader(BasePydanticReader):

    connection_string: Optional[str] = None
    file_extractor: Optional[Dict[str, Union[str, BaseReader]]] = None

    def _initialize_change_feed_client(self):
        from azure.storage.blob.changefeed import ChangeFeedClient
        return ChangeFeedClient.from_connection_string(self.connection_string)

    def fetch_changes(
        self,
        start_time: Optional[datetime] = None,
        continuation_token: Optional[str] = None,
        container_name: Optional[str] = None
    ) -> List[Document]:
        """
        Fetch all change feed events from a specified datetime or using a continuation token.

        Args:
            start_time (datetime, optional): The UTC datetime to start fetching changes.
            continuation_token (str, optional): Token to resume fetching changes.

        Returns:
            List of change feed events.
        """
        change_feed_client = self._initialize_change_feed_client()

        logger.info("Fetching change feed events.")
        events = []
        results_per_page = 500  # Adjust as needed.
        try:
            if continuation_token:
                page_iterator = change_feed_client.list_changes(
                    results_per_page=results_per_page
                ).by_page(continuation_token=continuation_token)
            else:
                page_iterator = change_feed_client.list_changes(
                    start_time=start_time, results_per_page=results_per_page
                ).by_page()
            for page in page_iterator:
                for event in page:
                    # Extract container name from the event subject
                    match = re.search(r"/containers/([^/]+)/blobs/(.+)", event.get("subject"))
                    event_container_name = match.group(1) if match else None
                    blob_name = match.group(2) if match else None
                    # Filter out events not targeting the specified container if applicable.
                    if container_name and event_container_name != container_name:
                        continue
                    events.append(event)
            return events
        except Exception as e:
            logger.error(f"Error fetching changes: {e}")
            raise

    def process_event(self, event):
        # Log event details.
        event_type = event.get("eventType", "Unknown")
        logger.info(f"Received event: {event_type}")

        # Process event based on type.
        if event_type == "BlobCreated":
            self._handle_blob_created(event)
        elif event_type == "BlobDeleted":
            self._handle_blob_deleted(event)
        elif event_type == "BlobUpdated":
            self._handle_blob_updated(event)
        else:
            logger.debug(f"Unhandled event type: {event_type}")

    def _download_blob(self, container_name: str, blob_name: str) -> bytes:
        """
        Download the blob content from the given URL using a temporary file.
        """
        try:
            with BlobServiceClient.from_connection_string(self.connection_string) as service_client:
                blob_client = service_client.get_blob_client(container_name, blob_name)
                with tempfile.NamedTemporaryFile(delete=True) as temp_file:
                    blob_data = blob_client.download_blob()
                    blob_data.readinto(temp_file)
                    temp_file.flush()
                    # Read the content
                    blob_metadata = blob_client.get_blob_properties()

                    def get_metadata(file_name: str) -> Dict[str, Any]:
                        return blob_metadata.get(file_name, {})

                    loader = SimpleDirectoryReader(
                        input_files=[temp_file], file_extractor=self.file_extractor, file_metadata=get_metadata
                    )

                    return loader.load_data()
        except Exception as e:
            logger.error(f"Error downloading blob from {blob_name}: {e}")
            raise

    def _handle_blob_created(self, event):
        blob_url = event.get("data", {}).get("url", "Unknown")
        logger.info(f"Handling blob created event for blob: {blob_url}")
        self._download_blob(blob_url)  # new code: download the blob
        # Add logic for processing a newly created blob here.

    def _handle_blob_deleted(self, event):
        blob_url = event.get("data", {}).get("url", "Unknown")
        logger.info(f"Handling blob deleted event for blob: {blob_url}")
        # Add logic for processing a deleted blob here.

    def _handle_blob_updated(self, event):
        blob_url = event.get("data", {}).get("url", "Unknown")
        logger.info(f"Handling blob updated event for blob: {blob_url}")
        self._download_blob(blob_url)  # new code: download the blob
        # Add logic for processing an updated blob here.

    def run(
        self,
        start_time: Optional[datetime] = None,
        continuation_token: Optional[str] = None,
        container_name: Optional[str] = None
    ):
        """
        Retrieve and process change feed events starting from a specified datetime or using a continuation token.
        """
        events = self.fetch_changes(start_time=start_time, continuation_token=continuation_token, container_name=container_name)
        for event in events:
            self.process_event(event)

    def load_data(self, **kwargs) -> List[Document]:
        """
        Overridden method from BaseReader to fetch change feed events.
        Expects optional 'start_time' and 'continuation_token' in kwargs.
        """
        start_time = kwargs.get("start_time")
        continuation_token = kwargs.get("continuation_token")
        container_name = kwargs.get("container_name")
        return self.run(start_time=start_time, continuation_token=continuation_token, container_name=container_name)
