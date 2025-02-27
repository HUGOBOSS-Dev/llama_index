import logging
from datetime import datetime
from typing import Optional, List
from llama_index.core.readers.base import BaseReader
from llama_index.core.schema import Document

logger = logging.getLogger(__name__)

class BlobChangeFeedReader(BaseReader):
    def __init__(self, connection_string: str, container_name: str):
        self.connection_string = connection_string
        self.container_name = container_name
        self.change_feed_client = self._initialize_change_feed_client()

    def _initialize_change_feed_client(self):
        from azure.storage.blob.changefeed import ChangeFeedClient
        return ChangeFeedClient.from_connection_string(self.connection_string)

    def fetch_changes(
        self,
        start_time: Optional[datetime] = None,
        continuation_token: Optional[str] = None
    ) -> List[Document]:
        """
        Fetch all change feed events from a specified datetime or using a continuation token.

        Args:
            start_time (datetime, optional): The UTC datetime to start fetching changes.
            continuation_token (str, optional): Token to resume fetching changes.

        Returns:
            List of change feed events.
        """
        logger.info("Fetching change feed events.")
        events = []
        results_per_page = 500  # Adjust as needed.
        try:
            if continuation_token:
                page_iterator = self.change_feed_client.list_changes(
                    results_per_page=results_per_page
                ).by_page(continuation_token=continuation_token)
            else:
                page_iterator = self.change_feed_client.list_changes(
                    start_time=start_time, results_per_page=results_per_page
                ).by_page()
            for page in page_iterator:
                for event in page:
                    # Filter out events not targeting the specified container if applicable.
                    if hasattr(event, "container_name") and event.container_name != self.container_name:
                        continue
                    events.append(event)
            return events
        except Exception as e:
            logger.error(f"Error fetching changes: {e}")
            raise

    def process_event(self, event):
        # Log event details.
        event_type = getattr(event, "event_type", "Unknown")
        blob_url = getattr(event, "blob_url", "Unknown")
        logger.info(f"Received event: {event_type} for blob: {blob_url}")

        # Process event based on type.
        if event_type == "BlobCreated":
            self._handle_blob_created(event)
        elif event_type == "BlobDeleted":
            self._handle_blob_deleted(event)
        elif event_type == "BlobUpdated":
            self._handle_blob_updated(event)
        else:
            logger.debug(f"Unhandled event type: {event_type}")

    def _handle_blob_created(self, event):
        blob_url = getattr(event, "blob_url", "Unknown")
        logger.info(f"Handling blob created event for blob: {blob_url}")
        # Add logic for processing a newly created blob here.

    def _handle_blob_deleted(self, event):
        blob_url = getattr(event, "blob_url", "Unknown")
        logger.info(f"Handling blob deleted event for blob: {blob_url}")
        # Add logic for processing a deleted blob here.

    def _handle_blob_updated(self, event):
        blob_url = getattr(event, "blob_url", "Unknown")
        logger.info(f"Handling blob updated event for blob: {blob_url}")
        # Add logic for processing an updated blob here.

    def run(
        self,
        start_time: Optional[datetime] = None,
        continuation_token: Optional[str] = None
    ):
        """
        Retrieve and process change feed events starting from a specified datetime or using a continuation token.
        """
        events = self.fetch_changes(start_time=start_time, continuation_token=continuation_token)
        for event in events:
            self.process_event(event)

    def load_data(self, **kwargs) -> List[Document]:
        """
        Overridden method from BaseReader to fetch change feed events.
        Expects optional 'start_time' and 'continuation_token' in kwargs.
        """
        start_time = kwargs.get("start_time")
        continuation_token = kwargs.get("continuation_token")
        return self.run(start_time=start_time, continuation_token=continuation_token)
