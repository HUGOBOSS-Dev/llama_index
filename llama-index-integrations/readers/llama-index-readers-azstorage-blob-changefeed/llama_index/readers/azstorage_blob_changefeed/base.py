import logging
from datetime import datetime
import re
from typing import Any, Dict, Optional, List, Union
import tempfile
from llama_index.core.readers import SimpleDirectoryReader
from llama_index.core.readers.base import BasePydanticReader
from llama_index.core.schema import Document
from azure.storage.blob import BlobServiceClient
from azure.storage.blob.changefeed import ChangeFeedClient
from pydantic import Field

logger = logging.getLogger(__name__)

class BlobChangeFeedReader(BasePydanticReader):

    connection_string: Optional[str] = None
    file_extractor: Optional[Dict[str, Union[str, BasePydanticReader]]] = Field(None, exclude=True)
    continuation_token: Optional[str] = Field(None, exclude=True)

    def _initialize_change_feed_client(self):
        return ChangeFeedClient.from_connection_string(self.connection_string)

    def _fetch_changes(
        self,
        start_time: Optional[datetime] = None,
        continuation_token: Optional[str] = None,
        container_name: Optional[str] = None
    ) -> List[Document]:
        """Fetch and process changes from Azure Blob Storage change feed."""
        client = self._initialize_change_feed_client()
        logger.info("Fetching change feed events.")
        documents = []
        results_per_page = 1

        self.continuation_token = continuation_token

        try:
            if continuation_token:
                change_feed = client.list_changes(
                    results_per_page=results_per_page
                ).by_page(continuation_token=continuation_token)
            else:
                change_feed = client.list_changes(
                    start_time=start_time, results_per_page=results_per_page
                ).by_page()

                for page in change_feed:
                    self.continuation_token = change_feed.continuation_token
                    for event in page:
                        match = re.search(r"/containers/([^/]+)/blobs/(.+)", event.get("subject", ""))

                        if not match:
                            continue

                        evt_container, blob_name = match.groups()
                        if container_name and evt_container != container_name:
                            continue

                        documents.extend(self._process_event(event, evt_container, blob_name))

        except Exception as e:
            logger.error(f"Error fetching changes: {e}")
            raise

        return documents

    def _process_event(self, event, container_name: str, blob_name: str) -> List[Document]:
        """Process a single blob change event."""
        event_type = event.get("eventType", "Unknown")
        logger.info(f"Processing {event_type} event")

        if event_type in ("BlobCreated", "BlobUpdated"):
            return self._download_and_process_blob(container_name, blob_name)
        elif event_type == "BlobDeleted":
            url = event.get("data", {}).get("url", "Unknown")
            logger.info(f"Blob deleted: {url}")
        else:
            logger.debug(f"Unhandled event type: {event_type}")

        return []

    def _download_and_process_blob(self, container_name: str, blob_name: str) -> List[Document]:
        """Download blob content and process it into documents."""
        try:
            with BlobServiceClient.from_connection_string(self.connection_string) as service_client:
                blob_client = service_client.get_blob_client(container=container_name, blob=blob_name)

                with tempfile.NamedTemporaryFile(delete=False) as temp_file:
                    blob_client.download_blob().readinto(temp_file)
                    temp_file_path = temp_file.name

                def get_metadata(file_name: str):
                        return self._extract_blob_metadata(blob_client.get_blob_properties())

                loader = SimpleDirectoryReader(
                    input_files=[temp_file_path],
                    file_extractor=self.file_extractor,
                    file_metadata=get_metadata
                )

                return loader.load_data()

        except Exception as e:
            logger.error(f"Error processing blob {blob_name}: {e}")
            raise
    def _extract_blob_metadata(self, file_metadata: Dict[str, Any]) -> Dict[str, Any]:
        meta: dict = file_metadata

        creation_time = meta.get("creation_time")
        creation_time = creation_time.strftime("%Y-%m-%d") if creation_time else None

        last_modified = meta.get("last_modified")
        last_modified = last_modified.strftime("%Y-%m-%d") if last_modified else None

        last_accessed_on = meta.get("last_accessed_on")
        last_accessed_on = (
            last_accessed_on.strftime("%Y-%m-%d") if last_accessed_on else None
        )

        extracted_meta = {
            "file_name": meta.get("name"),
            "file_type": meta.get("content_settings", {}).get("content_type"),
            "file_size": meta.get("size"),
            "creation_date": creation_time,
            "last_modified_date": last_modified,
            "last_accessed_date": last_accessed_on,
            "container": meta.get("container"),
        }

        extracted_meta.update(meta.get("metadata") or {})
        extracted_meta.update(meta.get("tags") or {})

        return extracted_meta

    def load_data(self, **kwargs) -> List[Document]:
        """Implementation of the BasePydanticReader interface."""
        return self._fetch_changes(
            start_time=kwargs.get("start_time"),
            continuation_token=kwargs.get("continuation_token"),
            container_name=kwargs.get("container_name")
        )
