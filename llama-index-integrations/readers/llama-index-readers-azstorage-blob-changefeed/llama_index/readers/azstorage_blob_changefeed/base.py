from enum import Enum
import logging
from datetime import datetime
import re
from typing import Any, Dict, Iterator, Optional, List, Union
import tempfile
from llama_index.core.readers import SimpleDirectoryReader
from llama_index.core.readers.base import BasePydanticReader
from llama_index.core.schema import Document
from azure.storage.blob import BlobServiceClient
from azure.storage.blob.changefeed import ChangeFeedClient
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

class EventType(Enum):
    UPSERT = "Upsert"
    DELETED = "Deleted"

class ChangeInfo(BaseModel):
    container: str
    blob_name: str
    event_type: EventType

class BlobChangeFeedReader(BasePydanticReader):

    connection_string: Optional[str] = None
    file_extractor: Optional[Dict[str, Union[str, BasePydanticReader]]] = Field(None, exclude=True)
    continuation_token: Optional[str] = Field(None, exclude=True)
    deleted_blobs: Optional[list[ChangeInfo]] = Field([], exclude=True)

    def _initialize_change_feed_client(self):
        return ChangeFeedClient.from_connection_string(self.connection_string)

    def load_data(self, **kwargs) -> List[Document]:
        """Implementation of the BasePydanticReader interface."""
        start_time=kwargs.get("start_time")
        continuation_token=kwargs.get("continuation_token")
        container_name=kwargs.get("container_name")

        self.continuation_token = continuation_token
        self.deleted_blobs = []

        try:

            with BlobServiceClient.from_connection_string(self.connection_string) as service_client:

                for change in self._get_changes(start_time=start_time, continuation_token=continuation_token):
                    if container_name and change.container != container_name:
                        continue

                    if change.event_type == EventType.UPSERT:
                        self._download_and_process_blob(service_client, change.container, change.blob_name)
                    elif change.event_type == EventType.DELETED:
                        self.deleted_blobs.append(change)

        except Exception as e:
            logger.error(f"Error loading data: {e}")
            raise

    def _get_changes(self,
        start_time: Optional[datetime] = None,
        continuation_token: Optional[str] = None,
    ) -> Iterator[ChangeInfo]:
        """Fetch and process changes from Azure Blob Storage change feed."""
        client = self._initialize_change_feed_client()
        logger.info("Fetching change feed events.")
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

                        blob_event_type = event.get("eventType", "Unknown")
                        if blob_event_type in ("BlobCreated", "BlobUpdated"):
                            event_type = EventType.UPSERT
                        elif blob_event_type == "BlobDeleted":
                            event_type = EventType.DELETED
                        else:
                            continue

                        yield ChangeInfo(container=evt_container, blob_name=blob_name, event_type=event_type)


        except Exception as e:
            logger.error(f"Error fetching changes: {e}")
            raise


    def _download_and_process_blob(self, service_client: BlobServiceClient, container_name: str, blob_name: str) -> List[Document]:
        """Download blob content and process it into documents."""
        try:
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
