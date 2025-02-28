import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta
import tempfile
import os

from llama_index.core.schema import Document
from llama_index.readers.azstorage_blob_changefeed.base import BlobChangeFeedReader


class TestBlobChangeFeedReader(unittest.TestCase):
    def setUp(self):
        # Use a properly formatted mock connection string to pass validation
        self.mock_connection_string = "DefaultEndpointsProtocol=https;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;EndpointSuffix=core.windows.net"

    def test_initialization(self):
        reader = BlobChangeFeedReader(connection_string=self.mock_connection_string)
        assert reader.connection_string == self.mock_connection_string
        assert reader.file_extractor is None
        assert reader.continuation_token is None

    @patch("llama_index.readers.azstorage_blob_changefeed.base.ChangeFeedClient")
    def test_fetch_changes_with_start_time(self, mock_cf_client_cls):
        # Setup mocks
        mock_cf_instance = MagicMock()
        mock_cf_client_cls.from_connection_string.return_value = mock_cf_instance

        mock_list_changes = MagicMock()
        mock_cf_instance.list_changes.return_value = mock_list_changes

        mock_page_iterator = MagicMock()
        mock_list_changes.by_page.return_value = mock_page_iterator

        # Create a mock page with test events
        mock_page = [
            {
                "eventType": "BlobCreated",
                "subject": "/containers/testcontainer/blobs/testblob.txt",
                "data": {
                    "url": "https://teststorage.blob.core.windows.net/testcontainer/testblob.txt"
                },
            }
        ]
        mock_page_iterator.__iter__.return_value = [mock_page]
        mock_page_iterator.continuation_token = "test-token"

        # Setup reader to return documents on blob processing
        test_doc = Document(text="Test document content")

        reader = BlobChangeFeedReader(connection_string=self.mock_connection_string)

        # Mock the document processing
        with patch.object(reader, "_process_event", return_value=[test_doc]):
            start_time = datetime.now() - timedelta(days=1)
            with patch.object(
                reader, "_initialize_change_feed_client", return_value=mock_cf_instance
            ):
                result = reader._fetch_changes(start_time=start_time)

            # Verify results
            assert len(result) == 1
            assert result[0].text == "Test document content"
            assert reader.continuation_token == "test-token"

    @patch("llama_index.readers.azstorage_blob_changefeed.base.ChangeFeedClient")
    def test_fetch_changes_with_container_filter(self, mock_cf_client_cls):
        # Setup mocks
        mock_cf_instance = MagicMock()
        mock_cf_client_cls.from_connection_string.return_value = mock_cf_instance

        mock_list_changes = MagicMock()
        mock_cf_instance.list_changes.return_value = mock_list_changes

        mock_page_iterator = MagicMock()
        mock_list_changes.by_page.return_value = mock_page_iterator

        # Create two events, only one matching the target container
        mock_page = [
            {
                "eventType": "BlobCreated",
                "subject": "/containers/container1/blobs/blob1.txt",
                "data": {
                    "url": "https://teststorage.blob.core.windows.net/container1/blob1.txt"
                },
            },
            {
                "eventType": "BlobCreated",
                "subject": "/containers/container2/blobs/blob2.txt",
                "data": {
                    "url": "https://teststorage.blob.core.windows.net/container2/blob2.txt"
                },
            },
        ]
        mock_page_iterator.__iter__.return_value = [mock_page]

        reader = BlobChangeFeedReader(connection_string=self.mock_connection_string)

        # Mock the document processing and change feed client initialization
        with patch.object(
            reader, "_process_event", return_value=[Document(text="Test content")]
        ), patch.object(
            reader, "_initialize_change_feed_client", return_value=mock_cf_instance
        ):
            result = reader._fetch_changes(container_name="container1")

            # Verify _process_event was only called once with the right container
            assert reader._process_event.call_count == 1
            reader._process_event.assert_called_once_with(
                mock_page[0], "container1", "blob1.txt"
            )

    @patch("llama_index.readers.azstorage_blob_changefeed.base.BlobServiceClient")
    def test_download_and_process_blob(self, mock_blob_service_cls):
        # Setup blob client mocks
        mock_service_client = MagicMock()
        mock_blob_service_cls.from_connection_string.return_value = mock_service_client

        mock_blob_client = MagicMock()
        mock_service_client.get_blob_client.return_value = mock_blob_client

        mock_download = MagicMock()
        mock_blob_client.download_blob.return_value = mock_download

        # Create a temporary file with test content
        with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp_file:
            tmp_file.write("Test document content")
            tmp_path = tmp_file.name

        # Mock the download to write to our temp file
        def mock_readinto(file):
            with open(tmp_path, "rb") as src:
                file.write(src.read())

        mock_download.readinto.side_effect = mock_readinto

        # Mock blob properties
        mock_properties = {
            "name": "test.txt",
            "content_settings": {"content_type": "text/plain"},
            "size": 100,
            "creation_time": datetime.now(),
            "last_modified": datetime.now(),
            "metadata": {"author": "test"},
            "container": "testcontainer",
        }
        mock_blob_client.get_blob_properties.return_value = mock_properties

        reader = BlobChangeFeedReader(connection_string=self.mock_connection_string)

        try:
            result = reader._download_and_process_blob("testcontainer", "testblob.txt")

            # Verify service client was initialized correctly
            mock_blob_service_cls.from_connection_string.assert_called_once_with(
                self.mock_connection_string
            )
            mock_service_client.get_blob_client.assert_called_once_with(
                container="testcontainer", blob="testblob.txt"
            )

            # Verify blob download was called
            mock_blob_client.download_blob.assert_called_once()

            # Verify result
            assert len(result) > 0
            assert isinstance(result[0], Document)

            # The document should have metadata
            assert result[0].metadata
            assert "file_name" in result[0].metadata

        finally:
            # Clean up the temp file
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    def test_process_event(self):
        reader = BlobChangeFeedReader(connection_string=self.mock_connection_string)

        # Test BlobCreated event
        with patch.object(
            reader,
            "_download_and_process_blob",
            return_value=[Document(text="Test content")],
        ) as mock_download:
            event = {
                "eventType": "BlobCreated",
                "subject": "/containers/testcontainer/blobs/test.txt",
            }
            result = reader._process_event(event, "testcontainer", "test.txt")

            mock_download.assert_called_once_with("testcontainer", "test.txt")
            assert len(result) == 1

        # Test BlobDeleted event - should not call download method and return empty list
        with patch.object(reader, "_download_and_process_blob") as mock_download:
            event = {
                "eventType": "BlobDeleted",
                "subject": "/containers/testcontainer/blobs/test.txt",
                "data": {
                    "url": "https://test.blob.core.windows.net/testcontainer/test.txt"
                },
            }
            result = reader._process_event(event, "testcontainer", "test.txt")

            mock_download.assert_not_called()
            assert len(result) == 0

        # Test unknown event - should return empty list
        with patch.object(reader, "_download_and_process_blob") as mock_download:
            event = {
                "eventType": "Unknown",
                "subject": "/containers/testcontainer/blobs/test.txt",
            }
            result = reader._process_event(event, "testcontainer", "test.txt")

            mock_download.assert_not_called()
            assert len(result) == 0

    def test_extract_blob_metadata(self):
        reader = BlobChangeFeedReader(connection_string=self.mock_connection_string)

        # Create test metadata
        created = datetime.now()
        modified = datetime.now() - timedelta(days=2)
        accessed = datetime.now() - timedelta(days=1)

        properties = {
            "name": "testblob.txt",
            "content_settings": {"content_type": "text/plain"},
            "size": 1024,
            "creation_time": created,
            "last_modified": modified,
            "last_accessed_on": accessed,
            "container": "testcontainer",
            "metadata": {"author": "test", "version": "1.0"},
            "tags": {"category": "docs", "status": "draft"},
        }

        result = reader._extract_blob_metadata(properties)

        # Verify metadata extraction
        assert result["file_name"] == "testblob.txt"
        assert result["file_type"] == "text/plain"
        assert result["file_size"] == 1024
        assert result["creation_date"] == created.strftime("%Y-%m-%d")
        assert result["last_modified_date"] == modified.strftime("%Y-%m-%d")
        assert result["last_accessed_date"] == accessed.strftime("%Y-%m-%d")
        assert result["container"] == "testcontainer"

        # Custom metadata should be included
        assert result["author"] == "test"
        assert result["version"] == "1.0"

        # Tags should be included
        assert result["category"] == "docs"
        assert result["status"] == "draft"


if __name__ == "__main__":
    unittest.main()
