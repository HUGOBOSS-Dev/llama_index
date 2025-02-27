from datetime import datetime, timezone
from llama_index.readers.azstorage_blob_changefeed.base import BlobChangeFeedReader

# Fake event to simulate change feed events.
class FakeEvent:
    def __init__(self, event_type, blob_url, container_name) -> None:
        self.event_type = event_type
        self.blob_url = blob_url
        self.container_name = container_name

# Simplified FakeChangeFeedClient merging dummy functionality.
class FakeChangeFeedClient:
    def __init__(self, pages) -> None:
        self.pages = pages
    def list_changes(self, start_time=None, results_per_page=500, continuation_token=None):
        return self
    def by_page(self, continuation_token=None):
        yield from self.pages

# Subclass for testing to capture handler calls.
class TestReader(BlobChangeFeedReader):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.handled_events = []
    def _handle_blob_created(self, event):
        self.handled_events.append(("created", event.blob_url))
    def _handle_blob_deleted(self, event):
        self.handled_events.append(("deleted", event.blob_url))
    def _handle_blob_updated(self, event):
        self.handled_events.append(("updated", event.blob_url))

def test_fetch_changes(monkeypatch):
    # Prepare fake events: one non-matching container will be filtered out.
    events_page = [
        FakeEvent("BlobCreated", "url1", "match_container"),
        FakeEvent("BlobDeleted", "url2", "non_matching"),
        FakeEvent("BlobUpdated", "url3", "match_container")
    ]
    fake_client = FakeChangeFeedClient(pages=[events_page])

    def dummy_initialize(self):
        return fake_client
    monkeypatch.setattr(BlobChangeFeedReader, "_initialize_change_feed_client", dummy_initialize)

    reader = BlobChangeFeedReader(connection_string="dummy", container_name="match_container")
    events = reader.fetch_changes(start_time=datetime.now(timezone.utc))
    assert len(events) == 2
    assert all(e.container_name == "match_container" for e in events)

def test_process_event():
    reader = TestReader(connection_string="dummy", container_name="any")
    # Test each event type.
    events = [
        FakeEvent("BlobCreated", "url_created", "any"),
        FakeEvent("BlobDeleted", "url_deleted", "any"),
        FakeEvent("BlobUpdated", "url_updated", "any"),
        FakeEvent("Unknown", "url_unknown", "any")
    ]
    for event in events:
        reader.process_event(event)

    assert ("created", "url_created") in reader.handled_events
    assert ("deleted", "url_deleted") in reader.handled_events
    assert ("updated", "url_updated") in reader.handled_events
