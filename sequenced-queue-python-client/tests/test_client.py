import threading
import time

from sequenced_queue.client import QueueClientError, QueueItem, RetryableQueueError, SequencedQueueClient, SequencedQueueWorker


class FakeClient:
    def __init__(self):
        self.completed = []
        self.failed = []
        self.claims = []

    def claim(self, queue_name, worker_id, supported_item_types, lease_seconds):
        if self.claims:
            return self.claims.pop(0)
        return {"items": []}

    def complete(self, queue_name, item_id, worker_id, lease_id, result):
        self.completed.append((queue_name, item_id, worker_id, lease_id, result))

    def fail(self, queue_name, item_id, worker_id, lease_id, retryable, error_type, error_message, backoff_seconds=None):
        self.failed.append((retryable, error_type, error_message))

    def heartbeat(self, queue_name, lease_id, worker_id, extend_by_seconds):
        return None


def test_worker_completes_successful_handler():
    client = FakeClient()
    worker = SequencedQueueWorker(client, "q", "w1", ["type"], 60)

    @worker.handler("type")
    def handle(item: QueueItem):
        return {"ok": item.payload["ok"]}

    worker._handle_claim({"leaseId": "lease-1"}, {"itemId": "item-1", "sequenceNo": 1, "itemType": "type", "payload": {"ok": True}, "headers": {}})

    assert client.completed == [("q", "item-1", "w1", "lease-1", {"ok": True})]
    assert client.failed == []


def test_worker_run_once_returns_true_only_when_item_was_handled():
    client = FakeClient()
    client.claims.append({
        "leaseId": "lease-1",
        "items": [{"itemId": "item-1", "sequenceNo": 1, "itemType": "type", "payload": {"ok": True}, "headers": {}}],
    })
    worker = SequencedQueueWorker(client, "q", "w1", ["type"], 60)

    @worker.handler("type")
    def handle(item: QueueItem):
        return {"ok": item.payload["ok"]}

    assert worker.run_once() is True
    assert worker.run_once() is False
    assert client.completed == [("q", "item-1", "w1", "lease-1", {"ok": True})]


def test_worker_marks_retryable_exception_retryable():
    client = FakeClient()
    worker = SequencedQueueWorker(client, "q", "w1", ["type"], 60)

    @worker.handler("type")
    def handle(item: QueueItem):
        raise RetryableQueueError("try again")

    worker._handle_claim({"leaseId": "lease-1"}, {"itemId": "item-1", "sequenceNo": 1, "itemType": "type", "payload": {}, "headers": {}})

    assert client.failed[0][0] is True


def test_client_url_encodes_queue_and_source_path_segments(monkeypatch):
    captured = {}

    class Response:
        status_code = 200
        text = "[]"

        def json(self):
            return []

    def fake_get(url, headers, timeout):
        captured["url"] = url
        captured["headers"] = headers
        captured["timeout"] = timeout
        return Response()

    monkeypatch.setattr("sequenced_queue.client.requests.get", fake_get)

    client = SequencedQueueClient("http://example.test", api_key="key")
    result = client.source_items("queue name/alpha", "source id/1")

    assert result == []
    assert captured["url"] == "http://example.test/queues/queue%20name%2Falpha/sources/source%20id%2F1/items"
    assert captured["headers"]["Authorization"] == "Bearer key"


def test_worker_does_not_complete_or_fail_after_heartbeat_loses_lease():
    class LeaseLostClient(FakeClient):
        def __init__(self):
            super().__init__()
            self.heartbeat_seen = threading.Event()

        def heartbeat(self, queue_name, lease_id, worker_id, extend_by_seconds):
            self.heartbeat_seen.set()
            raise QueueClientError(409, "lease expired")

    client = LeaseLostClient()
    worker = SequencedQueueWorker(client, "q", "w1", ["type"], 1)

    @worker.handler("type")
    def handle(item: QueueItem):
        assert client.heartbeat_seen.wait(timeout=3)
        time.sleep(0.1)
        return {"ok": True}

    worker._handle_claim({"leaseId": "lease-1"}, {"itemId": "item-1", "sequenceNo": 1, "itemType": "type", "payload": {}, "headers": {}})

    assert client.completed == []
    assert client.failed == []
