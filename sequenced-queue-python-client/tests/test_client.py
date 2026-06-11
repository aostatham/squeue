from sequenced_queue.client import QueueItem, SequencedQueueWorker, RetryableQueueError


class FakeClient:
    def __init__(self):
        self.completed = []
        self.failed = []

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


def test_worker_marks_retryable_exception_retryable():
    client = FakeClient()
    worker = SequencedQueueWorker(client, "q", "w1", ["type"], 60)

    @worker.handler("type")
    def handle(item: QueueItem):
        raise RetryableQueueError("try again")

    worker._handle_claim({"leaseId": "lease-1"}, {"itemId": "item-1", "sequenceNo": 1, "itemType": "type", "payload": {}, "headers": {}})

    assert client.failed[0][0] is True
