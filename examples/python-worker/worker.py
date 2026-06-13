import os

from sequenced_queue.client import QueueItem, RetryableQueueError, SequencedQueueClient


def main() -> None:
    queue_name = os.getenv("SQ_QUEUE", "wf.commands")
    item_type = os.getenv("SQ_ITEM_TYPE", "example.command")
    worker_id = os.getenv("SQ_WORKER_ID", "python-worker")
    lease_seconds = int(os.getenv("SQ_LEASE_SECONDS", "30"))

    client = SequencedQueueClient(
        base_url=os.getenv("SQ_BASE_URL", "http://localhost:8080"),
        api_key=os.getenv("SQ_API_KEY", "dev-key"),
    )
    worker = client.worker(queue_name, worker_id, [item_type], lease_seconds)

    @worker.handler(item_type)
    def handle(item: QueueItem) -> dict[str, object]:
        print(f"python worker handled itemId={item.item_id} sequenceNo={item.sequence_no} payload={item.payload}")
        if item.payload.get("retry"):
            raise RetryableQueueError("payload requested retry")
        return {"handledBy": worker_id}

    try:
        worker.run_forever()
    except KeyboardInterrupt:
        worker.stop()


if __name__ == "__main__":
    main()
