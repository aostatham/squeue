import os

from sequenced_queue.client import SequencedQueueClient


def main() -> None:
    client = SequencedQueueClient(
        base_url=os.getenv("SQ_BASE_URL", "http://localhost:8080"),
        api_key=os.getenv("SQ_API_KEY", "dev-key"),
    )
    response = client.enqueue(
        queue_name=os.getenv("SQ_QUEUE", "wf.commands"),
        source_id=os.getenv("SQ_SOURCE_ID", "example-source"),
        item_type=os.getenv("SQ_ITEM_TYPE", "example.command"),
        payload={"message": "hello from Python", "createdBy": "python-producer"},
        headers={"example": True},
        max_attempts=5,
    )
    print(
        "enqueued "
        f"itemId={response['itemId']} "
        f"queue={response['queueName']} "
        f"source={response['sourceId']} "
        f"sequenceNo={response['sequenceNo']} "
        f"status={response['status']}"
    )


if __name__ == "__main__":
    main()
