import os
from dataclasses import dataclass

from sequenced_queue.client import SequencedQueueClient


def main() -> None:
    run(config_from_env())


def run(config: "Config") -> dict[str, object]:
    client = SequencedQueueClient(
        base_url=config.base_url,
        api_key=config.api_key,
    )
    response = client.enqueue(
        queue_name=config.queue_name,
        source_id=config.source_id,
        item_type=config.item_type,
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
    return response


def config_from_env() -> "Config":
    return Config(
        base_url=os.getenv("SQ_BASE_URL", "http://localhost:8080"),
        api_key=os.getenv("SQ_API_KEY", "dev-key"),
        queue_name=os.getenv("SQ_QUEUE", "wf.commands"),
        source_id=os.getenv("SQ_SOURCE_ID", "example-source"),
        item_type=os.getenv("SQ_ITEM_TYPE", "example.command"),
    )


@dataclass(frozen=True)
class Config:
    base_url: str
    api_key: str
    queue_name: str
    source_id: str
    item_type: str


if __name__ == "__main__":
    main()
