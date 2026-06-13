import os
from dataclasses import dataclass

from sequenced_queue.client import QueueItem, RetryableQueueError, SequencedQueueClient


def main() -> None:
    run(config_from_env())


def run(config: "Config") -> bool:
    client = SequencedQueueClient(
        base_url=config.base_url,
        api_key=config.api_key,
    )
    worker = client.worker(config.queue_name, config.worker_id, [config.item_type], config.lease_seconds)

    @worker.handler(config.item_type)
    def handle(item: QueueItem) -> dict[str, object]:
        print(f"python worker handled itemId={item.item_id} sequenceNo={item.sequence_no} payload={item.payload}")
        if item.payload.get("retry"):
            raise RetryableQueueError("payload requested retry")
        return {"handledBy": config.worker_id}

    if config.run_once:
        return worker.run_once()

    try:
        worker.run_forever()
    except KeyboardInterrupt:
        worker.stop()
    return False


def config_from_env() -> "Config":
    return Config(
        base_url=os.getenv("SQ_BASE_URL", "http://localhost:8080"),
        api_key=os.getenv("SQ_API_KEY", "dev-key"),
        queue_name=os.getenv("SQ_QUEUE", "wf.commands"),
        worker_id=os.getenv("SQ_WORKER_ID", "python-worker"),
        item_type=os.getenv("SQ_ITEM_TYPE", "example.command"),
        lease_seconds=int(os.getenv("SQ_LEASE_SECONDS", "30")),
        run_once=os.getenv("SQ_RUN_ONCE", "false").lower() == "true",
    )


@dataclass(frozen=True)
class Config:
    base_url: str
    api_key: str
    queue_name: str
    worker_id: str
    item_type: str
    lease_seconds: int
    run_once: bool


if __name__ == "__main__":
    main()
