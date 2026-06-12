from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable
from urllib.parse import quote

import requests


class QueueClientError(RuntimeError):
    def __init__(self, status_code: int, message: str):
        super().__init__(message)
        self.status_code = status_code


class RetryableQueueError(RuntimeError):
    pass


@dataclass(frozen=True)
class QueueItem:
    item_id: str
    sequence_no: int
    item_type: str
    payload: dict[str, Any]
    headers: dict[str, Any]


class SequencedQueueClient:
    def __init__(self, base_url: str, api_key: str | None = None, timeout: float = 10.0):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout

    def enqueue(
        self,
        queue_name: str,
        source_id: str,
        item_type: str,
        payload: dict[str, Any] | None = None,
        headers: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
        available_at: str | None = None,
        max_attempts: int | None = None,
    ) -> dict[str, Any]:
        return self._post(
            f"/queues/{_path_segment(queue_name)}/items",
            {
                "sourceId": source_id,
                "itemType": item_type,
                "idempotencyKey": idempotency_key,
                "payload": payload or {},
                "headers": headers or {},
                "availableAt": available_at,
                "maxAttempts": max_attempts,
            },
        )

    def claim(self, queue_name: str, worker_id: str, supported_item_types: list[str], lease_seconds: int = 60) -> dict[str, Any]:
        return self._post(
            f"/queues/{_path_segment(queue_name)}/claims",
            {
                "workerId": worker_id,
                "supportedItemTypes": supported_item_types,
                "leaseSeconds": lease_seconds,
                "maxItems": 1,
            },
        )

    def complete(self, queue_name: str, item_id: str, worker_id: str, lease_id: str, result: dict[str, Any] | None = None) -> dict[str, Any]:
        return self._post(
            f"/queues/{_path_segment(queue_name)}/items/{_path_segment(item_id)}/complete",
            {"workerId": worker_id, "leaseId": lease_id, "result": result or {}},
        )

    def fail(
        self,
        queue_name: str,
        item_id: str,
        worker_id: str,
        lease_id: str,
        retryable: bool,
        error_type: str,
        error_message: str,
        backoff_seconds: int | None = None,
    ) -> dict[str, Any]:
        return self._post(
            f"/queues/{_path_segment(queue_name)}/items/{_path_segment(item_id)}/fail",
            {
                "workerId": worker_id,
                "leaseId": lease_id,
                "retryable": retryable,
                "errorType": error_type,
                "errorMessage": error_message,
                "backoffSeconds": backoff_seconds,
            },
        )

    def heartbeat(self, queue_name: str, lease_id: str, worker_id: str, extend_by_seconds: int = 60) -> None:
        self._post(
            f"/queues/{_path_segment(queue_name)}/leases/{_path_segment(lease_id)}/heartbeat",
            {"workerId": worker_id, "extendBySeconds": extend_by_seconds},
            expect_json=False,
        )

    def source_items(self, queue_name: str, source_id: str) -> list[dict[str, Any]]:
        return self._get(f"/queues/{_path_segment(queue_name)}/sources/{_path_segment(source_id)}/items")

    def worker(self, queue_name: str, worker_id: str, supported_item_types: list[str], lease_seconds: int = 60) -> "SequencedQueueWorker":
        return SequencedQueueWorker(self, queue_name, worker_id, supported_item_types, lease_seconds)

    def _get(self, path: str) -> list[dict[str, Any]]:
        response = requests.get(
            self.base_url + path,
            headers=self._headers(),
            timeout=self.timeout,
        )
        if response.status_code >= 400:
            raise QueueClientError(response.status_code, response.text)
        if not response.text:
            return []
        return response.json()

    def _post(self, path: str, body: dict[str, Any], expect_json: bool = True) -> dict[str, Any]:
        response = requests.post(
            self.base_url + path,
            json=body,
            headers=self._headers(),
            timeout=self.timeout,
        )
        if response.status_code >= 400:
            raise QueueClientError(response.status_code, response.text)
        if not expect_json or not response.text:
            return {}
        return response.json()

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers


class SequencedQueueWorker:
    def __init__(
        self,
        client: SequencedQueueClient,
        queue_name: str,
        worker_id: str,
        supported_item_types: list[str],
        lease_seconds: int = 60,
    ):
        self.client = client
        self.queue_name = queue_name
        self.worker_id = worker_id
        self.supported_item_types = supported_item_types
        self.lease_seconds = lease_seconds
        self.handlers: dict[str, Callable[[QueueItem], dict[str, Any] | None]] = {}
        self._running = False

    def handler(self, item_type: str) -> Callable[[Callable[[QueueItem], dict[str, Any] | None]], Callable[[QueueItem], dict[str, Any] | None]]:
        def register(func: Callable[[QueueItem], dict[str, Any] | None]) -> Callable[[QueueItem], dict[str, Any] | None]:
            self.handlers[item_type] = func
            if item_type not in self.supported_item_types:
                self.supported_item_types.append(item_type)
            return func

        return register

    def run_forever(self) -> None:
        self._running = True
        empty_sleep = 0.1
        while self._running:
            claim = self.client.claim(self.queue_name, self.worker_id, self.supported_item_types, self.lease_seconds)
            items = claim.get("items") or []
            if not items:
                time.sleep(empty_sleep)
                empty_sleep = min(5.0, empty_sleep * 2)
                continue
            empty_sleep = 0.1
            self._handle_claim(claim, items[0])

    def stop(self) -> None:
        self._running = False

    def _handle_claim(self, claim: dict[str, Any], raw_item: dict[str, Any]) -> None:
        lease_id = claim["leaseId"]
        item = QueueItem(
            item_id=raw_item["itemId"],
            sequence_no=raw_item["sequenceNo"],
            item_type=raw_item["itemType"],
            payload=raw_item.get("payload") or {},
            headers=raw_item.get("headers") or {},
        )
        stop_heartbeat = threading.Event()
        lease_lost = threading.Event()
        heartbeat = threading.Thread(target=self._heartbeat_loop, args=(lease_id, stop_heartbeat, lease_lost), daemon=True)
        heartbeat.start()
        try:
            handler = self.handlers.get(item.item_type)
            if handler is None:
                self.client.fail(self.queue_name, item.item_id, self.worker_id, lease_id, False, "NO_HANDLER", f"No handler for {item.item_type}")
                return
            result = handler(item)
            if lease_lost.is_set():
                return
            self.client.complete(self.queue_name, item.item_id, self.worker_id, lease_id, result or {})
        except RetryableQueueError as exc:
            if lease_lost.is_set():
                return
            self.client.fail(self.queue_name, item.item_id, self.worker_id, lease_id, True, exc.__class__.__name__, str(exc))
        except Exception as exc:
            if lease_lost.is_set():
                return
            self.client.fail(self.queue_name, item.item_id, self.worker_id, lease_id, False, exc.__class__.__name__, str(exc))
        finally:
            stop_heartbeat.set()
            heartbeat.join(timeout=1)

    def _heartbeat_loop(self, lease_id: str, stop: threading.Event, lease_lost: threading.Event) -> None:
        interval = max(1, self.lease_seconds // 2)
        while not stop.wait(interval):
            try:
                self.client.heartbeat(self.queue_name, lease_id, self.worker_id, self.lease_seconds)
            except Exception:
                lease_lost.set()
                stop.set()


def _path_segment(value: str) -> str:
    return quote(str(value), safe="")
