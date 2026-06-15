# sequenced-queue Python Client

Python REST client and polling worker helper for `sequenced-queue`.

The package talks to the HTTP API only. It does not access PostgreSQL directly and does not apply Flyway migrations.

When the server returns a structured error such as `FIELD_TOO_LARGE`, the client preserves the response details on `QueueClientError` without adding oversized field content to the exception message.

Install for local development:

```sh
python -m pip install -e .
```

Run tests:

```sh
python -m pytest
```
