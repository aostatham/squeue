# Direct PostgreSQL Notify Wake-Up

Direct Java workers can optionally use PostgreSQL `LISTEN/NOTIFY` as a wake-up hint.

The queue tables remain the durable source of truth. Notifications are not work storage, not delivery acknowledgements, and not an exactly-once mechanism. A notified worker still claims work through the normal core claim path.

## Enabling Notification Emission

Producer, admin, or recovery clients that should wake notify workers must enable PostgreSQL notifications:

```java
SequencedQueueDirectClient client = SequencedQueueDirectClient.builder()
    .dataSource(dataSource)
    .validateSchemaOnBuild(true)
    .postgresNotifications(PostgresNotificationOptions.enabled()
        .channel("sequenced_queue_wakeup"))
    .build();
```

When notification emission is not enabled, direct workers still work through their fallback safety sweep.

## Enabling Worker Wake-Up

```java
SequencedQueueDirectWorker worker = client.worker("wf.commands")
    .workerId("wf-command-worker-1")
    .handler("wf.command", handler)
    .waitStrategy(DirectWorkerWaitStrategy.postgresNotify()
        .channel("sequenced_queue_wakeup")
        .fallbackPollInterval(Duration.ofSeconds(30)))
    .build();
```

The worker starts listening, drains currently available work through normal claim logic, and waits only when no work is claimable.

## Safety Rules

- The fallback safety sweep is required and defaults to 30 seconds.
- The worker uses one long-lived PostgreSQL listener connection in addition to ordinary operation connections.
- Missed notifications, listener reconnects, and `retry_wait` time passing are handled by the fallback sweep.
- Notification payloads include only `queueName` and a wake-up reason.
- Notification payloads do not include item payloads, headers, result JSON, error text, admin reason, or admin metadata.
- No schema migration is required; the schema baseline remains `V1`.

Direct Java notification wake-up has no REST dependency. REST/WebSocket/SSE worker wake-up is separate post-MVP work.
