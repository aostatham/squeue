package com.sequencedqueue.direct;

import java.util.UUID;

public record EnqueueResponse(
    UUID itemId,
    String queueName,
    String sourceId,
    long sequenceNo,
    String status
) {
}
