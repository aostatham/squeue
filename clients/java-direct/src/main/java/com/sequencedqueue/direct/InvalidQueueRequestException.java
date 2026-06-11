package com.sequencedqueue.direct;

public class InvalidQueueRequestException extends QueueException {
    public InvalidQueueRequestException(String message) {
        super(message);
    }
}
