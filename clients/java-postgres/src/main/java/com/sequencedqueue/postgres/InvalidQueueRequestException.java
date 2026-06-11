package com.sequencedqueue.postgres;

public class InvalidQueueRequestException extends QueueException {
    public InvalidQueueRequestException(String message) {
        super(message);
    }
}
