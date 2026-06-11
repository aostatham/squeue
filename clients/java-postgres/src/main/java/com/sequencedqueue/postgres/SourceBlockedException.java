package com.sequencedqueue.postgres;

public class SourceBlockedException extends QueueException {
    public SourceBlockedException(String message) {
        super(message);
    }
}
