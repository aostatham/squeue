package com.sequencedqueue.postgres;

public class LeaseLostException extends QueueException {
    public LeaseLostException(String message) {
        super(message);
    }
}
