package com.sequencedqueue.direct;

public class LeaseLostException extends QueueException {
    public LeaseLostException(String message) {
        super(message);
    }
}
