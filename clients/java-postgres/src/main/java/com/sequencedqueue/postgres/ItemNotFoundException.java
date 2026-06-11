package com.sequencedqueue.postgres;

public class ItemNotFoundException extends QueueException {
    public ItemNotFoundException(String message) {
        super(message);
    }
}
