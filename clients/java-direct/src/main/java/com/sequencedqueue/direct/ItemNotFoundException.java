package com.sequencedqueue.direct;

public class ItemNotFoundException extends QueueException {
    public ItemNotFoundException(String message) {
        super(message);
    }
}
