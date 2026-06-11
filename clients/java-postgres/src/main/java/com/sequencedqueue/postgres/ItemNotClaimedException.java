package com.sequencedqueue.postgres;

public class ItemNotClaimedException extends QueueException {
    public ItemNotClaimedException(String message) {
        super(message);
    }
}
