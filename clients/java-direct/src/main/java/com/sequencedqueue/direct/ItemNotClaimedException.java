package com.sequencedqueue.direct;

public class ItemNotClaimedException extends QueueException {
    public ItemNotClaimedException(String message) {
        super(message);
    }
}
