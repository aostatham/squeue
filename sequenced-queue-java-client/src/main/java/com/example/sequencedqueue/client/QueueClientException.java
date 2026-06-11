package com.example.sequencedqueue.client;

public class QueueClientException extends RuntimeException {
    private final int statusCode;

    public QueueClientException(int statusCode, String message) {
        super(message);
        this.statusCode = statusCode;
    }

    public int statusCode() {
        return statusCode;
    }
}
