package com.sequencedqueue.direct;

public record HeartbeatRequest(String workerId, Integer extendBySeconds) {
}
