package com.example.sequencedqueue.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SequencedQueueClientTest {
    private HttpServer server;

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void urlEncodesQueueAndSourcePathSegments() {
        AtomicReference<String> requestedPath = new AtomicReference<>();
        server.createContext("/", exchange -> {
            requestedPath.set(exchange.getRequestURI().getRawPath());
            respond(exchange, 200, "[]");
        });

        client().sourceItems("queue name/alpha", "source id/1");

        assertEquals("/queues/queue%20name%2Falpha/sources/source%20id%2F1/items", requestedPath.get());
    }

    @Test
    void preservesBaseUrlContextPath() {
        AtomicReference<String> requestedPath = new AtomicReference<>();
        server.createContext("/", exchange -> {
            requestedPath.set(exchange.getRequestURI().getRawPath());
            respond(exchange, 200, "[]");
        });

        SequencedQueueClient.builder()
            .baseUrl("http://127.0.0.1:" + server.getAddress().getPort() + "/app")
            .build()
            .sourceItems("queue name/alpha", "source id/1");

        assertEquals("/app/queues/queue%20name%2Falpha/sources/source%20id%2F1/items", requestedPath.get());
    }

    @Test
    void workerDoesNotCompleteOrFailAfterHeartbeatLosesLease() throws Exception {
        UUID leaseId = UUID.randomUUID();
        UUID itemId = UUID.randomUUID();
        AtomicInteger claims = new AtomicInteger();
        AtomicBoolean completed = new AtomicBoolean(false);
        AtomicBoolean failed = new AtomicBoolean(false);
        CountDownLatch heartbeatRejected = new CountDownLatch(1);
        CountDownLatch handlerFinished = new CountDownLatch(1);

        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getRawPath();
            if (path.equals("/queues/q/claims")) {
                if (claims.getAndIncrement() == 0) {
                    respond(exchange, 200, claimJson(leaseId, itemId));
                } else {
                    respond(exchange, 200, "{\"items\":[]}");
                }
            } else if (path.equals("/queues/q/leases/" + leaseId + "/heartbeat")) {
                respond(exchange, 409, "{\"message\":\"lease expired\"}");
                heartbeatRejected.countDown();
            } else if (path.equals("/queues/q/items/" + itemId + "/complete")) {
                completed.set(true);
                respond(exchange, 200, "{}");
            } else if (path.equals("/queues/q/items/" + itemId + "/fail")) {
                failed.set(true);
                respond(exchange, 200, "{}");
            } else {
                respond(exchange, 404, "{}");
            }
        });

        SequencedQueueWorker worker = client().worker("q")
            .workerId("w1")
            .leaseSeconds(1)
            .handler("type", item -> {
                try {
                    assertTrue(heartbeatRejected.await(3, TimeUnit.SECONDS));
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return QueueResult.retryableFailure("INTERRUPTED", "interrupted");
                }
                handlerFinished.countDown();
                return QueueResult.success(Map.of("ok", true));
            })
            .build();

        Thread thread = new Thread(worker::runForever);
        thread.start();
        assertTrue(handlerFinished.await(5, TimeUnit.SECONDS));
        worker.stop();
        thread.interrupt();
        thread.join(2000);

        assertFalse(completed.get());
        assertFalse(failed.get());
    }

    @Test
    void runOnceReturnsTrueOnlyWhenItemWasHandled() {
        UUID leaseId = UUID.randomUUID();
        UUID itemId = UUID.randomUUID();
        AtomicInteger claims = new AtomicInteger();
        AtomicBoolean completed = new AtomicBoolean(false);

        server.createContext("/", exchange -> {
            String path = exchange.getRequestURI().getRawPath();
            if (path.equals("/queues/q/claims")) {
                if (claims.getAndIncrement() == 0) {
                    respond(exchange, 200, claimJson(leaseId, itemId));
                } else {
                    respond(exchange, 200, "{\"items\":[]}");
                }
            } else if (path.equals("/queues/q/items/" + itemId + "/complete")) {
                completed.set(true);
                respond(exchange, 200, "{}");
            } else {
                respond(exchange, 200, "{}");
            }
        });

        SequencedQueueWorker worker = client().worker("q")
            .workerId("w1")
            .leaseSeconds(60)
            .handler("type", item -> QueueResult.success(Map.of("ok", true)))
            .build();

        assertTrue(worker.runOnce());
        assertFalse(worker.runOnce());
        assertTrue(completed.get());
    }

    private SequencedQueueClient client() {
        return SequencedQueueClient.builder()
            .baseUrl("http://127.0.0.1:" + server.getAddress().getPort())
            .apiKey("key")
            .build();
    }

    private static String claimJson(UUID leaseId, UUID itemId) {
        return """
            {
              "leaseId": "%s",
              "queueName": "q",
              "sourceId": "source-1",
              "leaseUntil": "%s",
              "items": [
                {
                  "itemId": "%s",
                  "sequenceNo": 1,
                  "itemType": "type",
                  "payload": {},
                  "headers": {}
                }
              ]
            }
            """.formatted(leaseId, OffsetDateTime.now().plusMinutes(1), itemId);
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes();
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
