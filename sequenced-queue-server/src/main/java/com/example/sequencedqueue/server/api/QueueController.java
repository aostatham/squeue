package com.example.sequencedqueue.server.api;

import static com.sequencedqueue.core.QueueDtos.*;

import java.util.List;
import java.util.UUID;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/queues/{queueName}")
public class QueueController {
    private final QueueFacade service;

    public QueueController(QueueFacade service) {
        this.service = service;
    }

    @PostMapping("/items")
    public EnqueueResponse enqueue(@PathVariable("queueName") String queueName, @RequestBody EnqueueRequest request) {
        return service.enqueue(queueName, request);
    }

    @PostMapping("/claims")
    public ClaimResponse claim(@PathVariable("queueName") String queueName, @RequestBody ClaimRequest request) {
        return service.claim(queueName, request);
    }

    @PostMapping("/items/{itemId}/complete")
    public ItemResponse complete(@PathVariable("queueName") String queueName, @PathVariable("itemId") UUID itemId, @RequestBody CompleteRequest request) {
        return service.complete(queueName, itemId, request);
    }

    @PostMapping("/items/{itemId}/fail")
    public ItemResponse fail(@PathVariable("queueName") String queueName, @PathVariable("itemId") UUID itemId, @RequestBody FailRequest request) {
        return service.fail(queueName, itemId, request);
    }

    @PostMapping("/leases/{leaseId}/heartbeat")
    public ResponseEntity<Void> heartbeat(@PathVariable("queueName") String queueName, @PathVariable("leaseId") UUID leaseId, @RequestBody HeartbeatRequest request) {
        service.heartbeat(queueName, leaseId, request);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/sources/{sourceId}/items")
    public List<ItemResponse> sourceItems(@PathVariable("queueName") String queueName, @PathVariable("sourceId") String sourceId) {
        return service.getSourceItems(queueName, sourceId);
    }

    @GetMapping("/items/{itemId}")
    public ItemResponse item(@PathVariable("queueName") String queueName, @PathVariable("itemId") UUID itemId) {
        return service.getItem(queueName, itemId);
    }
}
