package com.example.sequencedqueue.server.api;

import static com.example.sequencedqueue.server.api.ApiDtos.*;

import java.util.List;
import java.util.UUID;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/admin/queues/{queueName}")
public class AdminController {
    private final QueueFacade service;

    public AdminController(QueueFacade service) {
        this.service = service;
    }

    @GetMapping("/blocked-sources")
    public List<SourceResponse> blockedSources(@PathVariable String queueName) {
        return service.blockedSources(queueName);
    }

    @PostMapping("/items/{itemId}/retry")
    public ItemResponse retry(@PathVariable String queueName, @PathVariable UUID itemId) {
        return service.retry(queueName, itemId);
    }

    @PostMapping("/items/{itemId}/skip")
    public ItemResponse skip(@PathVariable String queueName, @PathVariable UUID itemId) {
        return service.skip(queueName, itemId);
    }

    @PostMapping("/items/{itemId}/cancel")
    public ItemResponse cancel(@PathVariable String queueName, @PathVariable UUID itemId) {
        return service.cancel(queueName, itemId);
    }

    @PostMapping("/sources/{sourceId}/unblock")
    public SourceResponse unblockSource(@PathVariable String queueName, @PathVariable String sourceId) {
        return service.unblockSource(queueName, sourceId);
    }
}
