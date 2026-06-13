package com.example.sequencedqueue.server.api;

import static com.sequencedqueue.core.QueueDtos.*;

import java.util.List;
import java.util.UUID;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/admin/queues/{queueName}")
public class AdminController {
    private final QueueFacade service;

    public AdminController(QueueFacade service) {
        this.service = service;
    }

    @GetMapping("/blocked-sources")
    public List<BlockedSourceResponse> blockedSources(
        @PathVariable("queueName") String queueName,
        @RequestParam(name = "limit", defaultValue = "100") int limit,
        @RequestParam(name = "offset", defaultValue = "0") int offset
    ) {
        return service.inspectBlockedSources(queueName, limit, offset);
    }

    @GetMapping("/dead-lettered")
    public List<ItemResponse> deadLettered(
        @PathVariable("queueName") String queueName,
        @RequestParam(name = "limit", defaultValue = "100") int limit,
        @RequestParam(name = "offset", defaultValue = "0") int offset
    ) {
        return service.deadLetteredItems(queueName, limit, offset);
    }

    @GetMapping("/sources/{sourceId}/items")
    public List<ItemResponse> sourceItems(@PathVariable("queueName") String queueName, @PathVariable("sourceId") String sourceId) {
        return service.getSourceItems(queueName, sourceId);
    }

    @GetMapping("/items/{itemId}")
    public ItemResponse item(@PathVariable("queueName") String queueName, @PathVariable("itemId") UUID itemId) {
        return service.getItem(queueName, itemId);
    }

    @GetMapping("/audit")
    public List<AdminAuditResponse> audit(
        @PathVariable("queueName") String queueName,
        @RequestParam(name = "limit", defaultValue = "100") int limit,
        @RequestParam(name = "offset", defaultValue = "0") int offset
    ) {
        return service.adminAudit(queueName, limit, offset);
    }

    @PostMapping("/items/{itemId}/retry")
    public ItemResponse retry(@PathVariable("queueName") String queueName, @PathVariable("itemId") UUID itemId, @RequestBody(required = false) AdminActionRequest body, HttpServletRequest request) {
        return service.retry(queueName, itemId, actorId(request), reason(body));
    }

    @PostMapping("/items/{itemId}/skip")
    public ItemResponse skip(@PathVariable("queueName") String queueName, @PathVariable("itemId") UUID itemId, @RequestBody(required = false) AdminActionRequest body, HttpServletRequest request) {
        return service.skip(queueName, itemId, actorId(request), reason(body));
    }

    @PostMapping("/items/{itemId}/cancel")
    public ItemResponse cancel(@PathVariable("queueName") String queueName, @PathVariable("itemId") UUID itemId, @RequestBody(required = false) AdminActionRequest body, HttpServletRequest request) {
        return service.cancel(queueName, itemId, actorId(request), reason(body));
    }

    @PostMapping("/sources/{sourceId}/unblock")
    public SourceResponse unblockSource(@PathVariable("queueName") String queueName, @PathVariable("sourceId") String sourceId, @RequestBody(required = false) AdminActionRequest body, HttpServletRequest request) {
        return service.unblockSource(queueName, sourceId, actorId(request), reason(body));
    }

    private static String actorId(HttpServletRequest request) {
        Object value = request.getAttribute(ApiKeyFilter.ACTOR_ID_ATTRIBUTE);
        return value == null ? null : value.toString();
    }

    private static String reason(AdminActionRequest body) {
        return body == null ? null : body.reason();
    }

    public record AdminActionRequest(String reason) {
    }
}
