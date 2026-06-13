package com.example.sequencedqueue.server.api;

import java.util.LinkedHashMap;

import com.sequencedqueue.core.QueueException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(QueueException.class)
    ResponseEntity<LinkedHashMap<String, Object>> queueException(QueueException ex) {
        HttpStatus status = HttpStatus.valueOf(ex.statusCode());
        return ResponseEntity.status(status).body(errorBody(errorCode(ex), responseMessage(ex), null, null, null));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<LinkedHashMap<String, Object>> invalidBody(HttpMessageNotReadableException ex) {
        return ResponseEntity.badRequest().body(errorBody("VALIDATION_ERROR", "invalid request body", null, null, null));
    }

    @ExceptionHandler(Exception.class)
    ResponseEntity<LinkedHashMap<String, Object>> unexpected(Exception ex) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorBody("INTERNAL_ERROR", "internal server error", null, null, null));
    }

    private static String errorCode(QueueException ex) {
        String message = ex.getMessage() == null ? "" : ex.getMessage().toLowerCase();
        if (ex.statusCode() == QueueException.BAD_REQUEST) {
            return "VALIDATION_ERROR";
        }
        if (ex.statusCode() == QueueException.NOT_FOUND) {
            if (message.contains("source")) {
                return "SOURCE_NOT_FOUND";
            }
            return "ITEM_NOT_FOUND";
        }
        if (ex.statusCode() == QueueException.CONFLICT) {
            if (message.contains("expired")) {
                return "LEASE_EXPIRED";
            }
            if (message.contains("lease")) {
                return "LEASE_LOST";
            }
            if (message.contains("not processing")) {
                return "ITEM_NOT_PROCESSING";
            }
            if (message.contains("blocked") || message.contains("blocking")) {
                return "SOURCE_BLOCKED";
            }
            if (message.contains("idempotency")) {
                return "IDEMPOTENCY_CONFLICT";
            }
            return "QUEUE_CONFLICT";
        }
        return "INTERNAL_ERROR";
    }

    private static String responseMessage(QueueException ex) {
        if (ex.statusCode() >= 500) {
            return "internal server error";
        }
        return ex.getMessage();
    }

    private static LinkedHashMap<String, Object> errorBody(String errorCode, String message, String queueName, String sourceId, String itemId) {
        LinkedHashMap<String, Object> body = new LinkedHashMap<>();
        body.put("errorCode", errorCode);
        body.put("message", message);
        if (queueName != null) {
            body.put("queueName", queueName);
        }
        if (sourceId != null) {
            body.put("sourceId", sourceId);
        }
        if (itemId != null) {
            body.put("itemId", itemId);
        }
        body.put("details", new LinkedHashMap<>());
        return body;
    }
}
