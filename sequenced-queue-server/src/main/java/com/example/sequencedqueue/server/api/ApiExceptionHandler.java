package com.example.sequencedqueue.server.api;

import java.util.LinkedHashMap;

import com.sequencedqueue.core.QueueException;
import com.sequencedqueue.core.QueueFieldTooLargeException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * Translates server exceptions into stable JSON REST error bodies.
 */
@RestControllerAdvice
public class ApiExceptionHandler {
    /**
     * Converts core queue exceptions into their corresponding HTTP responses.
     */
    @ExceptionHandler(QueueException.class)
    ResponseEntity<LinkedHashMap<String, Object>> queueException(QueueException ex) {
        HttpStatus status = HttpStatus.valueOf(ex.statusCode());
        LinkedHashMap<String, Object> body = errorBody(ex.errorCode().name(), responseMessage(ex), ex.queueName(), ex.sourceId(), ex.itemId() == null ? null : ex.itemId().toString());
        if (ex instanceof QueueFieldTooLargeException tooLarge) {
            body.put("fieldName", tooLarge.fieldName());
            body.put("maxBytes", tooLarge.maxBytes());
            body.put("actualBytes", tooLarge.actualBytes());
        }
        return ResponseEntity.status(status).body(body);
    }

    /**
     * Handles malformed JSON or unreadable request bodies.
     */
    @ExceptionHandler(HttpMessageNotReadableException.class)
    ResponseEntity<LinkedHashMap<String, Object>> invalidBody(HttpMessageNotReadableException ex) {
        return ResponseEntity.badRequest().body(errorBody("VALIDATION_ERROR", "invalid request body", null, null, null));
    }

    /**
     * Handles unexpected failures without exposing internal exception details.
     */
    @ExceptionHandler(Exception.class)
    ResponseEntity<LinkedHashMap<String, Object>> unexpected(Exception ex) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(errorBody("INTERNAL_ERROR", "internal server error", null, null, null));
    }

    /**
     * Returns a safe response message for a queue exception.
     */
    private static String responseMessage(QueueException ex) {
        if (ex.statusCode() >= 500) {
            return "internal server error";
        }
        return ex.getMessage();
    }

    /**
     * Builds the standard REST error body with optional safe queue context.
     */
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
