package com.example.sequencedqueue.server.api;

import java.util.Map;

import com.example.sequencedqueue.server.core.QueueException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {
    @ExceptionHandler(QueueException.class)
    ResponseEntity<Map<String, Object>> queueException(QueueException ex) {
        HttpStatus status = HttpStatus.valueOf(ex.statusCode());
        return ResponseEntity.status(status).body(Map.of(
            "error", status.getReasonPhrase(),
            "message", ex.getMessage()
        ));
    }
}
