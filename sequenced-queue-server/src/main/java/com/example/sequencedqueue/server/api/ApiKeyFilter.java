package com.example.sequencedqueue.server.api;

import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
public class ApiKeyFilter extends OncePerRequestFilter {
    public static final String ACTOR_ID_ATTRIBUTE = "sequencedQueue.actorId";

    private final String apiKey;
    private final String adminApiKey;
    private final ObjectMapper objectMapper;

    public ApiKeyFilter(
        @Value("${sequenced-queue.api-key}") String apiKey,
        @Value("${sequenced-queue.admin-api-key}") String adminApiKey,
        ObjectMapper objectMapper
    ) {
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalArgumentException("sequenced-queue.api-key must not be blank");
        }
        if (adminApiKey == null || adminApiKey.isBlank()) {
            throw new IllegalArgumentException("sequenced-queue.admin-api-key must not be blank");
        }
        if (apiKey.equals(adminApiKey)) {
            throw new IllegalArgumentException("sequenced-queue.api-key and sequenced-queue.admin-api-key must differ");
        }
        this.apiKey = apiKey;
        this.adminApiKey = adminApiKey;
        this.objectMapper = objectMapper;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
        throws ServletException, IOException {
        String path = applicationPath(request);
        if (!path.startsWith("/queues/") && !path.startsWith("/admin/")) {
            filterChain.doFilter(request, response);
            return;
        }

        String bearerToken = bearerToken(request);
        if (bearerToken == null || bearerToken.isBlank()) {
            writeError(response, HttpServletResponse.SC_UNAUTHORIZED, "UNAUTHORIZED", "missing bearer token");
            return;
        }

        if (path.startsWith("/admin/")) {
            if (!adminApiKey.equals(bearerToken)) {
                writeError(response, HttpServletResponse.SC_FORBIDDEN, "FORBIDDEN", "admin api key required");
                return;
            }
            request.setAttribute(ACTOR_ID_ATTRIBUTE, "admin-api-key");
        } else if (!apiKey.equals(bearerToken) && !adminApiKey.equals(bearerToken)) {
            writeError(response, HttpServletResponse.SC_FORBIDDEN, "FORBIDDEN", "invalid api key");
            return;
        } else {
            request.setAttribute(ACTOR_ID_ATTRIBUTE, adminApiKey.equals(bearerToken) ? "admin-api-key" : "api-key");
        }

        filterChain.doFilter(request, response);
    }

    private static String applicationPath(HttpServletRequest request) {
        String uri = request.getRequestURI();
        String contextPath = request.getContextPath();
        if (!contextPath.isBlank() && uri.startsWith(contextPath)) {
            return uri.substring(contextPath.length());
        }
        return uri;
    }

    private static String bearerToken(HttpServletRequest request) {
        String authorization = request.getHeader("Authorization");
        if (authorization == null || !authorization.startsWith("Bearer ")) {
            return null;
        }
        return authorization.substring("Bearer ".length());
    }

    private void writeError(HttpServletResponse response, int status, String errorCode, String message) throws IOException {
        response.setStatus(status);
        response.setContentType("application/json");
        objectMapper.writeValue(response.getWriter(), Map.of(
            "errorCode", errorCode,
            "message", message,
            "details", Map.of()
        ));
    }
}
