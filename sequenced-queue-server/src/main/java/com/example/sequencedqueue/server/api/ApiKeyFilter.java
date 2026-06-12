package com.example.sequencedqueue.server.api;

import java.io.IOException;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
public class ApiKeyFilter extends OncePerRequestFilter {
    private final String apiKey;
    private final String adminApiKey;

    public ApiKeyFilter(
        @Value("${sequenced-queue.api-key}") String apiKey,
        @Value("${sequenced-queue.admin-api-key}") String adminApiKey
    ) {
        this.apiKey = apiKey;
        this.adminApiKey = adminApiKey;
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
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "missing bearer token");
            return;
        }

        if (path.startsWith("/admin/")) {
            if (!adminApiKey.equals(bearerToken)) {
                response.sendError(HttpServletResponse.SC_FORBIDDEN, "admin api key required");
                return;
            }
        } else if (!apiKey.equals(bearerToken) && !adminApiKey.equals(bearerToken)) {
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "invalid api key");
            return;
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
}
