package com.example.sequencedqueue.server.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.yaml.snakeyaml.Yaml;

class OpenApiContractTest {
    @Test
    void publicControllerRoutesMatchOpenApiPaths() throws Exception {
        Map<String, Object> openApi = openApi();
        Map<String, Object> paths = map(openApi.get("paths"));
        Set<Route> documented = new LinkedHashSet<>();
        for (var path : paths.entrySet()) {
            for (String method : map(path.getValue()).keySet()) {
                documented.add(new Route(method.toUpperCase(), path.getKey()));
            }
        }

        Set<Route> implemented = implementedRoutes(QueueController.class, AdminController.class);

        assertEquals(implemented, documented);
    }

    @Test
    void openApiContainsSecuritySchemesAndTypedResponseObjects() throws Exception {
        Map<String, Object> components = map(openApi().get("components"));
        Map<String, Object> securitySchemes = map(components.get("securitySchemes"));
        assertTrue(securitySchemes.containsKey("WorkerApiKey"));
        assertTrue(securitySchemes.containsKey("AdminApiKey"));

        Map<String, Object> responses = map(components.get("responses"));
        assertNotNull(map(responses.get("Unauthorized")).get("content"));
        assertNotNull(map(responses.get("Forbidden")).get("content"));

        Map<String, Object> schemas = map(components.get("schemas"));
        assertStrongObjectSchema(schemas, "ItemResponse", "itemId", "status", "updatedAt");
        assertStrongObjectSchema(schemas, "SourceResponse", "queueName", "status", "updatedAt");
        assertEquals(List.of("pending", "processing", "succeeded", "retry_wait", "failed", "dead_lettered", "cancelled", "skipped"), map(schemas.get("ItemStatus")).get("enum"));
        assertEquals(List.of("idle", "leased", "blocked"), map(schemas.get("SourceStatus")).get("enum"));
    }

    @Test
    void queueAndAdminOperationsDeclareExpectedSecurityAndAuthResponses() throws Exception {
        Map<String, Object> paths = map(openApi().get("paths"));
        for (var pathEntry : paths.entrySet()) {
            for (var operationEntry : map(pathEntry.getValue()).entrySet()) {
                Map<String, Object> operation = map(operationEntry.getValue());
                List<Object> security = list(operation.get("security"));
                if (pathEntry.getKey().startsWith("/admin/")) {
                    assertEquals(List.of(Map.of("AdminApiKey", List.of())), security, pathEntry.getKey());
                } else {
                    assertEquals(List.of(Map.of("WorkerApiKey", List.of()), Map.of("AdminApiKey", List.of())), security, pathEntry.getKey());
                }

                Map<String, Object> responses = map(operation.get("responses"));
                assertTrue(responses.containsKey("401"), pathEntry.getKey());
                assertTrue(responses.containsKey("403"), pathEntry.getKey());
            }
        }
    }

    private static void assertStrongObjectSchema(Map<String, Object> schemas, String schemaName, String... expectedProperties) {
        Map<String, Object> schema = map(schemas.get(schemaName));
        assertEquals("object", schema.get("type"));
        assertFalse(Boolean.TRUE.equals(schema.get("additionalProperties")), schemaName);
        Map<String, Object> properties = map(schema.get("properties"));
        for (String expectedProperty : expectedProperties) {
            assertTrue(properties.containsKey(expectedProperty), schemaName + "." + expectedProperty);
        }
    }

    private static Set<Route> implementedRoutes(Class<?>... controllers) {
        Set<Route> routes = new LinkedHashSet<>();
        for (Class<?> controller : controllers) {
            String basePath = firstValue(controller.getAnnotation(RequestMapping.class).value());
            for (Method method : controller.getDeclaredMethods()) {
                GetMapping get = method.getAnnotation(GetMapping.class);
                if (get != null) {
                    routes.add(new Route("GET", normalize(basePath, firstValue(get.value()))));
                }
                PostMapping post = method.getAnnotation(PostMapping.class);
                if (post != null) {
                    routes.add(new Route("POST", normalize(basePath, firstValue(post.value()))));
                }
            }
        }
        return routes;
    }

    private static String firstValue(String[] values) {
        return values.length == 0 ? "" : values[0];
    }

    private static String normalize(String basePath, String methodPath) {
        if (methodPath.isBlank()) {
            return basePath;
        }
        return basePath + (methodPath.startsWith("/") ? methodPath : "/" + methodPath);
    }

    private static Map<String, Object> openApi() throws Exception {
        try (var reader = Files.newBufferedReader(repoRoot().resolve("docs/openapi.yaml"))) {
            return map(new Yaml().load(reader));
        }
    }

    private static Path repoRoot() {
        Path current = Path.of("").toAbsolutePath();
        return current.getFileName().toString().equals("sequenced-queue-server") ? current.getParent() : current;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object value) {
        return value == null ? new LinkedHashMap<>() : (Map<String, Object>) value;
    }

    @SuppressWarnings("unchecked")
    private static List<Object> list(Object value) {
        return (List<Object>) value;
    }

    private record Route(String method, String path) {
    }
}
