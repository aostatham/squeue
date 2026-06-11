package com.sequencedqueue.postgres;

import java.time.OffsetDateTime;

public record EnqueueRequest(
    String sourceId,
    String itemType,
    String idempotencyKey,
    String payloadJson,
    String headersJson,
    OffsetDateTime availableAt,
    Integer maxAttempts
) {
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String sourceId;
        private String itemType;
        private String idempotencyKey;
        private String payloadJson = "{}";
        private String headersJson = "{}";
        private OffsetDateTime availableAt;
        private Integer maxAttempts;

        public Builder sourceId(String sourceId) {
            this.sourceId = sourceId;
            return this;
        }

        public Builder itemType(String itemType) {
            this.itemType = itemType;
            return this;
        }

        public Builder idempotencyKey(String idempotencyKey) {
            this.idempotencyKey = idempotencyKey;
            return this;
        }

        public Builder payloadJson(String payloadJson) {
            this.payloadJson = payloadJson;
            return this;
        }

        public Builder headersJson(String headersJson) {
            this.headersJson = headersJson;
            return this;
        }

        public Builder availableAt(OffsetDateTime availableAt) {
            this.availableAt = availableAt;
            return this;
        }

        public Builder maxAttempts(Integer maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        public EnqueueRequest build() {
            return new EnqueueRequest(sourceId, itemType, idempotencyKey, payloadJson, headersJson, availableAt, maxAttempts);
        }
    }
}
