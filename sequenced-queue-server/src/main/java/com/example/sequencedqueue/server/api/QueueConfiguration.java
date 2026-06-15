package com.example.sequencedqueue.server.api;

import javax.sql.DataSource;

import com.sequencedqueue.core.QueueCoreFactory;
import com.sequencedqueue.core.QueueOperations;
import com.sequencedqueue.core.QueueSettings;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring wiring for the shared core queue service.
 */
@Configuration
public class QueueConfiguration {
    /**
     * Creates the core queue service from Spring configuration properties.
     */
    @Bean
    QueueOperations queueOperations(
        DataSource dataSource,
        ObjectMapper objectMapper,
        @Value("${sequenced-queue.default-lease-seconds:60}") int defaultLeaseSeconds,
        @Value("${sequenced-queue.max-lease-seconds:600}") int maxLeaseSeconds,
        @Value("${sequenced-queue.default-max-attempts:5}") int defaultMaxAttempts,
        @Value("${sequenced-queue.max-payload-bytes:262144}") int maxPayloadBytes,
        @Value("${sequenced-queue.max-headers-bytes:32768}") int maxHeadersBytes,
        @Value("${sequenced-queue.max-result-bytes:262144}") int maxResultBytes,
        @Value("${sequenced-queue.max-error-type-bytes:256}") int maxErrorTypeBytes,
        @Value("${sequenced-queue.max-error-message-bytes:8192}") int maxErrorMessageBytes,
        @Value("${sequenced-queue.max-admin-reason-bytes:2048}") int maxAdminReasonBytes,
        @Value("${sequenced-queue.max-admin-metadata-bytes:32768}") int maxAdminMetadataBytes,
        @Value("${sequenced-queue.max-retention-purge-batch-size:10000}") int maxRetentionPurgeBatchSize
    ) {
        QueueSettings settings = new QueueSettings(
            defaultLeaseSeconds,
            maxLeaseSeconds,
            defaultMaxAttempts,
            maxPayloadBytes,
            maxHeadersBytes,
            maxResultBytes,
            maxErrorTypeBytes,
            maxErrorMessageBytes,
            maxAdminReasonBytes,
            maxAdminMetadataBytes,
            maxRetentionPurgeBatchSize
        );
        return QueueCoreFactory.create(dataSource, objectMapper, settings);
    }
}
