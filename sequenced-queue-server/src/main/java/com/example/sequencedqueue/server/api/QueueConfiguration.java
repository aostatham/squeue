package com.example.sequencedqueue.server.api;

import com.example.sequencedqueue.server.core.QueueRepository;
import com.example.sequencedqueue.server.core.QueueService;
import com.example.sequencedqueue.server.core.RetryPolicy;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class QueueConfiguration {
    @Bean
    RetryPolicy retryPolicy() {
        return new RetryPolicy();
    }

    @Bean
    QueueService queueService(
        QueueRepository repository,
        RetryPolicy retryPolicy,
        ObjectMapper objectMapper,
        @Value("${sequenced-queue.default-lease-seconds:60}") int defaultLeaseSeconds,
        @Value("${sequenced-queue.default-max-attempts:5}") int defaultMaxAttempts
    ) {
        return new QueueService(repository, retryPolicy, objectMapper, defaultLeaseSeconds, defaultMaxAttempts);
    }
}
