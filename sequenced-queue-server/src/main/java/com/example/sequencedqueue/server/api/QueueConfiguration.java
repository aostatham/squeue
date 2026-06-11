package com.example.sequencedqueue.server.api;

import javax.sql.DataSource;

import com.sequencedqueue.core.QueueCoreFactory;
import com.sequencedqueue.core.QueueOperations;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class QueueConfiguration {
    @Bean
    QueueOperations queueOperations(
        DataSource dataSource,
        ObjectMapper objectMapper,
        @Value("${sequenced-queue.default-lease-seconds:60}") int defaultLeaseSeconds,
        @Value("${sequenced-queue.default-max-attempts:5}") int defaultMaxAttempts
    ) {
        return QueueCoreFactory.create(dataSource, objectMapper, defaultLeaseSeconds, defaultMaxAttempts);
    }
}
