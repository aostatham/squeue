package com.example.sequencedqueue.server;

import java.util.Arrays;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SequencedQueueServerApplication {
    public static void main(String[] args) {
        if (Arrays.stream(args).anyMatch(arg -> "--help".equals(arg) || "-h".equals(arg))) {
            System.out.println("""
                sequenced-queue-server

                Required runtime configuration:
                  SPRING_DATASOURCE_URL
                  SPRING_DATASOURCE_USERNAME
                  SPRING_DATASOURCE_PASSWORD
                  SEQUENCED_QUEUE_API_KEY
                  SEQUENCED_QUEUE_ADMIN_API_KEY

                Example:
                  docker run --rm -p 8080:8080 \\
                    -e SPRING_DATASOURCE_URL=jdbc:postgresql://host.docker.internal:5432/sequenced_queue \\
                    -e SPRING_DATASOURCE_USERNAME=sequenced_queue \\
                    -e SPRING_DATASOURCE_PASSWORD=sequenced_queue \\
                    -e SEQUENCED_QUEUE_API_KEY=replace-worker-key \\
                    -e SEQUENCED_QUEUE_ADMIN_API_KEY=replace-admin-key \\
                    sequenced-queue-server:0.1.0
                """);
            return;
        }
        SpringApplication.run(SequencedQueueServerApplication.class, args);
    }
}
