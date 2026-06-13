package com.example.sequencedqueue.examples;

import java.util.Map;

import com.sequencedqueue.direct.DirectQueueResult;
import com.sequencedqueue.direct.SequencedQueueDirectClient;
import com.sequencedqueue.direct.SequencedQueueDirectWorker;
import org.postgresql.ds.PGSimpleDataSource;

public final class JavaDirectWorkerExample {
    private JavaDirectWorkerExample() {
    }

    public static void main(String[] args) throws InterruptedException {
        run(configFromEnv());
    }

    public static boolean runOnce(SequencedQueueDirectClient client, String queueName, String workerId, String itemType, int leaseSeconds) {
        return worker(client, queueName, workerId, itemType, leaseSeconds).runOnce();
    }

    public static void run(Config config) throws InterruptedException {
        SequencedQueueDirectClient client = SequencedQueueDirectClient.builder()
            .dataSource(dataSource(config))
            .validateSchemaOnBuild(true)
            .build();

        SequencedQueueDirectWorker worker = worker(client, config.queueName(), config.workerId(), config.itemType(), config.leaseSeconds());
        if (config.runOnce()) {
            worker.runOnce();
            return;
        }

        Runtime.getRuntime().addShutdownHook(new Thread(worker::stop));
        worker.runForever();
    }

    private static SequencedQueueDirectWorker worker(SequencedQueueDirectClient client, String queueName, String workerId, String itemType, int leaseSeconds) {
        return client.worker(queueName)
            .workerId(workerId)
            .leaseSeconds(leaseSeconds)
            .handler(itemType, item -> {
                System.out.printf("java direct worker handled itemId=%s sequenceNo=%d payload=%s%n",
                    item.itemId(), item.sequenceNo(), item.payload());
                return DirectQueueResult.success(Map.of("handledBy", workerId));
            })
            .build();
    }

    private static Config configFromEnv() {
        String queueName = env("SQ_QUEUE", "wf.commands");
        String workerId = env("SQ_WORKER_ID", "java-direct-worker");
        String itemType = env("SQ_ITEM_TYPE", "example.command");
        int leaseSeconds = Integer.parseInt(env("SQ_LEASE_SECONDS", "30"));
        boolean runOnce = Boolean.parseBoolean(env("SQ_RUN_ONCE", "false"));
        return new Config(
            env("DATABASE_URL", "jdbc:postgresql://localhost:5432/sequenced_queue"),
            env("DATABASE_USERNAME", "sequenced_queue"),
            env("DATABASE_PASSWORD", "sequenced_queue"),
            queueName,
            workerId,
            itemType,
            leaseSeconds,
            runOnce
        );
    }

    private static PGSimpleDataSource dataSource(Config config) {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(config.databaseUrl());
        dataSource.setUser(config.databaseUsername());
        dataSource.setPassword(config.databasePassword());
        return dataSource;
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    public record Config(String databaseUrl, String databaseUsername, String databasePassword, String queueName, String workerId, String itemType, int leaseSeconds, boolean runOnce) {
    }
}
