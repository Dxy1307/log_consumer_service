package com.example.logservice;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import javax.sql.DataSource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Kafka Consumer
 */
public class LogConsumerServiceApp {

    public static void main(String[] args) {
        AppConfig config = AppConfig.load();

        DataSource dataSource = OracleDataSourceFactory.create(config);

        OracleStagingWriter stagingWriter = new OracleStagingWriter(dataSource);
        OracleBadLogWriter badLogWriter = new OracleBadLogWriter(dataSource);
        StagingMonitor stagingMonitor = new StagingMonitor(dataSource);

        FileLogger logger = new FileLogger("logs/consumer.txt", "CONSUMER");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Consumer app stopped.");
            logger.close();
        }));

        Stats stats = new Stats("CONSUMER",config.getStatsPrintIntervalMs(), logger);

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,config.getKafkaBootstrapServers());

        props.put(ConsumerConfig.GROUP_ID_CONFIG,config.getKafkaGroupId());

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        // disable auto commit
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");

        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,String.valueOf(config.getMaxPollRecords()));

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        int insertBatchSize = config.getMaxPollRecords();

        List<ConsumerRecord<String, String>> recordBuffer = new ArrayList<>();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(config.getKafkaTopic()));

            logger.info("Consumer app started. topic=" + config.getKafkaTopic()
                    + ", insertBatchSize=" + insertBatchSize);

            while (true) {
                try {
                    handleBackPressure(config,consumer,stagingMonitor,stats,logger);

                    // Only poll more Kafka records when current buffer is not enough for one DB batch.
                    if (recordBuffer.size() < insertBatchSize) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(config.getPollTimeoutMs()));

                        if (!records.isEmpty()) {
                            stats.addConsumerPolled(records.count());

                            for (ConsumerRecord<String, String> record : records) {
                                recordBuffer.add(record);
                            }
                        }
                    }

                    // Wait until exactly one full batch is available.
                    if (recordBuffer.size() < insertBatchSize) {
                        stats.printIfNeeded();
                        continue;
                    }

                    flushOneBatch(
                            recordBuffer,
                            insertBatchSize,
                            stagingWriter,
                            badLogWriter,
                            consumer,
                            config,
                            stats
                    );

                    stats.printIfNeeded();

                } catch (Exception e) {
                    // do not remove records from buffer and do not commit kafka offset if error occurs
                    // retry next loop; OracleStagingWriter uses MERGE, so retry is idempotent
                    logger.error("Kafka offset may not be committed. error=" + e.getMessage(), e);

                    stats.printIfNeeded();

                    sleep(1000);
                }
            }
        }
    }

    private static void flushOneBatch(
            List<ConsumerRecord<String, String>> recordBuffer,
            int insertBatchSize,
            OracleStagingWriter stagingWriter,
            OracleBadLogWriter badLogWriter,
            KafkaConsumer<String, String> consumer,
            AppConfig config,
            Stats stats
    ) throws Exception {

        List<ConsumerRecord<String, String>> batchRecords = new ArrayList<>(
                recordBuffer.subList(0, insertBatchSize)
        );

        List<LogEvent> validEvents = new ArrayList<>();
        List<OracleBadLogWriter.BadLog> badLogs = new ArrayList<>();

        for (ConsumerRecord<String, String> record : batchRecords) {
            try {
                LogEvent event = LogParser.parse(record.value());

                event.setKafkaTopic(record.topic());
                event.setKafkaPartition(record.partition());
                event.setKafkaOffset(record.offset());

                validEvents.add(event);

            } catch (Exception parseError) {
                OracleBadLogWriter.BadLog badLog = OracleBadLogWriter.fromRecord(record, parseError);

                badLogs.add(badLog);
            }
        }

        stats.addConsumerValid(validEvents.size());
        stats.addConsumerBad(badLogs.size());

        try {
            RetryUtil.runWithRetry(
                    () -> {
                        // insert valid log into staging table
                        stagingWriter.insertBatch(validEvents);

                        // insert bad log into bad_log table
                        badLogWriter.insertBadLogs(badLogs);

                        return null;
                    },
                    config.getRetryMaxAttempts(),
                    config.getRetryInitialBackoffMs(),
                    stats
            );

            stats.addStagingInsertSuccess(validEvents.size());

        } catch (Exception dbError) {
            // if insert DB failed, do not commit kafka offset
            stats.incStagingInsertFailed();

            throw dbError;
        }

        try {
            // Commit only offsets of records that were already inserted into DB.
            consumer.commitSync(buildOffsetCommitMap(batchRecords));

            stats.incKafkaCommitSuccess();

        } catch (Exception commitError) {
            stats.incKafkaCommitFailed();

            throw commitError;
        }

        // Remove from memory only after DB commit and Kafka offset commit both succeed.
        recordBuffer.subList(0, insertBatchSize).clear();
    }

    private static Map<TopicPartition, OffsetAndMetadata> buildOffsetCommitMap(
            List<ConsumerRecord<String, String>> records
    ) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

        for (ConsumerRecord<String, String> record : records) {
            TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());

            long nextOffset = record.offset() + 1;

            OffsetAndMetadata current = offsets.get(topicPartition);

            if (current == null || nextOffset > current.offset()) {
                offsets.put(topicPartition, new OffsetAndMetadata(nextOffset));
            }
        }

        return offsets;
    }

    private static void handleBackPressure(
            AppConfig config,
            KafkaConsumer<String, String> consumer,
            StagingMonitor stagingMonitor,
            Stats stats,
            FileLogger logger
    ) {
        try {
            long pending = stagingMonitor.countPending();

            if (pending > config.getStagingMaxPending()) {
                consumer.pause(consumer.assignment());

                stats.incBackpressurePauseCount();

                logger.info("Backpressure: consumer paused. pending=" + pending);

                sleep(1000);

            } else {
                consumer.resume(consumer.assignment());
            }

        } catch (Exception e) {
            logger.error("Backpressure: failed to check staging pending. error=" + e.getMessage(), e);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
