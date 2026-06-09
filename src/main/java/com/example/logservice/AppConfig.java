package com.example.logservice;

import java.io.InputStream;
import java.util.Properties;

/**
 * Read config from application.properties.
 */
public class AppConfig {

    private final Properties props;

    private AppConfig(Properties props) {
        this.props = props;
    }

    public static AppConfig load() {
        try {
            Properties props = new Properties();

            try (InputStream input = AppConfig.class
                    .getClassLoader()
                    .getResourceAsStream("application.properties")) {

                if (input == null) {
                    throw new IllegalStateException("Cannot find application.properties");
                }

                props.load(input);
            }

            return new AppConfig(props);

        } catch (Exception e) {
            throw new RuntimeException("Failed to load config", e);
        }
    }

    public String getKafkaBootstrapServers() {
        return props.getProperty("kafka.bootstrap.servers");
    }

    public String getKafkaTopic() {
        return props.getProperty("kafka.topic");
    }

    public String getKafkaGroupId() {
        return props.getProperty("kafka.group.id");
    }

    public int getMaxPollRecords() {
        return Integer.parseInt(props.getProperty("kafka.max.poll.records", "500"));
    }

    public long getPollTimeoutMs() {
        return Long.parseLong(props.getProperty("kafka.poll.timeout.ms", "1000"));
    }

    public String getOracleJdbcUrl() {
        return props.getProperty("oracle.jdbc.url");
    }

    public String getOracleUsername() {
        return props.getProperty("oracle.username");
    }

    public String getOraclePassword() {
        return props.getProperty("oracle.password");
    }

    public int getOraclePoolSize() {
        return Integer.parseInt(props.getProperty("oracle.pool.size", "10"));
    }

    public int getRetryMaxAttempts() {
        return Integer.parseInt(props.getProperty("consumer.retry.max.attempts", "3"));
    }

    public long getRetryInitialBackoffMs() {
        return Long.parseLong(props.getProperty("consumer.retry.initial.backoff.ms", "500"));
    }

    public long getStagingMaxPending() {
        return Long.parseLong(props.getProperty("staging.max.pending", "100000"));
    }

    public int getProcessorBatchSize() {
        return Integer.parseInt(props.getProperty("processor.batch.size", "1000"));
    }

    public long getProcessorSleepMs() {
        return Long.parseLong(props.getProperty("processor.sleep.ms", "1000"));
    }

    public int getProducerTps() {
        return Integer.parseInt(props.getProperty("producer.tps", "1000"));
    }

    public String getProducerClientId() {
        return props.getProperty("producer.client.id", "log-producer");
    }

    public String getProducerAcks() {
        return props.getProperty("producer.acks", "all");
    }

    public int getProducerLingerMs() {
        return Integer.parseInt(props.getProperty("producer.linger.ms", "10"));
    }

    public int getProducerBatchSize() {
        return Integer.parseInt(props.getProperty("producer.batch.size", "32768"));
    }

    public long getProducerBufferMemory() {
        return Long.parseLong(props.getProperty("producer.buffer.memory", "67108864"));
    }

    public long getStatsPrintIntervalMs() {
        return Long.parseLong(props.getProperty("stats.print.interval.ms", "5000"));
    }

    // add to test insert when 1000
    public int getConsumerInsertBatchSize() {
        return Integer.parseInt(props.getProperty("consumer.insert.batch.size", "1000"));
    }

    public long getConsumerInsertFlushTimeoutMs() {
        return Long.parseLong(props.getProperty("consumer.insert.flush.timeout.ms", "5000"));
    }
}