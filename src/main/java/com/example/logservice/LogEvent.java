package com.example.logservice;

import java.time.LocalDateTime;

/**
 * Model represent for one line of log after parsing.
 * Additional kafka metadata for duplicate check.
 */
public class LogEvent {

    private LocalDateTime timestamp;
    private String ip;
    private String method;
    private String path;
    private int status;
    private String rawLog;

    private String kafkaTopic;
    private int kafkaPartition;
    private long kafkaOffset;

    public LogEvent(
            LocalDateTime timestamp,
            String ip,
            String method,
            String path,
            int status,
            String rawLog
    ) {
        this.timestamp = timestamp;
        this.ip = ip;
        this.method = method;
        this.path = path;
        this.status = status;
        this.rawLog = rawLog;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public String getIp() {
        return ip;
    }

    public String getMethod() {
        return method;
    }

    public String getPath() {
        return path;
    }

    public int getStatus() {
        return status;
    }

    public String getRawLog() {
        return rawLog;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public void setKafkaTopic(String kafkaTopic) {
        this.kafkaTopic = kafkaTopic;
    }

    public int getKafkaPartition() {
        return kafkaPartition;
    }

    public void setKafkaPartition(int kafkaPartition) {
        this.kafkaPartition = kafkaPartition;
    }

    public long getKafkaOffset() {
        return kafkaOffset;
    }

    public void setKafkaOffset(long kafkaOffset) {
        this.kafkaOffset = kafkaOffset;
    }
}