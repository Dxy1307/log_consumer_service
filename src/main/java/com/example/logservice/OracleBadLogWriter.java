package com.example.logservice;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * If parse failed, not throw log away
 * insert to LOG_EVENT_BAD, after that commit kafka offset.
 */
public class OracleBadLogWriter {

    private final DataSource dataSource;

    public OracleBadLogWriter(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void insertBadLogs(List<BadLog> badLogs) throws Exception {
        if (badLogs == null || badLogs.isEmpty()) {
            return;
        }

        String sql = """
            MERGE INTO LOG_EVENT_BAD TARGET
            USING (
                SELECT
                    ? AS KAFKA_TOPIC,
                    ? AS KAFKA_PARTITION,
                    ? AS KAFKA_OFFSET,
                    ? AS RAW_LOG,
                    ? AS ERROR_MESSAGE
                FROM DUAL
            ) SOURCE
            ON (
                TARGET.KAFKA_TOPIC = SOURCE.KAFKA_TOPIC
                AND TARGET.KAFKA_PARTITION = SOURCE.KAFKA_PARTITION
                AND TARGET.KAFKA_OFFSET = SOURCE.KAFKA_OFFSET
            )
            WHEN NOT MATCHED THEN
                INSERT (
                    KAFKA_TOPIC,
                    KAFKA_PARTITION,
                    KAFKA_OFFSET,
                    RAW_LOG,
                    ERROR_MESSAGE,
                    CREATED_AT
                )
                VALUES (
                    SOURCE.KAFKA_TOPIC,
                    SOURCE.KAFKA_PARTITION,
                    SOURCE.KAFKA_OFFSET,
                    SOURCE.RAW_LOG,
                    SOURCE.ERROR_MESSAGE,
                    SYSTIMESTAMP
                )
            """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);

            try {
                for (BadLog badLog : badLogs) {
                    ps.setString(1, badLog.kafkaTopic());
                    ps.setInt(2, badLog.kafkaPartition());
                    ps.setLong(3, badLog.kafkaOffset());
                    ps.setString(4, badLog.rawLog());
                    ps.setString(5, trim(badLog.errorMessage(), 1000));

                    ps.addBatch();
                }

                ps.executeBatch();
                conn.commit();

            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
    }

    private String trim(String value, int maxLength) {
        if (value == null) {
            return null;
        }

        return value.length() <= maxLength ? value : value.substring(0, maxLength);
    }

    public static BadLog fromRecord(ConsumerRecord<String, String> record, Exception e) {
        return new BadLog(record.topic(),record.partition(),record.offset(),record.value(),e.getMessage());
    }

    public record BadLog(
            String kafkaTopic,
            int kafkaPartition,
            long kafkaOffset,
            String rawLog,
            String errorMessage
    ) {
    }
}