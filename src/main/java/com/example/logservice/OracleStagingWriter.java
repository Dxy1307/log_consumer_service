package com.example.logservice;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;

/**
 * insert log to LOG_EVENT_STG.
 * anti-duplication by MERGE script
 * commit DB transaction
 */
public class OracleStagingWriter {

    private final DataSource dataSource;

    public OracleStagingWriter(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void insertBatch(List<LogEvent> events) throws Exception {
        if (events == null || events.isEmpty()) {
            return;
        }

        String sql = """
            MERGE INTO LOG_EVENT_STG TARGET
            USING (
                SELECT
                    ? AS KAFKA_TOPIC,
                    ? AS KAFKA_PARTITION,
                    ? AS KAFKA_OFFSET,
                    ? AS EVENT_TIME,
                    ? AS IP,
                    ? AS METHOD,
                    ? AS PATH,
                    ? AS STATUS,
                    ? AS RAW_LOG
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
                    EVENT_TIME,
                    IP,
                    METHOD,
                    PATH,
                    STATUS,
                    RAW_LOG,
                    PROCESS_STATUS,
                    CREATED_AT
                )
                VALUES (
                    SOURCE.KAFKA_TOPIC,
                    SOURCE.KAFKA_PARTITION,
                    SOURCE.KAFKA_OFFSET,
                    SOURCE.EVENT_TIME,
                    SOURCE.IP,
                    SOURCE.METHOD,
                    SOURCE.PATH,
                    SOURCE.STATUS,
                    SOURCE.RAW_LOG,
                    'NEW',
                    SYSTIMESTAMP
                )
            """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            conn.setAutoCommit(false);

            try {
                for (LogEvent event : events) {
                    ps.setString(1, event.getKafkaTopic());
                    ps.setInt(2, event.getKafkaPartition());
                    ps.setLong(3, event.getKafkaOffset());
                    ps.setTimestamp(4, Timestamp.valueOf(event.getTimestamp()));
                    ps.setString(5, event.getIp());
                    ps.setString(6, event.getMethod());
                    ps.setString(7, event.getPath());
                    ps.setInt(8, event.getStatus());
                    ps.setString(9, event.getRawLog());

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
}