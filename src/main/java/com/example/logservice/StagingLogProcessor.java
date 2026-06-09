package com.example.logservice;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Processing data from staging table to main table.
 */
public class StagingLogProcessor {

    private final DataSource dataSource;
    private final FileLogger logger;

    public StagingLogProcessor(DataSource dataSource) {
        this(dataSource, null);
    }

    public StagingLogProcessor(DataSource dataSource, FileLogger logger) {
        this.dataSource = dataSource;
        this.logger = logger;
    }

    public int processOnce(int batchSize) throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try {
                List<LockedRow> rows = lockNewRows(conn, batchSize);

                // Wait until staging has enough rows for one full batch.
                // Example: batchSize=1000 means only move when there are exactly 1000 locked rows.
                if (rows.size() < batchSize) {
                    conn.commit();
                    return 0;
                }

                markProcessing(conn, rows);

                insertToMainTable(conn, rows);

                markDone(conn, rows);

                conn.commit();

                return rows.size();

            } catch (Exception e) {
                conn.rollback();
                throw e;
            }
        }
    }

    private List<LockedRow> lockNewRows(Connection conn, int batchSize) throws Exception {
        String sql = """
            SELECT ROWID AS RID, ID
            FROM LOG_EVENT_STG
            WHERE PROCESS_STATUS = 'NEW'
              AND ROWNUM <= ?
            FOR UPDATE SKIP LOCKED
            """;

        List<LockedRow> rows = new ArrayList<>();

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, batchSize);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    rows.add(new LockedRow(
                        rs.getString("RID"),
                        rs.getLong("ID")
                    ));
                }
            }
        }

        return rows;
    }

    private void markProcessing(Connection conn, List<LockedRow> rows) throws Exception {
        String sql = """
            UPDATE LOG_EVENT_STG
            SET PROCESS_STATUS = 'PROCESSING',
                UPDATED_AT = SYSTIMESTAMP
            WHERE ROWID = ?
            """;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (LockedRow row : rows) {
                ps.setString(1, row.rowId());
                ps.addBatch();
            }

            ps.executeBatch();
        }
    }

    private void insertToMainTable(Connection conn, List<LockedRow> rows) throws Exception {
        String placeholders = buildPlaceholders(rows.size());

        String sql = """
            INSERT INTO LOG_EVENT (
                EVENT_TIME,
                IP,
                METHOD,
                PATH,
                STATUS,
                CREATED_AT
            )
            SELECT
                EVENT_TIME,
                IP,
                METHOD,
                PATH,
                STATUS,
                SYSTIMESTAMP
            FROM LOG_EVENT_STG
            WHERE ID IN (
            """ + placeholders + """
            )
            """;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            bindIds(ps, rows);
            ps.executeUpdate();
        }
    }

    private void markDone(Connection conn, List<LockedRow> rows) throws Exception {
        String sql = """
            UPDATE LOG_EVENT_STG
            SET PROCESS_STATUS = 'DONE',
                UPDATED_AT = SYSTIMESTAMP
            WHERE ROWID = ?
            """;

        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (LockedRow row : rows) {
                ps.setString(1, row.rowId());
                ps.addBatch();
            }

            ps.executeBatch();
        }
    }

    public int recoverProcessingRows() throws Exception {
        // if app crash when status is PROCESSING
        // after 10 minutes, reset to NEW to process again
        String sql = """
            UPDATE LOG_EVENT_STG
            SET PROCESS_STATUS = 'NEW',
                UPDATED_AT = SYSTIMESTAMP
            WHERE PROCESS_STATUS = 'PROCESSING'
              AND UPDATED_AT < SYSTIMESTAMP - INTERVAL '10' MINUTE
            """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            int rows = ps.executeUpdate();

            if (rows > 0 && logger != null) {
                logger.info("Recovered PROCESSING rows. rows=" + rows);
            }

            return rows;
        }
    }

    private String buildPlaceholders(int size) {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < size; i++) {
            if (i > 0) {
                sb.append(",");
            }

            sb.append("?");
        }

        return sb.toString();
    }

    private void bindIds(PreparedStatement ps, List<LockedRow> rows) throws Exception {
        for (int i = 0; i < rows.size(); i++) {
            ps.setLong(i + 1, rows.get(i).id());
        }
    }

    private record LockedRow(String rowId, Long id) {
    }
}