package com.example.logservice;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * check backlog in staging table.
 * if pending too large, consumer can pause to anti-duplication.
 */
public class StagingMonitor {

    private final DataSource dataSource;

    public StagingMonitor(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public long countPending() throws Exception {
        String sql = """
            SELECT COUNT(*)
            FROM LOG_EVENT_STG
            WHERE PROCESS_STATUS = 'NEW'
            """;

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            if (rs.next()) {
                return rs.getLong(1);
            }

            return 0;
        }
    }
}