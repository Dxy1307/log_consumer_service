package com.example.logservice;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;

/**
 * Create connection pool to Oracle by HikariCP.
 */
public class OracleDataSourceFactory {

    public static DataSource create(AppConfig config) {
        HikariConfig hikariConfig = new HikariConfig();

        hikariConfig.setJdbcUrl(config.getOracleJdbcUrl());
        hikariConfig.setUsername(config.getOracleUsername());
        hikariConfig.setPassword(config.getOraclePassword());

        hikariConfig.setMaximumPoolSize(config.getOraclePoolSize());
        hikariConfig.setMinimumIdle(2);

        hikariConfig.setPoolName("oracle-log-service-pool");

        hikariConfig.setConnectionTimeout(30000);
        hikariConfig.setIdleTimeout(600000);
        hikariConfig.setMaxLifetime(1800000);

        return new HikariDataSource(hikariConfig);
    }
}