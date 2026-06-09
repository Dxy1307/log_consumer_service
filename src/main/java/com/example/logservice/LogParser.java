package com.example.logservice;

import java.time.LocalDateTime;

/**
 * Parse log to LogEvent with format:
 * 2026-06-08T10:15:30 192.168.1.10 GET /api/users 200
 */
public class LogParser {

    public static LogEvent parse(String rawLog) {
        if (rawLog == null || rawLog.isBlank()) {
            throw new IllegalArgumentException("Raw log is empty");
        }

        String[] parts = rawLog.trim().split("\\s+");

        if (parts.length != 5) {
            throw new IllegalArgumentException("Invalid log format: " + rawLog);
        }

        LocalDateTime timestamp = LocalDateTime.parse(parts[0]);
        String ip = parts[1];
        String method = parts[2];
        String path = parts[3];
        int status = Integer.parseInt(parts[4]);

        return new LogEvent(timestamp,ip,method,path,status,rawLog);
    }
}