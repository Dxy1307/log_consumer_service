package com.example.logservice;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Simple file logger for this plain Java project.
 * Each app should create its own FileLogger instance.
 */
public class FileLogger implements AutoCloseable {

    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final String appName;
    private final BufferedWriter writer;

    public FileLogger(String filePath, String appName) {
        try {
            this.appName = appName;

            Path path = Path.of(filePath);
            Path parent = path.getParent();

            if (parent != null) {
                Files.createDirectories(parent);
            }

            this.writer = Files.newBufferedWriter(
                    path,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );
        } catch (IOException e) {
            throw new RuntimeException("Cannot create log file: " + filePath, e);
        }
    }

    public synchronized void info(String message) {
        write("INFO", message, null);
    }

    public synchronized void error(String message) {
        write("ERROR", message, null);
    }

    public synchronized void error(String message, Throwable throwable) {
        write("ERROR", message, throwable);
    }

    private void write(String level, String message, Throwable throwable) {
        try {
            writer.write("[" + LocalDateTime.now().format(FORMATTER) + "] "
                    + "[" + appName + "] "
                    + "[" + level + "] "
                    + message);
            writer.newLine();

            if (throwable != null) {
                writer.write("[" + LocalDateTime.now().format(FORMATTER) + "] "
                        + "[" + appName + "] "
                        + "[" + level + "] "
                        + "exception=" + throwable.getClass().getName()
                        + ", message=" + throwable.getMessage());
                writer.newLine();
            }

            writer.flush();
        } catch (IOException e) {
            throw new RuntimeException("Cannot write log", e);
        }
    }

    @Override
    public synchronized void close() {
        try {
            writer.flush();
            writer.close();
        } catch (IOException ignored) {
        }
    }
}
