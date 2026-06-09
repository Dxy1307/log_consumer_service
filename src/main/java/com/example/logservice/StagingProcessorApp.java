package com.example.logservice;

import javax.sql.DataSource;

/**
 * Standalone app for move from staging table to main table.
 * Can run multiple instances to scale.
 */
public class StagingProcessorApp {

    public static void main(String[] args) {
        AppConfig config = AppConfig.load();

        DataSource dataSource = OracleDataSourceFactory.create(config);

        FileLogger logger = new FileLogger("logs/processor.txt", "PROCESSOR");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Staging processor app stopped.");
            logger.close();
        }));

        StagingLogProcessor processor = new StagingLogProcessor(dataSource, logger);

        Stats stats = new Stats(
                "PROCESSOR",
                config.getStatsPrintIntervalMs(),
                logger
        );

        logger.info("Staging processor app started. batchSize=" + config.getProcessorBatchSize());

        while (true) {
            try {
                int recoveredRows = processor.recoverProcessingRows();

                if (recoveredRows > 0) {
                    stats.addProcessorRecovered(recoveredRows);
                }

                int processed = processor.processOnce(config.getProcessorBatchSize());

                if (processed > 0) {
                    stats.addProcessorProcessed(processed);

                    logger.info("Processed rows=" + processed);
                } else {
                    sleep(config.getProcessorSleepMs());
                }

            } catch (Exception e) {
                stats.incProcessorFailed();

                logger.error("Processor error. error=" + e.getMessage(), e);

                sleep(config.getProcessorSleepMs());
            }

            stats.printIfNeeded();
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