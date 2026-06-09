package com.example.logservice;

import java.util.concurrent.Callable;

/**
 * Retry maxAttempts times with exponential backoff.
 */
public class RetryUtil {

    public static <T> T runWithRetry(
            Callable<T> action,
            int maxAttempts,
            long initialBackoffMs,
            Stats stats
    ) throws Exception {

        Exception lastException = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return action.call();

            } catch (Exception e) {
                lastException = e;

                if (stats != null) {
                    stats.incRetryCount();
                }

                if (attempt == maxAttempts) {
                    break;
                }

                long sleepMs = initialBackoffMs * (1L << (attempt - 1));

                System.err.println("[RETRY] attempt=" + attempt
                        + ", sleepMs=" + sleepMs
                        + ", error=" + e.getMessage());

                Thread.sleep(sleepMs);
            }
        }

        throw lastException;
    }
}