package com.example.logservice;

/**
 * Limit speed of producer.
 */
public class RateLimiter {

    private final int permitsPerSecond;
    private long nextAllowedTimeNanos;

    public RateLimiter(int permitsPerSecond) {
        if (permitsPerSecond <= 0) {
            throw new IllegalArgumentException("permitsPerSecond must be positive");
        }

        this.permitsPerSecond = permitsPerSecond;
        this.nextAllowedTimeNanos = System.nanoTime();
    }

    public void acquire() {
        long intervalNanos = 1_000_000_000L / permitsPerSecond;

        synchronized (this) {
            long now = System.nanoTime();

            if (now < nextAllowedTimeNanos) {
                long sleepNanos = nextAllowedTimeNanos - now;
                sleep(sleepNanos);
            }

            nextAllowedTimeNanos = Math.max(now, nextAllowedTimeNanos) + intervalNanos;
        }
    }

    private void sleep(long nanos) {
        long millis = nanos / 1_000_000L;
        int extraNanos = (int) (nanos % 1_000_000L);

        try {
            Thread.sleep(millis, extraNanos);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}