package com.example.logservice;

import java.util.concurrent.atomic.AtomicLong;

public class Stats {

    private final String name;
    private final long printIntervalMs;

    private final AtomicLong producerSent = new AtomicLong();
    private final AtomicLong producerSuccess = new AtomicLong();
    private final AtomicLong producerFailed = new AtomicLong();

    private final AtomicLong consumerPolled = new AtomicLong();
    private final AtomicLong consumerValid = new AtomicLong();
    private final AtomicLong consumerBad = new AtomicLong();

    private final AtomicLong stagingInsertSuccess = new AtomicLong();
    private final AtomicLong stagingInsertFailed = new AtomicLong();

    private final AtomicLong kafkaCommitSuccess = new AtomicLong();
    private final AtomicLong kafkaCommitFailed = new AtomicLong();

    private final AtomicLong retryCount = new AtomicLong();
    private final AtomicLong backpressurePauseCount = new AtomicLong();

    private final AtomicLong processorProcessed = new AtomicLong();
    private final AtomicLong processorFailed = new AtomicLong();
    private final AtomicLong processorRecovered = new AtomicLong();

    private long lastPrintTime;

    private final FileLogger logger;

    private long lastProducerSent;
    private long lastConsumerPolled;
    private long lastStagingInserted;
    private long lastProcessorProcessed;

    public Stats(String name, long printIntervalMs) {
        this(name, printIntervalMs, null);
    }

    public Stats(String name, long printIntervalMs, FileLogger logger) {
        this.name = name;
        this.printIntervalMs = printIntervalMs;
        this.logger = logger;
        this.lastPrintTime = System.currentTimeMillis();
    }

    public void incProducerSent() {
        producerSent.incrementAndGet();
    }

    public void incProducerSuccess() {
        producerSuccess.incrementAndGet();
    }

    public void incProducerFailed() {
        producerFailed.incrementAndGet();
    }

    public void addConsumerPolled(long value) {
        consumerPolled.addAndGet(value);
    }

    public void addConsumerValid(long value) {
        consumerValid.addAndGet(value);
    }

    public void addConsumerBad(long value) {
        consumerBad.addAndGet(value);
    }

    public void addStagingInsertSuccess(long value) {
        stagingInsertSuccess.addAndGet(value);
    }

    public void incStagingInsertFailed() {
        stagingInsertFailed.incrementAndGet();
    }

    public void incKafkaCommitSuccess() {
        kafkaCommitSuccess.incrementAndGet();
    }

    public void incKafkaCommitFailed() {
        kafkaCommitFailed.incrementAndGet();
    }

    public void incRetryCount() {
        retryCount.incrementAndGet();
    }

    public void incBackpressurePauseCount() {
        backpressurePauseCount.incrementAndGet();
    }

    public void addProcessorProcessed(long value) {
        processorProcessed.addAndGet(value);
    }

    public void incProcessorFailed() {
        processorFailed.incrementAndGet();
    }

    public void addProcessorRecovered(long value) {
        processorRecovered.addAndGet(value);
    }

    public void printIfNeeded() {
        long now = System.currentTimeMillis();

        if (now - lastPrintTime < printIntervalMs) {
            return;
        }

        long elapsedMs = now - lastPrintTime;
        double elapsedSeconds = elapsedMs / 1000.0;

        long currentProducerSent = producerSent.get();
        long currentConsumerPolled = consumerPolled.get();
        long currentStagingInserted = stagingInsertSuccess.get();
        long currentProcessorProcessed = processorProcessed.get();

        double producerTps = (currentProducerSent - lastProducerSent) / elapsedSeconds;

        double consumerPollTps = (currentConsumerPolled - lastConsumerPolled) / elapsedSeconds;

        double stagingInsertTps = (currentStagingInserted - lastStagingInserted) / elapsedSeconds;

        double processorTps = (currentProcessorProcessed - lastProcessorProcessed) / elapsedSeconds;

        StringBuilder sb = new StringBuilder();
        sb.append("========== [STATS] ").append(name).append(" ==========");

        if ("PRODUCER".equals(name)) {
            sb.append(System.lineSeparator()).append("Producer:");
            sb.append(System.lineSeparator()).append("  sent.total=").append(producerSent.get());
            sb.append(System.lineSeparator()).append("  success.total=").append(producerSuccess.get());
            sb.append(System.lineSeparator()).append("  failed.total=").append(producerFailed.get());
            sb.append(System.lineSeparator()).append("  sent.tps=").append(format(producerTps));
        }

        if ("CONSUMER".equals(name)) {
            sb.append(System.lineSeparator()).append("Consumer:");
            sb.append(System.lineSeparator()).append("  polled.total=").append(consumerPolled.get());
            sb.append(System.lineSeparator()).append("  valid.total=").append(consumerValid.get());
            sb.append(System.lineSeparator()).append("  bad.total=").append(consumerBad.get());
            sb.append(System.lineSeparator()).append("  polled.tps=").append(format(consumerPollTps));

            sb.append(System.lineSeparator()).append("Staging Insert:");
            sb.append(System.lineSeparator()).append("  success.total=").append(stagingInsertSuccess.get());
            sb.append(System.lineSeparator()).append("  failed.total=").append(stagingInsertFailed.get());
            sb.append(System.lineSeparator()).append("  insert.tps=").append(format(stagingInsertTps));

            sb.append(System.lineSeparator()).append("Kafka Offset:");
            sb.append(System.lineSeparator()).append("  commit.success.total=").append(kafkaCommitSuccess.get());
            sb.append(System.lineSeparator()).append("  commit.failed.total=").append(kafkaCommitFailed.get());

            sb.append(System.lineSeparator()).append("Reliability:");
            sb.append(System.lineSeparator()).append("  retry.total=").append(retryCount.get());
            sb.append(System.lineSeparator()).append("  backpressure.pause.total=").append(backpressurePauseCount.get());
        }

        if ("PROCESSOR".equals(name)) {
            sb.append(System.lineSeparator()).append("Processor:");
            sb.append(System.lineSeparator()).append("  processed.total=").append(processorProcessed.get());
            sb.append(System.lineSeparator()).append("  failed.total=").append(processorFailed.get());
            sb.append(System.lineSeparator()).append("  recovered.total=").append(processorRecovered.get());
            sb.append(System.lineSeparator()).append("  processed.tps=").append(format(processorTps));
        }

        sb.append(System.lineSeparator()).append("==========================================");

        if (logger != null) {
            logger.info(sb.toString());
        } else {
            System.out.println();
            System.out.println(sb);
            System.out.println();
        }

        lastPrintTime = now;
        lastProducerSent = currentProducerSent;
        lastConsumerPolled = currentConsumerPolled;
        lastStagingInserted = currentStagingInserted;
        lastProcessorProcessed = currentProcessorProcessed;
    }

    private String format(double value) {
        return String.format("%.2f", value);
    }
}