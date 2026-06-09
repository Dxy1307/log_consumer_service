# Log Processing Service

## Overview

A high-throughput log processing system built with Java, Apache Kafka, and Oracle Database.

The system uses a **Staging Table Pattern** to ensure reliability, scalability, and fault tolerance when processing millions of logs.

### Architecture

```text
Producer
   Ñ†
   Å•
 Kafka
   Ñ†
   Å•
Consumer
   Ñ†
   Å•
LOG_EVENT_STG
   Ñ†
   Å•
Processor
   Ñ†
   Å•
LOG_EVENT
```

## Features

- Configurable log generation rate (TPS)
- Apache Kafka as message broker
- Batch processing for high performance
- Oracle Database integration
- Retry mechanism with exponential backoff
- Backpressure protection for overload scenarios
- Supports multiple Producers, Consumers, and Processors
- Crash recovery without log loss
- Separate log files for each application component

## Configuration

```properties
producer.tps=1000

kafka.max.poll.records=1000

processor.batch.size=1000

consumer.retry.max.attempts=3
consumer.retry.initial.backoff.ms=500

staging.max.pending=100000
```

## Running the Applications

### Start Producer

```bash
mvn exec:java -Dexec.mainClass=com.example.logservice.LogProducerApp
```

### Start Consumer

```bash
mvn exec:java -Dexec.mainClass=com.example.logservice.LogConsumerServiceApp
```

### Start Processor

```bash
mvn exec:java -Dexec.mainClass=com.example.logservice.StagingProcessorApp
```

## Scalability

- Run multiple Producer instances to increase throughput.
- Increase Kafka partitions and run multiple Consumers.
- Run multiple Processors using `FOR UPDATE SKIP LOCKED` to prevent duplicate processing.

## Reliability

- Kafka offsets are committed only after successful staging insert.
- Automatic retry on database failures.
- Recovery of records stuck in `PROCESSING` state after application crashes.

## Log Files

```text
logs/
Ñ•ÑüÑü producer.txt
Ñ•ÑüÑü consumer.txt
Ñ§ÑüÑü processor.txt
```

## Technology Stack

- Java 17
- Maven
- Apache Kafka
- Oracle Database
- HikariCP