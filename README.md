# Log Processing Service

## Overview

H? th?ng x? l? log s? d?ng **Kafka + Oracle Database + Staging Table Pattern**.

Flow:

```text
Producer
   Å´
 Kafka
   Å´
Consumer
   Å´
LOG_EVENT_STG
   Å´
Processor
   Å´
LOG_EVENT
```

## Features

* Producer sinh log v?i TPS c?u h?nh ???c
* Kafka l?m message broker
* Batch insert Oracle
* Retry t?i ?a 3 l?n v?i Exponential Backoff
* Backpressure khi staging backlog qu? l?n
* H? tr? nhi?u Producer / Consumer / Processor
* Kh?ng m?t log khi Consumer crash
* Logging ri?ng cho t?ng app

## Log Files

```text
logs/
Ñ•ÑüÑü producer.txt
Ñ•ÑüÑü consumer.txt
Ñ§ÑüÑü processor.txt
```

## Main Configuration

```properties
kafka.max.poll.records=1000
producer.tps=1000

processor.batch.size=1000

consumer.retry.max.attempts=3
consumer.retry.initial.backoff.ms=500

staging.max.pending=100000
```

## Run

### Producer

```bash
mvn exec:java -Dexec.mainClass=com.example.logservice.LogProducerApp
```

### Consumer

```bash
mvn exec:java -Dexec.mainClass=com.example.logservice.LogConsumerServiceApp
```

### Processor

```bash
mvn exec:java -Dexec.mainClass=com.example.logservice.StagingProcessorApp
```

## Scalability

* Ch?y nhi?u Producer ?? t?ng TPS
* T?ng Kafka partitions v? ch?y nhi?u Consumer
* Ch?y nhi?u Processor, s? d?ng `FOR UPDATE SKIP LOCKED` ?? tr?nh x? l? tr?ng

## Reliability

* Commit Kafka offset sau khi insert staging th?nh c?ng
* Retry khi DB l?i
* Recover record ?ang PROCESSING n?u app b? crash

## Tech Stack

* Java 17
* Maven
* Apache Kafka
* Oracle Database
* HikariCP
