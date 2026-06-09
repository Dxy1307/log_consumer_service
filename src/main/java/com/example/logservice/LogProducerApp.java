package com.example.logservice;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

/**
 * Generate log and send to Kafka.
 */
public class LogProducerApp {

    private static final List<String> METHODS = List.of("GET","POST","PUT","DELETE","PATCH");

    private static final List<String> PATHS = List.of(
            "/api/users",
            "/api/orders",
            "/api/products",
            "/api/payments",
            "/api/login",
            "/api/logout",
            "/api/search",
            "/api/cart",
            "/api/inventory",
            "/api/reports"
    );

    private static final List<Integer> STATUSES = List.of(200,201,204,400,401,403,404,500,503);

    private static final Random RANDOM = new Random();

    public static void main(String[] args) {
        AppConfig config = AppConfig.load();

        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,config.getKafkaBootstrapServers());

        props.put(ProducerConfig.CLIENT_ID_CONFIG,config.getProducerClientId());

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        props.put(ProducerConfig.ACKS_CONFIG,config.getProducerAcks());

        props.put(ProducerConfig.RETRIES_CONFIG,Integer.MAX_VALUE);

        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,true);

        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,5);

        props.put(ProducerConfig.LINGER_MS_CONFIG,config.getProducerLingerMs());

        props.put(ProducerConfig.BATCH_SIZE_CONFIG,config.getProducerBatchSize());

        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG,config.getProducerBufferMemory());

        RateLimiter rateLimiter = new RateLimiter(config.getProducerTps());

        FileLogger logger = new FileLogger("logs/producer.txt", "PRODUCER");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Producer app stopped.");
            logger.close();
        }));

        Stats stats = new Stats("PRODUCER",config.getStatsPrintIntervalMs(), logger);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            logger.info("Producer app started. topic=" + config.getKafkaTopic()
                    + ", tps=" + config.getProducerTps());

            while (true) {
                rateLimiter.acquire();

                String log = generateLog();

                String key = extractIp(log);

                ProducerRecord<String, String> record = new ProducerRecord<>(config.getKafkaTopic(),key,log);

                stats.incProducerSent();

                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata,Exception exception) {
                        if (exception != null) {
                            stats.incProducerFailed();

                            logger.error("Send failed. error=" + exception.getMessage(), exception);
                        } else {
                            stats.incProducerSuccess();
                        }
                    }
                });

                stats.printIfNeeded();
            }
        }
    }

    private static String generateLog() {
        LocalDateTime timestamp = LocalDateTime.now();

        String ip = generateIp();
        String method = randomOf(METHODS);
        String path = randomOf(PATHS);
        int status = randomOf(STATUSES);

        return timestamp + " " + ip + " " + method + " " + path + " " + status;
    }

    private static String generateIp() {
        return RANDOM.nextInt(256)
                + "."
                + RANDOM.nextInt(256)
                + "."
                + RANDOM.nextInt(256)
                + "."
                + RANDOM.nextInt(256);
    }

    private static <T> T randomOf(List<T> values) {
        return values.get(RANDOM.nextInt(values.size()));
    }

    private static String extractIp(String log) {
        String[] parts = log.split("\\s+");

        if (parts.length >= 2) {
            return parts[1];
        }

        return UUID.randomUUID().toString();
    }
}