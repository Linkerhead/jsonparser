package ru.hofftech.omni.pimmigrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.hofftech.omni.pimmigrator.model.ProductMessageDto;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class KafkaProductSender {

    private final Producer<String, ProductMessageDto> producer;
    private final String topicName;


    //todo добавить настройки безопасности
    public KafkaProductSender() {
        String bootstrapServers = getRequiredEnv("KAFKA_BOOTSTRAP_SERVERS");
        this.topicName = getRequiredEnv("KAFKA_TOPIC_NAME");
        String acks = getOptionalEnv("KAFKA_ACKS", "1");
        int retries = Integer.parseInt(getOptionalEnv("KAFKA_RETRIES", "3"));

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "ru.hofftech.omni.pimmigrator.config.ProductMessageSerializer");
        props.put(ProducerConfig.ACKS_CONFIG, acks);
        props.put(ProducerConfig.RETRIES_CONFIG, retries);

        this.producer = new KafkaProducer<>(props);
    }

    private String getRequiredEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            value = System.getProperty(name);
        }
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Required environment variable or system property '" + name + "' is not set");
        }
        return value;
    }

    private String getOptionalEnv(String name, String defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            value = System.getProperty(name, defaultValue);
        }
        return value;
    }

    public void sendBatch(List<ProductMessageDto> batch) {
        if (batch == null || batch.isEmpty()) {
            log.warn("Batch is empty, nothing to send to topic {}", topicName);
            return;
        }

        ObjectMapper objectMapper = new ObjectMapper();
        AtomicInteger errorCount = new AtomicInteger(0);

        for (ProductMessageDto product : batch) {
            String keyJson;
            try {
                keyJson = objectMapper.writeValueAsString(
                        Map.of("productId", product.productId())
                );
            } catch (Exception e) {
                log.error("Failed to serialize key for product {}: {}", product.productId(), e.getMessage(), e);
                errorCount.incrementAndGet();
                continue;
            }

            ProducerRecord<String, ProductMessageDto> record =
                    new ProducerRecord<>(topicName, keyJson, product);

            try {
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        errorCount.incrementAndGet();
                        log.error("Error sending product {}: {}", product.productId(), exception.getMessage(), exception);
                    }
                });
            } catch (Exception e) {
                errorCount.incrementAndGet();
                log.error("Failed to enqueue product {}: {}", product.productId(), e.getMessage(), e);
            }
        }

        try {
            producer.flush();
        } catch (Exception e) {
            log.error("Error flushing producer for topic {}: {}", topicName, e.getMessage(), e);
        }

        int sentCount = batch.size() - errorCount.get();
        if (errorCount.get() == 0) {
            log.info("Successfully sent {} products to the topic {}", batch.size(), topicName);
        } else {
            log.warn("Sent {} products to topic {}, errors: {}", sentCount, topicName, errorCount.get());
        }
    }


    public void close() {
        producer.close();
    }
}
