package ru.hofftech.omni.pimmigrator.service;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import ru.hofftech.omni.pimmigrator.model.ProductMessageDto;
import ru.hofftech.omni.pimmigrator.model.ProductPimAttribute;
import ru.hofftech.omni.pimmigrator.model.RawProduct;


import java.io.File;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ProductJsonStreamer {

    private static final int BATCH_SIZE = 2000;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final KafkaProductSender kafkaSender;

    public ProductJsonStreamer(KafkaProductSender kafkaSender) {
        this.kafkaSender = kafkaSender;
    }

    public void process(File jsonFile) {
        try {
            int totalProducts = countProducts(jsonFile);
            log.info("{} Products found in file {}", jsonFile.getName(), totalProducts);

            // Теперь основная обработка
            streamAndSend(jsonFile);

        } catch (Exception e) {
            log.error("Error processing file {}: {}", jsonFile.getName(), e.getMessage(), e);
        }
    }

    /**
     * Подсчёт количества продуктов в JSON-массиве.
     */
    private int countProducts(File jsonFile) throws Exception {
        int count = 0;
        try (JsonParser parser = new JsonFactory().createParser(jsonFile)) {
            if (parser.nextToken() != JsonToken.START_ARRAY) {
                throw new IllegalStateException("Expected JSON array at root of file");
            }
            while (parser.nextToken() == JsonToken.START_OBJECT) {
                count++;
                parser.skipChildren();
            }
        }
        return count;
    }

    /**
     * Потоковое чтение и отправка в Kafka.
     */
    private void streamAndSend(File jsonFile) throws Exception {
        List<ProductMessageDto> batch = new ArrayList<>();
        int productIndex = 0;

        try (JsonParser parser = new JsonFactory().createParser(jsonFile)) {
            if (parser.nextToken() != JsonToken.START_ARRAY) {
                throw new IllegalStateException("Expected JSON array at root of file");
            }

            while (parser.nextToken() == JsonToken.START_OBJECT) {
                productIndex++;
                try {
                    RawProduct rawProduct = objectMapper.readValue(parser, RawProduct.class);

                    ProductMessageDto dto = ProductMessageDto.builder()
                            .productId(rawProduct.productId())
                            .attributes(rawProduct.attributes().stream()
                                    .map(a -> ProductPimAttribute.builder()
                                            .attributePimId(a.attributePimId())
                                            .name(a.name())
                                            .type(a.type())
                                            .value(a.value())
                                            .build())
                                    .toList())
                            .build();

                    batch.add(dto);

                    if (batch.size() >= BATCH_SIZE) {
                        kafkaSender.sendBatch(batch);
                        batch.clear();
                    }

                } catch (Exception e) {
                    log.error("Error product parsing. index={}, reason={}", productIndex, e.getMessage(), e);
                    parser.skipChildren();
                }
            }

            if (!batch.isEmpty()) {
                kafkaSender.sendBatch(batch);
            }
        }
    }
}
