package ru.hofftech.omni.pimmigrator;


import lombok.extern.slf4j.Slf4j;
import ru.hofftech.omni.pimmigrator.service.KafkaProductSender;
import ru.hofftech.omni.pimmigrator.service.ProductJsonStreamer;

import java.io.File;

@Slf4j
public class Application {
    public static void main(String[] args) {
        if (args.length < 1) {
            log.error("Укажите путь к JSON файлу в аргументах запуска");
            System.exit(1);
        }

        File jsonFile = new File(args[0]);
        if (!jsonFile.exists()) {
            log.error("Файл не найден: {}", jsonFile.getAbsolutePath());
            System.exit(2);
        }

        KafkaProductSender kafkaSender = new KafkaProductSender();
        ProductJsonStreamer streamer = new ProductJsonStreamer(kafkaSender);
        streamer.process(jsonFile);
    }
}

