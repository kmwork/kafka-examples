package ru.datana.kafka.gateway.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import ru.datana.kafka.gateway.config.AppConts;
import ru.datana.kafka.gateway.config.AppOptions;

import java.util.Arrays;
import java.util.Properties;


@Slf4j
public class DatanaKafkaProducerApp {
    private final static String APP_CONFIG_FILE_NAME = "datana-kafka-client-config.properties";
    private static long delay = 0;
    private static int noOfMessages = 10;

    public static void main(String[] args) {

        log.info(AppConts.APP_LOG_PREFIX + "================ Запуск  ================. Аргументы = " + Arrays.toString(args));
        AppOptions appOptions = new AppOptions();
        try {

            appOptions.load();
            Properties properties = appOptions.getProperties();
            properties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "datana-producer");
            try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
                producer.initTransactions(); //initiate transactions
                producer.beginTransaction(); //begin transactions
                for (int i = 0; i < noOfMessages; i++) {
                    producer.send(new ProducerRecord<String, String>(appOptions.getKafkaTopic(), Integer.toString(i), Long.toString(System.nanoTime())));

                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                    }
                }
                producer.commitTransaction(); //commit
            }

        } catch (Exception ex) {
            log.error(AppConts.ERROR_LOG_PREFIX + " Ошибка в программе", ex);
        }
        log.info(AppConts.APP_LOG_PREFIX + "********* Завершение программы *********");
    }


}