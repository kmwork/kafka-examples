package ru.datana.kafka.gateway;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import ru.datana.kafka.gateway.config.AppConts;
import ru.datana.kafka.gateway.config.AppOptions;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Future;


@Slf4j
public class DatanaKafkaProducerApp {
    private final static String APP_CONFIG_FILE_NAME = "datana-kafka-client-config.properties";
    private static long delay = 10 * 1000;
    private static int noOfMessages = 100;

    public static void main(String[] args) {

        log.info(AppConts.APP_LOG_PREFIX + "================ Запуск  ================. Аргументы = " + Arrays.toString(args));
        AppOptions appOptions = new AppOptions();
        try {

            appOptions.load();
            Properties properties = appOptions.getProperties();
            properties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "datana-producer");
            try (Producer<String, String> producer = new KafkaProducer<>(properties)) {
                //producer.initTransactions(); //initiate transactions
                //producer.beginTransaction(); //begin transactions
                for (int i = 0; i < noOfMessages; i++) {
                    String messageId = "kostya_id_" + System.nanoTime() + "_index: " + i;
                    String messageText = "****KostyaHello****, class =" + DatanaKafkaProducerApp.class.getSimpleName() + ", index = " + i + ", nanoTime =" + System.nanoTime();
                    Future<RecordMetadata> kafkaFuture = producer.send(new ProducerRecord<String, String>(appOptions.getKafkaTopic(), messageId, messageText));
                    RecordMetadata meta = kafkaFuture.get();
                    log.info("[SEND] meta = " + meta);
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                    }
                }
                //producer.commitTransaction(); //commit
            }

        } catch (Exception ex) {
            log.error(AppConts.ERROR_LOG_PREFIX + " Ошибка в программе", ex);
        }
        log.info(AppConts.APP_LOG_PREFIX + "********* Завершение программы *********");
    }


}