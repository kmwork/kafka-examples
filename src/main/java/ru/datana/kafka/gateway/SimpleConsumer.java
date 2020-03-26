package ru.datana.kafka.gateway;


import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import ru.datana.kafka.gateway.config.AppConts;
import ru.datana.kafka.gateway.config.AppOptions;
import ru.datana.kafka.gateway.utils.AppException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class SimpleConsumer {


    private String clientId;
    private KafkaConsumer<String, String> consumer;

    private AtomicBoolean closed = new AtomicBoolean();
    private CountDownLatch shutdownlatch = new CountDownLatch(1);
    private Collection<TopicPartition> partitions = new ArrayList<>(1);

    public SimpleConsumer() throws AppException {
        AppOptions appOptions = new AppOptions();
        appOptions.load();
        Properties properties = appOptions.getProperties();

        this.clientId = properties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG);
        this.consumer = new KafkaConsumer<>(properties);
        partitions.add(new TopicPartition(appOptions.getKafkaTopic(), 0));
    }

    public void run() {

        try {
            log.info("Starting the Consumer : {}", clientId);
            consumer.assign(partitions);
            // consumer.seek(partition, offset); // User has to load the initial offset

            log.info("C : {}, Started to process records for partitions : {}", clientId, partitions);

            while (!closed.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofHours(1));

                if (records.isEmpty()) {
                    log.info("C : {}, Found no records", clientId);
                    continue;
                }

                log.info("C : {} Total No. of records received : {}", clientId, records.count());
                for (ConsumerRecord<String, String> record : records) {
                    log.info("C : {}, Record received topic : {}, partition : {}, key : {}, value : {}, offset : {}",
                            clientId, record.topic(), record.partition(), record.key(), record.value(),
                            record.offset());
                    Thread.sleep(50);
                }
                // User has to take care of committing the offsets
            }
        } catch (Exception e) {
            log.error("Error while consuming messages", e);
        } finally {
            consumer.close();
            shutdownlatch.countDown();
            log.info("C : {}, consumer exited", clientId);
        }
    }

    public void close() {
        try {
            closed.set(true);
            shutdownlatch.await();
        } catch (InterruptedException e) {
            log.error("Error", e);
        }
    }

    public static void main(String[] args) {


        try {


            final SimpleConsumer consumer = new SimpleConsumer();
            consumer.run();

            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

                @Override
                public void run() {
                    consumer.close();
                }
            }));
        } catch (Exception ex) {
            log.error(AppConts.ERROR_LOG_PREFIX + " Ошибка в программе", ex);
        }
        log.info(AppConts.APP_LOG_PREFIX + "********* Завершение программы *********");
    }
//
//    private static Properties getConsumerConfigs(Namespace result) {
//        Properties configs = new Properties();
//        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, result.getString("bootstrap.servers"));
//        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, result.getString("auto.offset.reset"));
//        configs.put(ConsumerConfig.CLIENT_ID_CONFIG, result.getString("clientId"));
//        configs.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, result.getString("max.partition.fetch.bytes"));
//
//        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, CustomDeserializer.class.getName());
//        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomDeserializer.class.getName());
//        return configs;
//    }


}

