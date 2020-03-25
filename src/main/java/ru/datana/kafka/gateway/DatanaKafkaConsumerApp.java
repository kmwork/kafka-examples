package ru.datana.kafka.gateway;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.Banner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import ru.datana.kafka.gateway.config.AppConts;
import ru.datana.kafka.gateway.config.DatanaSpringConfig;

import java.util.Arrays;

@Slf4j
@SpringBootApplication
@Import(DatanaSpringConfig.class)
public class DatanaKafkaConsumerApp {
    public static void main(String[] args) {
        log.info(AppConts.APP_LOG_PREFIX + "================ Запуск  ================. Аргументы = " + Arrays.toString(args));

        try {
            SpringApplication app = new SpringApplication(DatanaKafkaConsumerApp.class);
            app.setBannerMode(Banner.Mode.OFF);
            ///createSystemConsumer();
            app.run(args);
        } catch (Exception ex) {
            log.error(AppConts.ERROR_LOG_PREFIX + " Ошибка в программе", ex);
        }
        log.info(AppConts.APP_LOG_PREFIX + "********* Завершение программы *********");
    }

//    private static void createSystemConsumer() throws AppException {
//
//        AppOptions appOptions = new AppOptions();
//        appOptions.load();
//        String name = appOptions.getKafkaTopic();
//        log.info("Creating kafka consumer for topic {}", name);
//        ContainerProperties containerProps = new ContainerProperties(name);
//
//        Properties props = appOptions.getProperties();
//        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
//        ConsumerFactory<String, String> factory = new DefaultKafkaConsumerFactory(props);
//
//        ConcurrentMessageListenerContainer<String, GenericMessageListener> container =
//                new ConcurrentMessageListenerContainer(factory, containerProps);
//        container.setupMessageListener(new DatanaKafkaListener());
//        container.start();
//        log.info("Successfully created kafka consumer for topic {}", name);
//    }
}