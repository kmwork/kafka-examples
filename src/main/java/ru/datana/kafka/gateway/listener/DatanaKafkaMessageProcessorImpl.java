package ru.datana.kafka.gateway.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class DatanaKafkaMessageProcessorImpl implements DatanaKafkaMessageProcessor {


    @Override
    @KafkaListener(topics = "datana_topic_kafka", errorHandler = "datanaKafkaErrorHandler",
            clientIdPrefix = "KOSTYA-DatanaKafkaMessageProcessorImpl", containerFactory = "kafkaListenerContainerFactory")
    public void process(String key, String message) {
        log.info("[OK] !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1 Received Message: " + message + " from key: " + key);
    }
}