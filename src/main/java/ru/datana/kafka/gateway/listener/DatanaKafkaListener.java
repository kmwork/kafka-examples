package ru.datana.kafka.gateway.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Slf4j
@Component
public class DatanaKafkaListener {


    @PostConstruct
    protected void init() {
        log.info("[OK] &&&&&&&&&&&&&&&&&&&&&&& init ok");
    }

    @KafkaListener(topics = "datana_topic_kafka", groupId = "datana-test-group", errorHandler = "datanaKafkaErrorHandler")
    public void listenWithHeaders(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        log.info("[OK] !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1 Received Message: " + message + " from partition: " + partition);
    }
}