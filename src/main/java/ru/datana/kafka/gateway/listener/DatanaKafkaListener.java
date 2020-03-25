package ru.datana.kafka.gateway.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.support.GenericMessage;

@Slf4j
//public class DatanaKafkaListener implements ConsumerAwareMessageListener {
public class DatanaKafkaListener {

    @KafkaListener(topics = "datana_topic_kafka", errorHandler = "datanaKafkaErrorHandler")
    public void execute(final GenericMessage<String> message) {
        log.info(
                "Reading offset: {} and message received: {}",
                message.getHeaders().get("kafka_offset"),
                message.getPayload());
        throw new RuntimeException("Ops");
    }


//    @Override
//    public void onMessage(ConsumerRecord consumerRecord, Consumer consumer) {
//
//    }
}