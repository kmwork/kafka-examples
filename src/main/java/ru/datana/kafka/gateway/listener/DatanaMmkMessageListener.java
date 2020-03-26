package ru.datana.kafka.gateway.listener;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;

public class DatanaMmkMessageListener implements MessageListener<String, String> {

    // inject your own concrete processor
    private IMessageProcessor messageProcessor;

    public DatanaMmkMessageListener() {

    }

    @Override
    public void onMessage(ConsumerRecord<String, String> consumerRecord) {

        // process message
        messageProcessor.process(consumerRecord.key(), consumerRecord.value());
    }
}