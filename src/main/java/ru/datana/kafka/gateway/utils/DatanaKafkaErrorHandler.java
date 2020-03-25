package ru.datana.kafka.gateway.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

@Component("datanaKafkaErrorHandler")
@Slf4j
public class DatanaKafkaErrorHandler implements KafkaListenerErrorHandler {

    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception) {
        log.error("Error while processing: " + ObjectUtils.nullSafeToString(message), exception);
        return message;
    }
}
