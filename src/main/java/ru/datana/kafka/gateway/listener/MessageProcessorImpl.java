package ru.datana.kafka.gateway.listener;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MessageProcessorImpl implements IMessageProcessor {


    @Override
    public void process(String key, String message) {
        log.info("[OK] !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1 Received Message: " + message + " from key: " + key);
    }
}