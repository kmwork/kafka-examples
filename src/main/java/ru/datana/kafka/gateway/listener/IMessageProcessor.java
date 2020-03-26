package ru.datana.kafka.gateway.listener;

public interface IMessageProcessor {
    void process(String key, String value);
}
