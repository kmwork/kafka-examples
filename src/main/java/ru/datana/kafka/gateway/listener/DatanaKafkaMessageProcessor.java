package ru.datana.kafka.gateway.listener;

public interface DatanaKafkaMessageProcessor {
    void process(String key, String value);
}
