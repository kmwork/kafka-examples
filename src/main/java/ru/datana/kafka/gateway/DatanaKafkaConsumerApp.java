package ru.datana.kafka.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class DatanaKafkaConsumerApp {
    public static void main(String[] args) {
        SpringApplication.run(DatanaKafkaConsumerApp.class, args);
    }
}