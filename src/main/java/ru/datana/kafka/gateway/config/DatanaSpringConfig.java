package ru.datana.kafka.gateway.config;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.MessageListenerContainer;
import ru.datana.kafka.gateway.listener.DatanaMmkMessageListener;
import ru.datana.kafka.gateway.utils.AppException;

import java.util.Properties;

@Configuration
@EnableKafka
public class DatanaSpringConfig {

    @Bean
    public ConsumerFactory<String, MessageListenerContainer> consumerFactory() throws AppException {
//        JsonDeserializer<MessageListenerContainer> deserializer = new JsonDeserializer<>(MessageListenerContainer.class);
//        deserializer.setRemoveTypeHeaders(false);
//        deserializer.addTrustedPackages("*");
//        deserializer.setUseTypeMapperForKey(true);


        AppOptions appOptions = new AppOptions();
        appOptions.load();
        Properties properties = appOptions.getProperties();
        properties.setProperty(CommonClientConfigs.CLIENT_ID_CONFIG, "datana-consumer");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        //properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);

        return new DefaultKafkaConsumerFactory(properties/*, new StringDeserializer(), deserializer */);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, MessageListenerContainer> kafkaListenerContainerFactory() throws AppException {
        ConcurrentKafkaListenerContainerFactory<String, MessageListenerContainer> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setBatchListener(true);
        factory.getContainerProperties().setMessageListener(new DatanaMmkMessageListener());
        return factory;
    }
}