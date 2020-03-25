package ru.datana.kafka.gateway.config;


import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import ru.datana.kafka.gateway.utils.AppException;
import ru.datana.kafka.gateway.utils.LanitFileUtils;
import ru.datana.kafka.gateway.utils.ValueParser;

import java.util.Properties;

/**
 * POJO для конфига из парсинга конфига datana_siemens.properties
 */
@ToString
@Slf4j
public class AppOptions {

    @Getter
    private String kafkaTopic;

    @Getter
    private String kafkaBrokerUrl;


    @Getter
    private String appVersion;
    @Getter
    private Properties properties;


    public void load() throws AppException {
        properties = LanitFileUtils.readDataConfig();
        appVersion = ValueParser.readPropAsText(properties, "app.version");
        kafkaTopic = ValueParser.readPropAsText(properties, "kafka.topic");
        kafkaBrokerUrl = ValueParser.readPropAsText(properties, "kafka.brokerUrl");

        log.info("[Настройки] Параметры = " + toString());

    }

}
