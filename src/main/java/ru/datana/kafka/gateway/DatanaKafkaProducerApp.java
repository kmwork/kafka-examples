package ru.datana.kafka.gateway.producer;

import lombok.extern.slf4j.Slf4j;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import ru.datana.kafka.gateway.config.AppConts;
import ru.datana.kafka.gateway.config.AppOptions;

import java.util.Arrays;

import static net.sourceforge.argparse4j.impl.Arguments.store;

@Slf4j
public class DatanaKafkaProducerApp {
    private final static String APP_CONFIG_FILE_NAME = "datana-kafka-client-config.properties";
    private static long delay = 0;
    private static int noOfMessages = 10;

    public static void main(String[] args) {

        log.info(AppConts.APP_LOG_PREFIX + "================ Запуск  ================. Аргументы = " + Arrays.toString(args));
        AppOptions appOptions = new AppOptions();
        try {

            appOptions.load();
            try (Producer<String, String> producer = new KafkaProducer<>(appOptions.getProperties())) {
                producer.initTransactions(); //initiate transactions
                producer.beginTransaction(); //begin transactions
                for (int i = 0; i < noOfMessages; i++) {
                    producer.send(new ProducerRecord<String, String>(appOptions.getKafkaTopic(), Integer.toString(i), Long.toString(System.nanoTime())));

                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                    }
                }
                producer.commitTransaction(); //commit
            }

        } catch (Exception ex) {
            log.error(AppConts.ERROR_LOG_PREFIX + " Ошибка в программе", ex);
        }
        log.info(AppConts.APP_LOG_PREFIX + "********* Завершение программы *********");
    }

    /**
     * Get the command-line argument parser.
     */
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("simple-producer")
                .defaultHelp(true)
                .description("This example is to demonstrate kafka producer capabilities");

        parser.addArgument("--app.dir").action(store())
                .required(true)
                .type(String.class)
                .metavar("APP-DIR")
                .help("path to file "+APP_CONFIG_FILE_NAME);

        parser.addArgument("--messages").action(store())
                .required(true)
                .type(Long.class)
                .metavar("NUM-MESSAGE")
                .help("number of messages to produce");

        parser.addArgument("--syncsend").action(store())
                .required(false)
                .type(Boolean.class)
                .setDefault(true)
                .metavar("TRUE/FALSE")
                .help("sync/async send of messages");

        parser.addArgument("--delay").action(store())
                .required(false)
                .setDefault(10l)
                .type(Long.class)
                .metavar("DELAY")
                .help("number of milli seconds delay between messages.");

        parser.addArgument("--messagetype").action(store())
                .required(false)
                .setDefault("string")
                .type(String.class)
                .choices(Arrays.asList("string", "myevent"))
                .metavar("STRING/MYEVENT")
                .help("generate string messages or MyEvent messages");

        return parser;
    }

}