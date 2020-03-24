package ru.datana.kafka.gateway.producer;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Properties;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class DatanaKafkaProducerApp {
    private final static String APP_CONFIG_FILE_NAME = "datana-kafka-client-config.properties";

    public static void main(String[] args) {
        ArgumentParser parser = argParser();

        try {
            Namespace res = parser.parseArgs(args);

            /* parse args */
            String appDir = res.getString("app.dir");
            Boolean syncSend = res.getBoolean("syncsend");
            long noOfMessages = res.getLong("messages");
            long delay = res.getLong("delay");
            String messageType = res.getString("messagetype");


            File fileConf = new File(appDir, APP_CONFIG_FILE_NAME);
            Properties producerConfig = new Properties();
            try (FileReader fileReader = new FileReader(fileConf, StandardCharsets.UTF_8)) {
                producerConfig.load(fileReader);
                producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
                producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            }

            String topic = producerConfig.getProperty("topic");

            try (Producer<String, String> producer = new KafkaProducer<>(producerConfig)) {
                producer.initTransactions(); //initiate transactions
                producer.beginTransaction(); //begin transactions
                for (int i = 0; i < noOfMessages; i++) {
                    producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), Long.toString(System.nanoTime())));

                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException e) {
                    }
                }
                producer.commitTransaction(); //commit
            }

        } catch (ArgumentParserException | IOException e) {
            if (args.length == 0) {
                parser.printHelp();
                System.exit(0);
            } if (e instanceof ArgumentParserException){
                parser.handleError((ArgumentParserException)e);
                System.exit(1);
            }
        }

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