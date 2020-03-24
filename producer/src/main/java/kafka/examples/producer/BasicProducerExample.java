/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.examples.producer;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Properties;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class BasicProducerExample {
private final static String APP_CONFIG_FILE_NAME = "datana-ssl-user-config.properties";
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
            try (FileReader fileReader = new FileReader(fileConf, Charset.forName("UTF-8"))) {
                producerConfig.load(fileReader);
                producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
                producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            }

            String topic = producerConfig.getProperty("topic");


            SimpleProducer<byte[], byte[]> producer = new SimpleProducer<>(producerConfig, syncSend);

            for (int i = 0; i < noOfMessages; i++) {
                producer.send(topic, getKey(i), getEvent(messageType, i));
                try {
                    Thread.sleep(delay);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            producer.close();
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

    private static byte[] getEvent(String messageType, int i) {
        if ("string".equalsIgnoreCase(messageType))
            return serialize("message" + i);
        else
            return serialize(new MyEvent(i, "event" + i, "test", System.currentTimeMillis()));
    }


    private static byte[] getKey(int i) {
        return serialize(Integer.valueOf(i));
    }

    public static byte[] serialize(final Object obj) {
        return org.apache.commons.lang3.SerializationUtils.serialize((Serializable) obj);
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