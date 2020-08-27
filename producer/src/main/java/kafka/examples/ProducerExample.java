package kafka.examples;

import kafka.examples.producer.MyEvent;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

import static net.sourceforge.argparse4j.impl.Arguments.store;

public class ProducerExample {
    public static void main(String[] args) {

        String brokerList ="127.0.0.1:9092";
        String topic = "test";
        Boolean syncSend = true;
        long noOfMessages = 10;
        long delay = 2000;
        String messageType ="string";


        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", brokerList);
        producerConfig.put("client.id", "basic-producer");
        producerConfig.put("acks", "all");
        producerConfig.put("retries", "3");
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer
                <String, String>(producerConfig);

        Scanner sc = new Scanner(System.in);
        int i = 0;
        while(true){
            if(i == 1000)break;
            i++;
            producer.send(new ProducerRecord<String, String>(topic,
                    Integer.toString(i), sc.nextLine()));







        }

        producer.close();

    }

    private static byte[] getEvent(String messageType, int i) {
        if ("string".equalsIgnoreCase(messageType))
            return serialize("message" + i);
        else
            return serialize(new MyEvent(i, "event" + i, "test", System.currentTimeMillis()));
    }


    private static byte[] getKey(int i) {
        return serialize(new Integer(i));
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

        parser.addArgument("--bootstrap.servers").action(store())
                .required(true)
                .type(String.class)
                .metavar("BROKER-LIST")
                .help("comma separated broker list");

        parser.addArgument("--topic").action(store())
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("produce messages to this topic");

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
