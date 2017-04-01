
package com.demo.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerTest {

    private final Producer<String, String> producer;
    public final static String             TOPIC = "spark_test";

    public KafkaProducerTest(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                  "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    void produce() {
        String data = "1,2,3,4,5";
        producer.send(new ProducerRecord<String, String>(TOPIC, data));
        producer.close();
    }

    public static void main(String[] args) {
        new KafkaProducerTest().produce();
    }
}
