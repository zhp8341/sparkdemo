
package com.demo.kafka.spark.persist;

import java.util.Date;
import java.util.Properties;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaProducerPersistTest {

    private final Producer<String, String> producer;
    public final static String             TOPIC = "spark_test_persist";

    public KafkaProducerPersistTest(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop1:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    void produce() throws Exception {
        ObjectMapper mapper = new ObjectMapper();

        for (int i = 1; i <3100; i++) {
            Order order = new Order("orderID" + i, Long.valueOf("" + i));
            order.setData(DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss"));
            producer.send(new ProducerRecord<String, String>(TOPIC, mapper.writeValueAsString(order)));

            //Thread.sleep(1000L);
        }

        producer.close();
    }

    public static void main(String[] args) throws Exception {
        new KafkaProducerPersistTest().produce();
        //System.out.println(DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss"));
    }
}
