package com.demo.kafka.spark.avro;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Created by guyue on 2017/4/1.
 */
public class Consumer <T extends SpecificRecordBase> extends org.apache.kafka.clients.consumer.KafkaConsumer{
    private org.apache.kafka.clients.consumer.Consumer<String, T> consumer ;
    private String servers ;

    public Consumer(Map configs) {
        super(configs);
    }

    public Consumer(Map configs, Deserializer keyDeserializer, Deserializer valueDeserializer) {
        super(configs, keyDeserializer, valueDeserializer);
    }

    public Consumer(Properties properties) {
        super(properties);
    }

    public Consumer(Properties properties, Deserializer keyDeserializer, Deserializer valueDeserializer) {
        super(properties, keyDeserializer, valueDeserializer);
    }

    public void subscribe(Topic topic) {
        this.subscribe(Collections.singletonList(topic.topicName));
    }

    public void subscribe(Topic topic, ConsumerRebalanceListener listener){
        this.subscribe(Collections.singletonList(topic.topicName), listener);
    }
}
