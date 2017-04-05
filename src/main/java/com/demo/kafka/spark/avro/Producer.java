package com.demo.kafka.spark.avro;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Created by guyue on 2017/4/1.
 */
public class Producer<T extends SpecificRecordBase> extends KafkaProducer  {
    private org.apache.kafka.clients.producer.Producer<String, T> producer ;
    private String servers ;
    public Producer(Map configs) {
        super(configs);
    }

    public Producer(Map configs, Serializer keySerializer, Serializer valueSerializer) {
        super(configs, keySerializer, valueSerializer);
    }

    public Producer(Properties properties) {
        super(properties);
    }

    public Producer(Properties properties, Serializer keySerializer, Serializer valueSerializer) {
        super(properties, keySerializer, valueSerializer);
    }

    public Future<RecordMetadata> send(Topic topic, T data, Callback callback) {
        if(callback == null){
            return this.send(new ProducerRecord<>(topic.topicName, data));
        }
       return this.send(new ProducerRecord<>(topic.topicName, data), callback)  ;
    }

    public Future<RecordMetadata> send(Topic topic, T data){
        return send(topic, data, null);
    }


}
