package com.demo.kafka.spark.avro;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.yt.otter.canal.protocol.avro.BinlogTO;

import java.util.Iterator;
import java.util.Properties;

/**
 * Created by Administrator on 2017/3/23.
 */
public class KafkaConsumerTest {
    public static void main(String[] args) {
        String servers = "hadoop1:9092,hadoop2:9092";
        Properties props = new Properties() ;
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroDeserializer.class.getName());
        Consumer<BinlogTO> consumer = new Consumer<BinlogTO>(props) ;
        consumer.subscribe(Topic.BINLOGTO);
        while (true) {
            ConsumerRecords records = consumer.poll(100) ;
            Iterator<ConsumerRecord> it = records.iterator();
            while (it.hasNext()){
                ConsumerRecord record =  it.next() ;
                System.out.println(record.offset() + "{" + record.key() + ":"+record.value()+"}");
            }

        }

    }
}
