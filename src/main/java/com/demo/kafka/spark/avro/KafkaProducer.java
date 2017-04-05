
package com.demo.kafka.spark.avro;

import java.util.Date;
import java.util.Properties;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import com.yt.otter.canal.protocol.avro.BinlogTO;

import avro.shaded.com.google.common.collect.Maps;
import jersey.repackaged.com.google.common.collect.Lists;

public class KafkaProducer <T extends SpecificRecordBase>{

    private final Producer<BinlogTO> producer;

    public KafkaProducer(){
        String servers = "hadoop1:9092,hadoop2:9092";
        Properties props = new Properties() ;
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class.getName());
        producer = new Producer<>(props);
    }

    void produce() throws Exception {
        BinlogTO binlogTO=new BinlogTO();
        binlogTO.setTableName("order");
        binlogTO.setOpTiem(DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss"));
        binlogTO.setPostChangeContent(Lists.newArrayList());
        binlogTO.setOpType("UPDATE");
        binlogTO.setSchemaName("数据库名称");
        binlogTO.setPrimary("id");
        binlogTO.setChangeColumnMap(Maps.newHashMap());
     for (int i = 0; i <100; i++) {
         System.out.println(binlogTO);
         producer.send(Topic.BINLOGTO, binlogTO);
         Thread.sleep(1000L);
    }
        
          
        
     
    }

    public static void main(String[] args) throws Exception {
        new KafkaProducer<BinlogTO>().produce();
       
    }
}
