
package com.demo.kafka.spark.avro;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;


import com.demo.redis.JedisUtil;
import com.yt.otter.canal.protocol.avro.BinlogTO;

import avro.shaded.com.google.common.collect.Maps;
import jersey.repackaged.com.google.common.collect.Lists;

public class OrderStreaming {

    private static AtomicLong   orderCount  = new AtomicLong(0);
    private static AtomicLong   totalPrice  = new AtomicLong(0);

    static Map<String, Integer> topicMap    = new HashMap<>();

    static Map<String, Object>  kafkaParams = new HashMap<>();

    static Set<String>          topics      = Collections.singleton("yangtuo-t_order-INSERT");
    //static Set<String>          topics      = Collections.singleton(Topic.BINLOGTO.topicName);

    private static final String KEYORDERCOUNT="spark_orderCount";
    private static final String KEYTOTALPRICE="spark_totalPrice";
    
    
    static {
        kafkaParams.put("bootstrap.servers", "hadoop1:9092,hadoop2:9092");
//        kafkaParams.put("key.deserializer", AvroDeserializer.class);
//        kafkaParams.put("value.deserializer", AvroDeserializer.class);
//        kafkaParams.put("key.deserializer", StringSerializer.class);
//        kafkaParams.put("value.deserializer", AvroSerializer.class);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroDeserializer.class.getName());
        kafkaParams.put("group.id", "orderstreaming_group");
        kafkaParams.put("auto.offset.reset", "earliest");//可用参数 latest, earliest, none
        kafkaParams.put("enable.auto.commit", false);
 
        String orderCountRD = JedisUtil.connectionRedis().get(KEYORDERCOUNT);
        String totalPriceRD = JedisUtil.connectionRedis().get(KEYTOTALPRICE);
        
        if(StringUtils.isNotEmpty(orderCountRD)){
            orderCount.addAndGet(Long.parseLong(orderCountRD)) ;
        }
        if(StringUtils.isNotEmpty(totalPriceRD)){
            totalPrice.addAndGet(Long.parseLong(totalPriceRD)) ;
        }
       System.out.println("初始化的值：totalPrice="+totalPrice+" orderCount= "+orderCount);
       System.out.println(topics);
    }

    public static void main(String[] args) {
       // SparkConf conf = new SparkConf().setMaster("spark://hadoop1:7077").setAppName("OrderStreaming").set("spark.cores.max", "2");
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("OrderStreaming").set("spark.cores.max", "2");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(5));
        JavaInputDStream<ConsumerRecord<Object, Object>> orderMsgStream = KafkaUtils.createDirectStream(jssc,
                                                                                                        LocationStrategies.PreferBrokers(),
                                                                                                        ConsumerStrategies.Subscribe(topics,
                                                                                                                                     kafkaParams));
        JavaDStream<BinlogTO> orderDStream = orderMsgStream.map(t2 -> {
            System.out.println("t2"+t2);
            BinlogTO binlogTO=new BinlogTO();
            binlogTO.setTableName("order");
            binlogTO.setOpTiem(DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss"));
            binlogTO.setPostChangeContent(Lists.newArrayList());
            binlogTO.setOpType("UPDATE");
            binlogTO.setSchemaName("数据库名称");
            binlogTO.setPrimary("id");
            binlogTO.setChangeColumnMap(Maps.newHashMap());
            return binlogTO;
        }).cache();
        
        orderDStream.foreachRDD((VoidFunction<JavaRDD<BinlogTO>>) orderJavaRDD -> {
            reloadDBDate();
            System.out.println(orderJavaRDD.collect());
            System.out.println("当前redis存储的值：totalPrice="+totalPrice+" orderCount= "+orderCount);
        });
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }    
    
    public static void reloadDBDate(){
        String orderCountRD = JedisUtil.connectionRedis().get(KEYORDERCOUNT);
        String totalPriceRD = JedisUtil.connectionRedis().get(KEYTOTALPRICE);
        if(StringUtils.isNotEmpty(orderCountRD)){
            orderCount.set(0L);
            orderCount.addAndGet(Long.parseLong(orderCountRD)) ;
        }
        if(StringUtils.isNotEmpty(totalPriceRD)){
            totalPrice.set(0L) ;
            totalPrice.addAndGet(Long.parseLong(totalPriceRD)) ;
        } 
    }
}
