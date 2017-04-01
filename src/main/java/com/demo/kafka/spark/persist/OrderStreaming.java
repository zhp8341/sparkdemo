
package com.demo.kafka.spark.persist;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import com.fasterxml.jackson.databind.ObjectMapper;

public class OrderStreaming {

    private static AtomicLong   orderCount  = new AtomicLong(0);
    private static AtomicLong   totalPrice  = new AtomicLong(0);

    static Map<String, Integer> topicMap    = new HashMap<>();

    static Map<String, Object>  kafkaParams = new HashMap<>();

    static Set<String>          topics      = Collections.singleton(KafkaProducerPersistTest.TOPIC);

    private static final String KEYORDERCOUNT="spark_orderCount";
    private static final String KEYTOTALPRICE="spark_totalPrice";
    
    
    static {
        kafkaParams.put("bootstrap.servers", "hadoop1:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "orderstreaming_group");
        kafkaParams.put("auto.offset.reset", "latest");
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
    }

    public static void main(String[] args) {
        System.setProperty("spark.scheduler.mode", "FAIR");
        SparkConf conf = new SparkConf().setMaster("spark://hadoop1:7077").setAppName("OrderStreaming");
        conf.set("spark.scheduler.mode", "FAIR");
        conf.set("spark.cores.max", "2");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLocalProperty("spark.scheduler.pool", "production");
        sc.setLogLevel("ERROR");
        
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(5));
        JavaInputDStream<ConsumerRecord<Object, Object>> orderMsgStream = KafkaUtils.createDirectStream(jssc,
                                                                                                        LocationStrategies.PreferBrokers(),
                                                                                                        ConsumerStrategies.Subscribe(topics,
                                                                                                                                     kafkaParams));
        final ObjectMapper mapper = new ObjectMapper();

        JavaDStream<Order> orderDStream = orderMsgStream.map(t2 -> {
            Order order = mapper.readValue(t2.value().toString(), Order.class);
            return order;
        }).cache();

        orderDStream.foreachRDD((VoidFunction<JavaRDD<Order>>) orderJavaRDD -> {
            reloadDBDate();
            System.out.println("当前redis存储的值：totalPrice="+totalPrice+" orderCount= "+orderCount);
            long count = orderJavaRDD.count();
            if (count > 0) {
                // 累加订单总数
                orderCount.addAndGet(count);
                Long sumPrice = orderJavaRDD.map(order -> order.getPrice()).reduce((a, b) -> a + b);
                // 然后把本次RDD中所有订单的价格总和累加到之前所有订单的价格总和中。
                totalPrice.addAndGet(sumPrice);
                // 数据订单总数和价格总和，生产环境中可以写入数据库
                System.out.println("Total order count : " + orderCount.get() + " with total price : " + totalPrice.get());
                JedisUtil.connectionRedis().set(KEYORDERCOUNT, String.valueOf(orderCount.get()))  ;
                JedisUtil.connectionRedis().set(KEYTOTALPRICE, String.valueOf(totalPrice.get()))  ;
            }

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
