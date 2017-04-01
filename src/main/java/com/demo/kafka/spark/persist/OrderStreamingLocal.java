
package com.demo.kafka.spark.persist;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.collections.CollectionUtils;
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

public class OrderStreamingLocal {

    private static AtomicLong   orderCount  = new AtomicLong(0);
    private static AtomicLong   totalPrice  = new AtomicLong(0);
    private static final String KEYORDERCOUNT="test_orderCount";
    private static final String KEYTOTALPRICE="test_totalPrice";
    
    static Map<String, Integer> topicMap    = new HashMap<>();

    static Map<String, Object>  kafkaParams = new HashMap<>();

    static Set<String>          topics      = Collections.singleton(KafkaProducerPersistTest.TOPIC);

    static {
        kafkaParams.put("bootstrap.servers", "hadoop1:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        //kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);
        getDBDate();
        System.out.println("初始化的值：totalPrice="+totalPrice+" orderCount= "+orderCount);

    }

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("OrderStreamingLocal");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(5));
        JavaInputDStream<ConsumerRecord<Object, Object>> orderMsgStream = KafkaUtils.createDirectStream(jssc,
                                                                                                        LocationStrategies.PreferBrokers(),
                                                                                                        ConsumerStrategies.Subscribe(topics,
                                                                                                                                     kafkaParams));
        final ObjectMapper mapper = new ObjectMapper();

        JavaDStream<Order> orderDStream = orderMsgStream.map(t2 -> {
            System.out.println(t2);
            Order order = mapper.readValue(t2.value().toString(), Order.class);
       
            return order;
        }).cache();

        try{
        orderDStream.foreachRDD((VoidFunction<JavaRDD<Order>>) orderJavaRDD -> {
//           if(!CollectionUtils.isEmpty(orderJavaRDD.collect())){
//               reloadDBDate();
//               System.out.println("当前redis存储的值：totalPrice="+totalPrice+" orderCount= "+orderCount);
//               long count = orderJavaRDD.count();
//               System.out.println("1======="+orderJavaRDD.collect());
//               if (count > 0) {
//                   System.out.println("2======="+orderJavaRDD.collect());
//                   // 累加订单总数
//                   orderCount.addAndGet(count);
//                   Long sumPrice = orderJavaRDD.map(order -> order.getPrice()).reduce((a, b) -> a + b);
//                   // 然后把本次RDD中所有订单的价格总和累加到之前所有订单的价格总和中。
//                   totalPrice.addAndGet(sumPrice);
//
//                   // 数据订单总数和价格总和，生产环境中可以写入数据库
//                   System.out.println("Total order count : " + orderCount.get() + " with total price : " + totalPrice.get());
//                   JedisUtil.connectionRedis().set(KEYORDERCOUNT, String.valueOf(orderCount.get()))  ;
//                   JedisUtil.connectionRedis().set(KEYTOTALPRICE, String.valueOf(totalPrice.get()))  ;
//               }
//           }
            reloadDBDate();
            orderJavaRDD.foreach(t -> {
                if(t!=null){
                    orderCount.addAndGet(1); 
                    totalPrice.addAndGet(t.getPrice());
                }
            });
            JedisUtil.connectionRedis().set(KEYORDERCOUNT, String.valueOf(orderCount.get()))  ;
            JedisUtil.connectionRedis().set(KEYTOTALPRICE, String.valueOf(totalPrice.get()))  ;
            System.out.println("Total order count : " + orderCount.get() + " with total price : " + totalPrice.get());
           
        });}
        catch (Exception e) {
            e.printStackTrace();
        }
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
            //System.exit(-1);
        }
    }
    
    public static void getDBDate(){
        String orderCountRD = JedisUtil.connectionRedis().get(KEYORDERCOUNT);
        String totalPriceRD = JedisUtil.connectionRedis().get(KEYTOTALPRICE);
        
        if(StringUtils.isNotEmpty(orderCountRD)){
            orderCount.addAndGet(Long.parseLong(orderCountRD)) ;
        }
        if(StringUtils.isNotEmpty(totalPriceRD)){
            totalPrice.addAndGet(Long.parseLong(totalPriceRD)) ;
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
