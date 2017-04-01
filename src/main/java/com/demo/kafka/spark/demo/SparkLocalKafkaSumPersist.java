
package com.demo.kafka.spark.demo;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.I;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.demo.redis.JedisUtil;
import com.google.common.collect.Lists;


/**
 *  使用map简单持久化
 * 
 * @ClassName: SparkLocalKafkaSum
 * @Description:
 * @author zhuhuipei
 * @date 2017年3月27日 下午7:53:05
 */
public class SparkLocalKafkaSumPersist {

    static Map<String, Integer> topicMap    = new HashMap<>();

    static Map<String, Object>  kafkaParams = new HashMap<>();

    static Set<String>          topics      = Collections.singleton(KafkaProducerTest.TOPIC);

    static {
        kafkaParams.put("bootstrap.servers", "hadoop1:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

    }
    

    public static void main(String[] args) {
        
        SparkConf conf = new SparkConf().setMaster("local[1]").setAppName("SparkLocalKafkaSum");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(2));
        JavaInputDStream<ConsumerRecord<Object, Object>> lines = KafkaUtils.createDirectStream(jssc,
                                                                                               LocationStrategies.PreferBrokers(),
                                                                                               ConsumerStrategies.Subscribe(topics,
                                                                                                                            kafkaParams));
        JavaDStream<Long> words = lines.flatMap(x -> {
            List<Long> list = Lists.newArrayList();
            if (x.value() == null) {
                return list.iterator();
            }
            String[] str = x.value().toString().split(",");
            
            for (String string : str) {
                list.add(Long.parseLong(string));
            }
//            System.out.println("====list======"+list);
//            List<Long> lists = Arrays.asList(5L, 4L, 3L, 2L, 1L, 5L, 4L, 3L, 2L, 1L, 5L, 4L, 3L, 2L, 1L, 5L, 4L, 3L, 2L, 1L, 5L, 4L, 3L, 2L, 1L, 5L, 4L, 3L, 2L, 1L, 5L, 4L, 3L, 2L, 1L, 5L, 4L, 3L, 2L, 1L, 5L, 4L, 3L, 2L, 1L, 5L, 4L, 3L, 2L, 1L, 5L, 4L, 3L, 2L, 1L, 5L, 4L, 3L, 2L, 1L, 5L, 4L, 3L, 2L, 1L, 5L, 4L, 3L, 2L, 1L, 5L, 4L, 3L, 2L, 1L, 5L, 4L, 3L, 2L, 1L);
//            
//            JedisUtil.push(list);
//            List<String> list2 = JedisUtil.connectionRedis().lrange(JedisUtil.KEY, 0, Long.MAX_VALUE);
//            List<Long> listLong = Lists.newArrayList();
//            for (String string : list2) {
//                listLong.add(Long.parseLong(string));
//            }
            
            
            return list.iterator();//JedisUtil.get(list).iterator();
        });

        JavaDStream<Long> sum = words.reduce((v1, v2) -> v1 + v2);
        sum.foreachRDD(new VoidFunction<JavaRDD<Long>>() {
            @Override
                public void call(JavaRDD<Long> o) throws Exception {
                
                  if(o.collect()!=null&&o.collect().iterator()!=null){
                      Iterator<Long>  i= o.collect().iterator();
                      while (i.hasNext()) {
                          Long x=i.next();
                          System.out.println("o.collect()="+x); 
                         
                          JedisUtil.set(x.toString());
                          
                      }
                     
                  }
                  
                   //JedisUtil.set(o.collect());
                }
            });
        sum.print();
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }

    }
}
