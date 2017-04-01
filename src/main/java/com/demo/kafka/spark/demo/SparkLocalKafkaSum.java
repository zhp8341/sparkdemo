
package com.demo.kafka.spark.demo;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.google.common.collect.Lists;

import scala.Tuple2;

/**
 * @ClassName: SparkLocalKafkaSum
 * @Description:
 * @author zhuhuipei
 * @date 2017年3月27日 下午7:53:05
 */
public class SparkLocalKafkaSum {

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

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkLocalKafkaSum");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(10));
        JavaInputDStream<ConsumerRecord<Object, Object>> lines = KafkaUtils.createDirectStream(jssc,
                                                                                               LocationStrategies.PreferBrokers(),
                                                                                               ConsumerStrategies.Subscribe(topics,
                                                                                                                            kafkaParams));

        JavaDStream<Integer> words = lines.flatMap(x -> {
            List<Integer> list = Lists.newArrayList();
            System.out.println(x);
            if (x.value() == null) {
                return list.iterator();
            }
            String[] str = x.value().toString().split(",");
            for (String string : str) {
                list.add(Integer.parseInt(string));
            }
            return list.iterator();
        });

        JavaDStream<Integer> sum = words.reduce((v1, v2) -> v1 + v2);
        
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
