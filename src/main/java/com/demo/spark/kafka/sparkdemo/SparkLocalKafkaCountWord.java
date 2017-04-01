
package com.demo.spark.kafka.sparkdemo;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

/**
 * 本地kafka和spark结合统计word
  * @ClassName: SparkLocalKafkaCountWord
  * @Description: 
  * @author zhuhuipei
  * @date 2017年3月27日 下午7:53:05
 */
public class SparkLocalKafkaCountWord {

    static Map<String, Integer>      topicMap    = new HashMap<>();

    static Map<String, Object>       kafkaParams = new HashMap<>();

    private static final int         numThreads  = 3;

    private static final String      topic       = "spark_test";

    static Map<TopicPartition, Long> offsets     = new HashMap<>();

    static Set<String>               topics      = Collections.singleton(topic);

    static {
        topicMap.put(topic, numThreads);
        kafkaParams.put("bootstrap.servers", "hadoop1:9092");
        kafkaParams.put("group.id", "group1");
        kafkaParams.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        offsets.put(new TopicPartition("spark_test", 1), 2L);
    }

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkLocalKafka SparkLocalKafkaCountWord streaming word count");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(5));
        JavaInputDStream<ConsumerRecord<Object, Object>> lines = KafkaUtils.createDirectStream(jssc,
                                                                                               LocationStrategies.PreferBrokers(),
                                                                                               ConsumerStrategies.Subscribe(topics,
                                                                                                                            kafkaParams, offsets));
        JavaDStream<String> words = lines.flatMap(x -> {
           System.out.println(x);
            return Arrays.asList(x.value().toString().split(" ")).iterator();
        });
        JavaPairDStream<String, Integer> pairs = words.mapToPair(x -> new Tuple2<String, Integer>(x, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((x, y) -> {
            return x + y;
        });
        wordCounts.print();
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.exit(-1);
        }

    }
}
