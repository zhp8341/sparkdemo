
package com.demo.spark.sparkdemo;





import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
public class SparkStreamHDFS {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkStreamHDFS").setMaster("spark://hadoop1:7077");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(15));
        System.out.println(jssc);
        //创建监听文件流
        JavaDStream<String> lines=jssc.textFileStream("hdfs://hadoop1:8020/spark/sparkwordCounts/input");

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public Iterator<String> call(String x) {
                return Arrays.asList(x.split(" ")).iterator();
            }
        });
        // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {

            @Override
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<>(s, 1);
            }
        });
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {

            @Override
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });
        wordCounts.print();
        wordCounts.dstream().saveAsTextFiles("hdfs://hadoop1:8020/spark/sparkwordCounts/output", "sparkwordCounts");

        jssc.start(); 
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            
            e.printStackTrace();
        }



    }

}
