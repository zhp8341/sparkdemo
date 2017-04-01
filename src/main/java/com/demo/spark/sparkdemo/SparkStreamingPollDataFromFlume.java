
package com.demo.spark.sparkdemo;

import java.util.Arrays;

import org.apache.spark.*;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

public class SparkStreamingPollDataFromFlume {
    public static void main(String[] args) throws Exception {
        System.setProperty("spark.scheduler.mode", "FAIR");
        //SparkConf conf = new SparkConf().setMaster("spark://192.168.4.163:7077").setAppName("NetworkWordCount");
        SparkConf conf = new SparkConf().setMaster("spark://hadoop1:7077").setAppName("NetworkWordCount_hadoop");
        conf.set("spark.scheduler.mode", "FAIR");
        conf.set("spark.cores.max", "2");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLocalProperty("spark.scheduler.pool", "production");
        sc.setLogLevel("ERROR");
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(5));
        JavaReceiverInputDStream<String> lines = jssc.socketTextStream("hadoop1", 9999);
        JavaDStream<String> words = lines.flatMap(
                                                  x -> Arrays.asList(x.split(" ")).iterator());
     // Count each word in each batch
        JavaPairDStream<String, Integer> pairs = words.mapToPair(
          s -> new Tuple2<>(s, 1));
        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(
          (i1, i2) -> i1 + i2);

        // Print the first ten elements of each RDD generated in this DStream to the console
        wordCounts.print();
        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate
       
    }
}
