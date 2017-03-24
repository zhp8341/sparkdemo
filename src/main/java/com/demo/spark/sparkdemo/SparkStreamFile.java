
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
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;


public class SparkStreamFile {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkStreamFile").setMaster("local[2]");
        //SparkConf conf = new SparkConf().setAppName("SparkStreamFile1111").setMaster("spark://hadoop1:7077").set("spark.cores.max", "4");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(15));//每隔15s
        System.out.println(jssc);
        // 创建监听文件流 监控该目录下是否有新的文件进来，如果有新文件产生 每隔15s（Durations.seconds(15)）秒后计算
         JavaDStream<String> lines=jssc.textFileStream("/Users/huipeizhu/Documents/sparkdata/input/");  
        //JavaReceiverInputDStream<String> lines = jssc.socketTextStream("localhost", 9999);
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

        //保存到目录下
         wordCounts.dstream().saveAsTextFiles("/Users/huipeizhu/Documents/sparkdata/output", "countWord");

        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block

            e.printStackTrace();
        }

    }

}
