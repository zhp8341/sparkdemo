package com.demo.spark.broadcast.accumulator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * @author zhuhuipei
 * @Description:
 * @date 2017/7/6
 * @time 上午11:46
 */
public class MyAccumulatorV2Main {
    private static JavaSparkContext jsc;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("MyAccumulatorV2Main");
        jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("ERROR");
        MyAccumulatorV2 myAccumulatorV2=new MyAccumulatorV2();
        jsc.sc().register(myAccumulatorV2, "myAccumulatorV2");
        JavaRDD<String> rdd = jsc.parallelize(Arrays.asList("A","B","C"), 5).cache();
        rdd.foreach(x ->myAccumulatorV2.add(x));
        myAccumulatorV2.value();
        MyAccumulatorV2 myAccumulatorV3= (MyAccumulatorV2) myAccumulatorV2.copy();//copy数据
        myAccumulatorV3.value();
        myAccumulatorV3.reset(); //清空数据
        try {
            Thread.sleep(1000*60L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
