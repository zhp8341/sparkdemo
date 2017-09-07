package com.demo.spark.broadcast.accumulator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.DoubleAccumulator;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

/**
 * @author zhuhuipei
 * @Description:
 * @date 2017/7/6
 * @time 上午10:13
 */
public class AccumulatorWordCountDemo {

    private static JavaSparkContext jsc;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("AccumulatorDemo");
        jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("ERROR");
        LongAccumulator accum = jsc.sc().longAccumulator();
        jsc.parallelize(Arrays.asList(1, 2, 3, 4)).foreach(x -> accum.add(x));

    }
}
