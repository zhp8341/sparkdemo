package com.demo.spark.broadcast.accumulator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;
import java.util.List;

/**
 * @author zhuhuipei
 * @Description:
 * @date 2017/7/6
 * @time 上午10:13
 */
public class AccumulatorDemo {

    private static JavaSparkContext jsc;

    public static void main(String[] args) {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("AccumulatorDemo");
        jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("ERROR");

        LongAccumulator accum =jsc.sc().longAccumulator("longAccumulator");
        //LongAccumulator accum =new LongAccumulator();
        //jsc.sc().register(accum, "LongAccumulator");
        JavaRDD<Integer> rdd = jsc.parallelize(list, 5).cache();
        JavaRDD<Integer> newData = rdd.map(new Function<Integer, Integer>() {


            @Override
            public Integer call(Integer x) throws Exception {
                if (x % 2 == 0) {
                    accum.add(1L);
                    return 1;
                } else {
                    return 0;
                }

            }
        });


        newData.count();


        accum.value();

        try {
            Thread.sleep(1000*60L);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
