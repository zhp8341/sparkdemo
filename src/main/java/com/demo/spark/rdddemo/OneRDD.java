
package com.demo.spark.rdddemo;

import java.util.Arrays;
import java.util.Comparator;
//import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import jersey.repackaged.com.google.common.collect.Maps;
import scala.Function1;
import scala.Tuple2;

/**
 * [1, 2, 3, 3, 5, 2] 单个RDD简单相关操作
 * 
 * @ClassName: OneRDD
 * @Description:
 * @author zhuhuipei
 * @date 2017年3月28日 上午10:47:02
 */
public class OneRDD {

    private static JavaSparkContext sc;

    public static void main(String[] args) throws Exception {
        List<Integer> list = Arrays.asList(5, 4, 3, 2, 1,6,9);// 5, 4, 3, 2, 1, 5, 4, 3, 2, 1, 5, 4, 3, 2, 1, 5, 4, 3, 2, 1,
                                                          // 5, 4, 3, 2, 1, 5, 4, 3, 2, 1, 5, 4, 3, 2, 1, 5, 4, 3, 2, 1,
                                                          // 5, 4, 3, 2, 1, 5, 4, 3, 2, 1, 5, 4, 3, 2, 1, 5, 4, 3, 2, 1,
                                                          // 5, 4, 3, 2, 1, 5, 4, 3, 2, 1, 5, 4, 3, 2, 1);
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("OneRDDDEMO");
        sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        // 原始数据转换成RDD
        JavaRDD<Integer> rdd = sc.parallelize(list);
        System.out.println("原始数据：" + rdd.collect());
        // distinct(rdd);
         filter(rdd);
        // map(rdd);
        // count(rdd);
        // countByValue(rdd);
        // take(rdd);
        // top(rdd);
        // reduce(rdd);
        // foreach(rdd);
        getmax(rdd);
        getmin(rdd);
        


    }

    /**
     * 去重操作
     * 
     * @Title: distinct
     * @param rdd
     * @author zhuhuipei
     * @date 2017年3月28日 上午9:39:34
     */
    public static void distinct(JavaRDD<Integer> rdd) {
        System.out.println("RDD去重操作:" + rdd.distinct().collect());
    }

    /**
     * 最每个元素进行筛选，返回符合条件的元素组成的一个新RDD
     * 
     * @Title: filter
     * @param rdd
     * @author zhuhuipei
     * @date 2017年3月28日 上午9:40:07
     */
    public static void filter(JavaRDD<Integer> rdd) {
        System.out.println("RDD去掉1的元素:" + rdd.filter(v -> v != null).collect());
    }

    /**
     * 对每个元素进行操作，返回一个新的RDD
     * 
     * @Title: map
     * @param rdd
     * @author zhuhuipei
     * @date 2017年3月28日 上午9:49:00
     */
    public static void map(JavaRDD<Integer> rdd) {
        System.out.println("RDD每个元素乘10:" + rdd.map(v -> v * 10).collect());
    }

    /**
     * 统计RDD的所有元素
     * 
     * @Title: count
     * @param rdd
     * @author zhuhuipei
     * @date 2017年3月28日 上午10:11:04
     */
    public static void count(JavaRDD<Integer> rdd) {
        System.out.println("统计RDD的所有元素:" + rdd.count());
    }

    /**
     * 每个元素出现的次数
     * 
     * @Title: countByValue
     * @param rdd
     * @author zhuhuipei
     * @date 2017年3月28日 上午10:14:32
     */
    public static void countByValue(JavaRDD<Integer> rdd) {
        System.out.println("每个元素出现的次数:" + rdd.countByValue());
    }

    /**
     * 取出rdd返回num个元素（默认好像）
     * 
     * @Title: take
     * @param rdd
     * @author zhuhuipei
     * @date 2017年3月28日 上午10:17:49
     */
    public static void take(JavaRDD<Integer> rdd) {
        System.out.println("取出rdd返回2个元素:" + rdd.take(2));
    }

    /**
     * 取出rdd返回最前N个元素
     * 
     * @Title: top
     * @param rdd
     * @author zhuhuipei
     * @date 2017年3月28日 上午10:19:29
     */
    public static void top(JavaRDD<Integer> rdd) {
        System.out.println("取出rdd返回最前2个元素:" + rdd.top(2));
    }

    /**
     * 并行整合RDD中所有数据
     * 
     * @Title: reduce
     * @param rdd
     * @author zhuhuipei
     * @date 2017年3月28日 上午10:37:52
     */
    public static void reduce(JavaRDD<Integer> rdd) {
        System.out.println("整合RDD中所有数据（sum）:" + rdd.reduce((v1, v2) -> v1 + v2));
    }

    /**
     * 遍历数据
     * 
     * @Title: foreach
     * @param rdd
     * @author zhuhuipei
     * @date 2017年3月28日 上午10:41:23
     */
    public static void foreach(JavaRDD<Integer> rdd) {
        System.out.print("foreach:");
        rdd.foreach(t -> System.out.print(t));
    }

    /**
     * 求最大值
      * @Title: getmax
      * @param rdd
      * @throws Exception       
      * @author zhuhuipei
      * @date 2017年5月25日 上午11:28:45
     */
    public static void getmax(JavaRDD<Integer> rdd) throws Exception {
        Integer max=  rdd.reduce((v1, v2) -> Math.max(v1, v2));
        System.out.println("max:"+max);
    }
    
    /**
     * 求最小值
      * @Title: getmax
      * @param rdd
      * @throws Exception       
      * @author zhuhuipei
      * @date 2017年5月25日 上午11:29:20
     */
    public static void getmin(JavaRDD<Integer> rdd) throws Exception {
        Integer min=  rdd.reduce((v1, v2) -> Math.min(v1, v2));
        System.out.println("min："+min);
    }
    


}
