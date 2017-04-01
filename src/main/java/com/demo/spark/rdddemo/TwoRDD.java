
package com.demo.spark.rdddemo;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * [1, 2, 3] [3, 4, 5] 两个个RDD简单相关操作
 * 
 * @ClassName: TwoRDD
 * @Description:
 * @author zhuhuipei
 * @date 2017年3月28日 上午10:48:12
 */
public class TwoRDD {

    private static JavaSparkContext sc;

    public static void main(String[] args) {
        List<Integer> list1 = Arrays.asList(1, 2, 3);
        List<Integer> list2 = Arrays.asList(3, 4, 5);
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TwoRDDDemo");
        sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        // 原始数据转换成RDD
        JavaRDD<Integer> rdd1 = sc.parallelize(list1);
        JavaRDD<Integer> rdd2 = sc.parallelize(list2);
        union(rdd1, rdd2);
        subtract(rdd1, rdd2);
        intersection(rdd1, rdd2);
        cartesian(rdd1, rdd2);

    }

    /**
     * 两个RDD集合
     * 
     * @Title: union
     * @param rdd1
     * @param rdd2
     * @author zhuhuipei
     * @date 2017年3月28日 上午10:51:52
     */
    public static void union(JavaRDD<Integer> rdd1, JavaRDD<Integer> rdd2) {
        System.out.println("两个RDD集合:" + rdd1.union(rdd2).collect());
    }

    /**
     * 去除两个RDD集合共同元素
     * 
     * @Title: intersection
     * @param rdd1
     * @param rdd2
     * @author zhuhuipei
     * @date 2017年3月28日 上午10:53:09
     */
    public static void subtract(JavaRDD<Integer> rdd1, JavaRDD<Integer> rdd2) {
        System.out.println("去除两个RDD集合共同元素:" + rdd1.subtract(rdd2).collect());
    }

    /**
     * 两个RDD集合共同元素
     * 
     * @Title: intersection
     * @param rdd1
     * @param rdd2
     * @author zhuhuipei
     * @date 2017年3月28日 上午10:56:29
     */
    public static void intersection(JavaRDD<Integer> rdd1, JavaRDD<Integer> rdd2) {
        System.out.println("两个RDD集合共同元素:" + rdd1.intersection(rdd2).collect());
    }

    /**
     * 两个RDD集合的笛卡尔积
     * 
     * @Title: cartesian
     * @param rdd1
     * @param rdd2
     * @author zhuhuipei
     * @date 2017年3月28日 上午10:54:18
     */
    public static void cartesian(JavaRDD<Integer> rdd1, JavaRDD<Integer> rdd2) {
        System.out.println("和另外一个RDD集合的笛卡尔积:" + rdd1.cartesian(rdd2).collect());
    }
}
