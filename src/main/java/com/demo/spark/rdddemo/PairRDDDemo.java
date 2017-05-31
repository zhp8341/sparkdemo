
package com.demo.spark.rdddemo;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import com.google.common.collect.Lists;

import scala.Tuple2;

/**
 * 键值对操作
 * 
 * @ClassName: PairRDDDemo
 * @Description:
 * @author zhuhuipei
 * @date 2017年5月30日 上午9:32:35
 */
public class PairRDDDemo {

    private static JavaSparkContext sc;

    public static void main(String[] args) {
        JavaPairRDD<Integer, Integer> pairs = createPairRDD();
        reduceByKey(pairs);
        groupByKey(pairs);
        keys(pairs);
        values(pairs);
        sortByKey(pairs);
        distinct(pairs);
        maxValue(pairs);
        mapValues(pairs);
        flatMapValues(pairs);
        countByKey(pairs);
        collectAsMap(pairs);
        lookup(pairs);
    }

    /**
     * 相同键的值进行相加
     * 
     * @Title: a
     * @author zhuhuipei
     * @date 2017年5月30日 上午9:47:02
     */
    public static void reduceByKey(JavaPairRDD<Integer, Integer> pairs) {
        JavaPairRDD<Integer, Integer> pair = pairs.reduceByKey((v1, v2) -> v1 + v2);
        System.out.println("reduceByKey 相同键的值进行相加=" + pair.collect());
    }

    /**
     * 相同键的值进行分组
     * 
     * @Title: groupByKey
     * @param pairs
     * @author zhuhuipei
     * @date 2017年5月30日 上午9:54:54
     */
    public static void groupByKey(JavaPairRDD<Integer, Integer> pairs) {
        JavaPairRDD<Integer, Iterable<Integer>> rdd = pairs.groupByKey();
        System.out.println("groupByKey 对具有相同键的值进行分组=" + rdd.collect());
    }

    /**
     * 获取所以得key
     * 
     * @Title: keys
     * @param pairs
     * @author zhuhuipei
     * @date 2017年5月30日 上午9:55:58
     */
    public static void keys(JavaPairRDD<Integer, Integer> pairs) {
        JavaRDD<Integer> keys = pairs.keys();
        System.out.println("keys 获取所以得键即key=" + keys.collect());
    }

    /**
     * 获取全部的values
     * 
     * @Title: values
     * @param pairs
     * @author zhuhuipei
     * @date 2017年5月30日 上午10:05:45
     */
    public static void values(JavaPairRDD<Integer, Integer> pairs) {
        JavaRDD<Integer> values = pairs.values();
        System.out.println("values 获取所以得键的值即value=" + values.collect());
    }

    /**
     * 根据键排序
     * 
     * @Title: sortByKey
     * @param pairs
     * @author zhuhuipei
     * @date 2017年5月30日 上午10:07:31
     */
    public static void sortByKey(JavaPairRDD<Integer, Integer> pairs) {
        JavaPairRDD<Integer, Integer> sortByKey = pairs.sortByKey();
        System.out.println("sortByKey 键排序=" + sortByKey.collect());
    }

    /**
     * 去重操作
     * 
     * @Title: distinct
     * @param pairs
     * @author zhuhuipei
     * @date 2017年5月30日 上午10:10:58
     */
    public static void distinct(JavaPairRDD<Integer, Integer> pairs) {
        JavaPairRDD<Integer, Integer> distinct = pairs.distinct();
        System.out.println("distinct 去重操作=" + distinct.collect());
    }

    /**
     * 相同的键值当中取出最大的那个键值对 如：[(1,2), (2,3), (3,4)，(3,8)] 结果就是[(1,2), (2,3),(3,8)]
     * 
     * @Title: maxValue
     * @param pairs
     * @author zhuhuipei
     * @date 2017年5月30日 上午10:15:22
     */
    public static void maxValue(JavaPairRDD<Integer, Integer> pairs) {
        JavaPairRDD<Integer, Integer> max = pairs.reduceByKey((v1, v2) -> Math.max(v1, v2));
        System.out.println("maxValue 取value最大的的键值对=" + max.collect());
    }

    /**
     * 随机改变value的值
     * 
     * @Title: mapValues
     * @param pairs
     * @author zhuhuipei
     * @date 2017年5月30日 上午10:26:43
     */
    public static void mapValues(JavaPairRDD<Integer, Integer> pairs) {
        JavaPairRDD<Integer, Integer> mapValues = pairs.mapValues(v1 -> v1 + new Random().nextInt(10));
        System.out.println("mapValues 随机改变value的值=" + mapValues.collect());
    }

    /**
     * 批量更改value的值（和mapValues是有区别的） 官方介绍：对 pair RDD 中的每个值应用 一个返回迭代器的函数，然后 对返回的每个元素都生成一个 对应原键的键值对记录。通常 用于符号化
     * 
     * @Title: flatMapValues
     * @param pairs
     * @author zhuhuipei
     * @date 2017年5月30日 上午10:42:04
     */
    public static void flatMapValues(JavaPairRDD<Integer, Integer> pairs) {
        JavaPairRDD<Integer, Integer> flatMapValues = pairs.flatMapValues(v1 -> Lists.newArrayList(10));
        System.out.println("flatMapValues 迭代操作=" + flatMapValues.collect());
    }

    /**
     * 对每个键对应的元素分别计数
     * 
     * @Title: countByKey
     * @param pairs
     * @author zhuhuipei
     * @date 2017年5月31日 上午11:33:11
     */
    public static void countByKey(JavaPairRDD<Integer, Integer> pairs) {
        Map<Integer, Long> countByKey = pairs.countByKey();
        System.out.println("countByKey 元素分别计数=" + countByKey);
    }

    /**
     * 将结果以映射表的形式返回，以便查询
     * 
     * @Title: collectAsMap
     * @param pairs
     * @author zhuhuipei
     * @date 2017年5月31日 上午11:34:31
     */
    public static void collectAsMap(JavaPairRDD<Integer, Integer> pairs) {
        Map<Integer, Integer> collectAsMap = pairs.collectAsMap();
        System.out.println("collectAsMap 将结果以映射表的形式返回=" + collectAsMap);
    }

    /**
     * 返回给定键对应的所有值
     * 
     * @Title: collectAsMap
     * @param pairs
     * @author zhuhuipei
     * @date 2017年5月31日 上午11:35:05
     */
    public static void lookup(JavaPairRDD<Integer, Integer> pairs) {
        List<Integer> lookup = pairs.lookup(9);
        System.out.println("lookup 返回给定键对应的所有值=" + lookup);
    }

    /**
     * 将一个list转换成键值对的模式
     * 
     * @Title: createPairRDD
     * @return
     * @author zhuhuipei
     * @date 2017年5月30日 上午9:45:31
     */
    public static JavaPairRDD<Integer, Integer> createPairRDD() {
        List<Integer> list = Arrays.asList(5, 4, 3, 2, 1, 6, 9, 5, 8, 9);
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("PairRDDDemo");
        sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        // JavaRDD<Integer> rdd = sc.parallelize(list);
        JavaRDD<Integer> rdd = sc.parallelize(list, 2); // 这个是分区用了，指定创建得到的 RDD 分区个数为 2。
        PairFunction<Integer, Integer, Integer> keyData = new PairFunction<Integer, Integer, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Tuple2<Integer, Integer> call(Integer x) throws Exception {
                return new Tuple2<Integer, Integer>(x, x + 1);// 键值对转换,key=x ,value=x+1
            }
        };
        JavaPairRDD<Integer, Integer> pairs = rdd.mapToPair(keyData);
        System.out.println("分区数量=" + pairs.partitions().size());
        System.out.println("转换后的键值对=" + pairs.collect());
        return pairs;
    }

}
