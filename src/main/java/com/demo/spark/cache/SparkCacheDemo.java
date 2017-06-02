
package com.demo.spark.cache;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class SparkCacheDemo {

    private static JavaSparkContext sc;

    public static void main(String[] args) {
        List<Integer> list = Arrays.asList(5, 4, 3, 2, 1, 6, 9);
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkCacheDemo");
        sc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = sc.parallelize(list);
        // rdd.persist(StorageLevel.DISK_ONLY()); //磁盘存储
        rdd.persist(StorageLevel.MEMORY_ONLY());// 内存
        // rdd.persist(StorageLevel.MEMORY_ONLY_2()); //内存存储两份

        rdd.collect();
        rdd.collect(); // 这里可以设置debug断点便于查看
        rdd.unpersist(); // 清楚缓存
        rdd.collect(); // 这里也可以设置debug断点便于查看
    }
}
