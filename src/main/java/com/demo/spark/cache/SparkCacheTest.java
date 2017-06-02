
package com.demo.spark.cache;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class SparkCacheTest {

    private static JavaSparkContext sc;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkCacheTest");
        sc = new JavaSparkContext(conf);
        sc.setLogLevel("error");
        noCache();
        cache();
        System.out.println("");
    }

    /**
     * 不用缓存
     * 
     * @Title: noCache
     * @author zhuhuipei
     * @date 2017年6月2日 下午4:22:01
     */
    public static void noCache() {
        JavaRDD<String> rdd = sc.textFile("./test.txt");
        rdd.count();
        Long t1 = System.currentTimeMillis();
        System.out.println("noCache()=rdd.count()=" + rdd.count());
        Long t2 = System.currentTimeMillis();
        Long t2_t1 = t2 - t1;
        System.out.println("nocache()=" + t2_t1);
    }

    /**
     * @Title: cache
     * @author zhuhuipei
     * @date 2017年6月2日 下午5:03:51
     */
    public static void cache() {
        JavaRDD<String> rdd = sc.textFile("./test.txt").persist(StorageLevel.MEMORY_ONLY());
        rdd.count();
        Long t1 = System.currentTimeMillis();
        System.out.println(" cache()=rdd.count()=" + rdd.count());
        Long t2 = System.currentTimeMillis();
        Long t2_t1 = t2 - t1;
        System.out.println("cache()=" + t2_t1);
    }
}
