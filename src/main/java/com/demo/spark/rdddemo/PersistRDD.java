
package com.demo.spark.rdddemo;

import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class PersistRDD {

    private static JavaSparkContext sc;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("PersistRDDDMOE");
        sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        JavaRDD<Integer> rdd =sc.parallelize(Arrays.asList(1,2,3,4,5));
        JavaRDD<Integer> rdd2=rdd.persist(StorageLevel.MEMORY_ONLY());
        System.out.println(rdd.count());
        System.out.println(StringUtils.join(rdd.collect(),','));
        System.out.println(StringUtils.join(rdd2.collect(),','));
    }
}
