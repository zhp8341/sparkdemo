
package com.demo.spark.broadcast.accumulator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

public class BroadcastDemo {

    private static JavaSparkContext sc;
    
   public static void main(String[] srt){
       
     
      
       SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("BroadcastDemo");
       sc = new JavaSparkContext(conf);
       //Arrays中含义是 "性别编码,名字" 如"1,张三"
       JavaRDD<String> linesRDD = sc.parallelize(Arrays.asList("1,张三", "0,李梅", "3,王五"));
 
  
       Map<String, String> sexMap = new HashMap<String, String>();
       sexMap.put("1", "男人");
       sexMap.put("0", "女人");
       //声明一个广播变量，值是sexMap,
       Broadcast<Map<String, String>> sexMapBC = sc.broadcast(sexMap);
       JavaRDD<String> retRDD = linesRDD.map(new Function<String, String>() {
           @Override
           public String call(String line) throws Exception {
               String[] splits = line.split(",");
               String sid = splits[0].trim();
               //获取广播变量中的值，如果找不到就是 “未知”
               String sName = sexMapBC.value().getOrDefault(sid, "未知");
               return splits[1]+"是 " + sName + " " ;
           }
       }).cache();
       retRDD.foreach(str -> System.out.println(str));
   }
   
  
   
}


