
package com.demo.redis;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;

import com.clearspring.analytics.util.Lists;

import redis.clients.jedis.Jedis;

public class JedisUtil {

    public static final String KEY = "date_persist_1";

    public static void main(String[] args) {
        List<Long> listl=Lists.newArrayList();
        listl.add(0L);
        push(listl);
    }

    public static Jedis connectionRedis() {
        Jedis jedis = new Jedis("10.117.41.72", 7000);
        return jedis;
    }

    
    public static synchronized void set(String value){
        connectionRedis().set("spark_set", value);
    }
    public static synchronized String get(String value){
        return connectionRedis().get("spark_set");
    }
    
    public static synchronized void push(List<Long> listl) {
        for (Long long1 : listl) {
            connectionRedis().lpush(KEY, String.valueOf(long1));
        }
       
    }

    public static List<Long> get(List<Long> nowList) {
        if(CollectionUtils.isEmpty(nowList)){
            return com.google.common.collect.Lists.newArrayList();
        }
        push(nowList);
        List<String> list = connectionRedis().lrange(KEY, 0, Long.MAX_VALUE);
        if (CollectionUtils.isEmpty(list)) {
            return nowList;
        }
        List<Long> listLong = Lists.newArrayList();
        for (String string : list) {
            listLong.add(Long.parseLong(string));
        }
        System.out.println("list=    "+list);
        System.out.println("listLong="+listLong);
        return listLong;
    }
}
