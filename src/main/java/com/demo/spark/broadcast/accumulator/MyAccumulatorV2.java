package com.demo.spark.broadcast.accumulator;

import org.apache.spark.util.AccumulatorV2;

import java.util.HashSet;
import java.util.Set;

/**
 * @author zhuhuipei
 * @Description:
 * @date 2017/7/6
 * @time 上午11:16
 */
public class MyAccumulatorV2 extends AccumulatorV2<String, Set<String>> {

    private Set<String> set=new HashSet<>();

    @Override
    public boolean isZero() {
        return set.isEmpty();
    }

    @Override
    public AccumulatorV2<String, Set<String>> copy() {
        MyAccumulatorV2 myAccumulatorV2=new MyAccumulatorV2();
        synchronized(myAccumulatorV2){
            myAccumulatorV2.set.addAll(set);
        }
        return myAccumulatorV2;
    }

    @Override
    public void reset() {
        set.clear();
    }

    @Override
    public void add(String s) {
        set.add(s);
    }

    @Override
    public void merge(AccumulatorV2<String, Set<String>> accumulatorV2) {
        set.addAll(accumulatorV2.value());
    }

    @Override
    public Set<String> value() {
        return set;
    }
}


