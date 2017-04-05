package com.demo.kafka.spark.avro;

import org.apache.avro.specific.SpecificRecordBase;

import com.yt.otter.canal.protocol.avro.BinlogTO;

import java.util.EnumSet;

/**
 * Created by Administrator on 2017/4/1.
 */
public enum Topic {
    BINLOGTO("yangtuo-t_order-UPDATE", new BinlogTO());

    public final String topicName;
    public final SpecificRecordBase topicType;

    Topic(String topicName, SpecificRecordBase topicType) {
        this.topicName = topicName;
        this.topicType = topicType;
    }

    public static Topic matchFor(String topicName) {
        return EnumSet.allOf(Topic.class).stream()
                .filter(topic -> topic.topicName.equals(topicName))
                .findFirst()
                .orElse(null);
    }
}
