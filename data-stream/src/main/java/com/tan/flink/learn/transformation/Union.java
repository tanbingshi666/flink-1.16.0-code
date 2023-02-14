package com.tan.flink.learn.transformation;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/16 16:39
 * describe content: flink-1.16.0-learn
 */
public class Union {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source1 = env.socketTextStream("hadoop", 10000);
        DataStreamSource<String> source2 = env.socketTextStream("hadoop", 10001);

        source1.union(source2);

    }

}
