package com.tan.flink.learn.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/16 15:13
 * describe content: flink-1.16.0-learn
 */
public class Map {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("hadoop", 10000);
        source.map(new MapFunction<String, String>() {
            private String prefix = "MAP_";

            @Override
            public String map(String in) throws Exception {
                return prefix + in;
            }
        });

    }

}
