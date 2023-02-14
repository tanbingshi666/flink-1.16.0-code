package com.tan.flink.learn.transformation;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class CoMap {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source1 = env.socketTextStream("hadoop101", 10000);
        DataStreamSource<String> source2 = env.socketTextStream("hadoop101", 10001);

        ConnectedStreams<String, String> connect = source1.connect(source2);

        connect.map(new CoMapFunction<String, String, String>() {
            @Override
            public String map1(String first) throws Exception {
                return null;
            }

            @Override
            public String map2(String second) throws Exception {
                return null;
            }
        });

    }

}
