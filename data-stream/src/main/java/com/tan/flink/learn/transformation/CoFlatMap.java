package com.tan.flink.learn.transformation;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

public class CoFlatMap {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source1 = env.socketTextStream("hadoop101", 10000);
        DataStreamSource<String> source2 = env.socketTextStream("hadoop101", 10001);

        ConnectedStreams<String, String> connect = source1.connect(source2);

        connect.flatMap(new CoFlatMapFunction<String, String, String>() {
            @Override
            public void flatMap1(String first, Collector<String> collector) throws Exception {
                // 业务处理逻辑
            }

            @Override
            public void flatMap2(String second, Collector<String> collector) throws Exception {
                // 业务处理逻辑
            }
        });
    }

}
